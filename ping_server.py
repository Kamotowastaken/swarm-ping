"""Swarm ping server — localhost mailbox for subagent members.

Members can only reach the network via `webfetch` (GET-only, no POST, no
sockets), so every operation here is a GET with query params. The conductor
(hosts this server) puts the base URL + run-id + member id in each brief.

Endpoints (all GET, JSON out):
  /health                        -> {"ok": true}
  /endpoints                     -> {"endpoints": [...]} (discovery listing)
  /send?run=R&from=A&to=B|all&body=T   store a direct message
  /inbox?run=R&who=B             return unread for B (direct + to=all)
  /peek?run=R&who=B              same, without marking delivered
  /post?run=R&from=A&body=T[&in_reply_to=N][&protocol=P]  append a board entry (threaded)
  /append?run=R&id=N&who=A&body=T  continue your own entry (total stays <=4KB)
  /finding?run=R&from=A&claim=T[&evidence=E][&quote=Q][&etype=observed|asserted][&scope=S][&confidence=low|medium|high][&protocol=P]  structured finding entry
  Pure creates (/post /finding /ask /send) accept &client_key=K for idempotent replay: repeats return the original id with "replay": true.
  /resolve?run=R&who=A&verdict=adopt|reject&winners=ids&losers=ids&why=T[&protocol=P]  typed immutable decision; marks entries settled
  /near?run=R&body=T  top-3 similar board entries by word overlap (promote, never suppress)
  Ballots: /ask gains options=A,B,C for closed questions; /vote?run=R&id=N&who=B&ranking=A,C,B casts a ranked ballot; /tally?run=R&id=N&who=A auto-emits winner finding + verdict (Borda)
  /board?run=R&since=N[&wait=S][&protocol=P][&kind=K]  entries id>N; long-poll up to S sec (max 25)
  /retract?run=R&id=N&who=B      delete B's own board entry N (children keep a dangling reply_to — readers must tolerate unresolvable ids)
  Lobby (barrier + help flow + presence):
  /enter?run=R&who=B             check into the lobby (re-entry clears a leave)
  /leave?run=R&who=B[&note=T]    clock out: drops from active, releases live claims held, announces on board
  /status?run=R&who=B&state=working|idle[&note=T]  advisory only — never affects matching/gating, but check-in is required
  /lobby?run=R[&wait=S]             -> {"checked_in": [...], "n": k, "open": j, "left": [...], "status": {who: "state[: note]"}, "waited": s} (long-polls until presence/open changes)
  Help flow (modes: fast = first-claim wins; auction = bids then /award):
  /ask?run=R&from=A&need=TAG&body=T[&hop=0][&mode=fast|auction][&options=A,B,C][&protocol=P]  post a HELP request (only hop=0 accepted; options= makes it a ballot)
  /open?run=R[&protocol=P]   unclaimed/unawarded requests younger than 600 s
  /claim?run=R&id=N&who=B[&eta=M][&note=T]  fast: atomic first-wins; auction: bid
  /award?run=R&id=N&who=A&winner=B  asker picks the winning bid
  /done?run=R&id=N&who=B&body=T[&protocol=P]  winner posts result (board + inbox notice)
  /fail?run=R&id=N&who=A&body=T  asker declares the request dead (kind=failure)
  /subscribe?run=R&who=B&topic=TAG  get inbox notices for NEEDs with this tag
  /unsubscribe?run=R&who=B&topic=TAG
  /transcript?run=R                 markdown render of the run's board

Replies to send/post/ask/done/fail/finding/resolve echo {"id": n, "len": k (utf-8 bytes), "head": first-120 chars} plus "warn" when len exceeds the 400-byte brief discipline (/append echoes the snippet plus "entry_len" total). /board replies carry "waited" seconds.

Rules: run/who/from/to/need/topic/protocol/client_key/options/winner match [A-Za-z0-9_-]{1,64}; body <= 4KB and non-empty on every write route. 400 otherwise (409 on lost claim races and post-resolution writes, settled re-resolve included).
Persistence: every send/post/finding/append/ask/claim/award/done/fail/resolve/vote/retract/leave/status appends JSONL to <swarmdir>/<run>/comms.jsonl
(env SWARM_DIR, default <cwd>/.swarm). Unread state is in-memory only — a
server restart marks everything unread again (members re-drain; harmless).

Security: binds 127.0.0.1 only, no auth. Never expose beyond localhost.
Stdlib only. Usage: ping_server.py [port]  (default 8471)
"""
import json
import os
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
BODY_MAX = 4096
BRIEF_BODY_MAX = 400  # brief discipline: longer bodies risk transit clipping
HEAD_LEN = 120
ASK_TTL = 600  # open requests older than this vanish from /open
WAIT_MAX = 25  # long-poll ceiling (seconds)
SWARM_DIR = os.environ.get(
    "SWARM_DIR",
    os.path.join(os.getcwd(), ".swarm"),
)


class Store:
    def __init__(self):
        # RLock: create_once() holds the lock across replay-check + store,
        # so nested store calls must re-enter.
        self.lock = threading.RLock()
        self.seq = 0
        self.inbox = {}  # (run, who) -> [msg]; never deleted (see drain)
        self.board = {}  # run -> [entry]
        self.delivered = set()  # (run, who, msg-id): at-most-once per reader
        self.present = {}  # run -> {who: ts} lobby check-ins (active)
        self.departed = {}  # run -> {who: {ts, note}} clocked-out members
        self.status = {}  # run -> {who: {state, note, ts}} working|idle
        self.asks = {}  # (run, id) -> {from, need, body, ts, claimed_by,
                        #  claimed_ts, mode, bids, awarded, protocol,
                        #  options, ballots}
        self.subs = {}  # (run, topic) -> set(who) need-tag subscriptions
        self.keys = {}  # (run, frm, key) -> entry id (idempotent replay)

    def _log(self, run, record):
        try:
            os.makedirs(os.path.join(SWARM_DIR, run), exist_ok=True)
            with open(
                os.path.join(SWARM_DIR, run, "comms.jsonl"),
                "a",
                encoding="utf-8",
            ) as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError as e:
            print(f"log-write failed: {e}", file=sys.stderr, flush=True)

    def send(self, run, frm, to, body):
        with self.lock:
            self.seq += 1
            msg = {"id": self.seq, "ts": time.time(), "from": frm,
                   "to": to, "body": body}
            self.inbox.setdefault((run, to), []).append(msg)
            self._log(run, {"kind": "msg", **msg})
            return msg["id"]

    def drain(self, run, who, peek=False):
        # At-most-once per reader: messages stay stored (audit trail mirrors
        # comms.jsonl); each (run, who) pair receives each message once.
        with self.lock:
            out = []
            for (r, to), msgs in self.inbox.items():
                if r != run:
                    continue
                if to != who and to != "all":
                    continue
                for m in msgs:
                    if m["from"] == who:
                        continue
                    if (run, who, m["id"]) in self.delivered:
                        continue
                    out.append(m)
                    if not peek:
                        self.delivered.add((run, who, m["id"]))
            out.sort(key=lambda m: m["id"])
            return out

    def post(self, run, frm, body, reply_to=0, protocol="general"):
        with self.lock:
            if reply_to and not any(e["id"] == reply_to
                                     for e in self.board.get(run, [])):
                return None
            self.seq += 1
            entry = {"id": self.seq, "kind": "note", "ts": time.time(),
                     "from": frm, "protocol": protocol, "body": body}
            if reply_to:
                entry["reply_to"] = reply_to
            self.board.setdefault(run, []).append(entry)
            self._log(run, {"kind": "note", "run": run, **entry})
            return entry["id"]

    def board_since(self, run, since, protocol=None, kind=None):
        # Snapshot copies: callers encode after the lock drops, while
        # resolve()/retract() may still mutate live entries.
        with self.lock:
            return [dict(e) for e in self.board.get(run, [])
                    if e["id"] > since
                    and (not protocol or
                         e.get("protocol", "general") == protocol)
                    and (not kind or e.get("kind") == kind)]

    def append(self, run, nid, who, body):
        with self.lock:
            for e in self.board.get(run, []):
                if e["id"] == nid and e["from"] == who:
                    if e.get("kind") not in ("note", "finding"):
                        return {"ok": False,
                                "error": "only note/finding entries grow"}
                    new_body = e["body"] + "\n… " + body
                    if len(new_body.encode("utf-8")) > BODY_MAX:
                        return {"ok": False,
                                "error": "entry would exceed 4KB"}
                    e["body"] = new_body
                    self._log(run, {"kind": "append", "run": run,
                                    "id": nid, "from": who,
                                    "added": len(body)})
                    return {"ok": True, "id": nid,
                            "len": len(new_body.encode("utf-8"))}
            return {"ok": False, "error": "entry not found or not yours"}

    def finding(self, run, frm, claim, evidence, quote, etype, scope,
                confidence, protocol):
        body = f"[{etype}/{confidence}] {claim} | " \
               f"evidence {evidence}" + \
               (f" scope {scope}" if scope else "") + \
               (f' quote "{quote}"' if quote else "")
        if len(body.encode("utf-8")) > BODY_MAX:
            return None  # composed entry must respect the board invariant
        with self.lock:
            self.seq += 1
            nid = self.seq
            entry = {"id": nid, "kind": "finding", "ts": time.time(),
                     "from": frm, "protocol": protocol,
                     "claim": claim, "evidence": evidence,
                     "quote": quote, "etype": etype, "scope": scope,
                     "confidence": confidence, "body": body}
            self.board.setdefault(run, []).append(entry)
            self._log(run, {"kind": "finding", "run": run, **entry})
            return nid

    def retract(self, run, nid, who):
        with self.lock:
            entries = self.board.get(run, [])
            for i, e in enumerate(entries):
                if e["id"] == nid and e["from"] == who:
                    del entries[i]
                    self._log(run, {"kind": "retract", "run": run,
                                    "id": nid, "from": who})
                    if (run, nid) in self.asks:
                        del self.asks[(run, nid)]
                    return True
            return False

    def enter(self, run, who):
        with self.lock:
            self.present.setdefault(run, {})[who] = time.time()
            self.departed.get(run, {}).pop(who, None)  # re-entry clears it
            return sorted(self.present[run])

    def leave(self, run, who, note=""):
        # Terminated: drop from active, release live claims held, announce
        # on the board (wakes long-pollers) + JSONL. Never 404s: leaving a
        # run you never entered is a no-op success (idempotent clock-out).
        with self.lock:
            self.present.get(run, {}).pop(who, None)
            self.status.get(run, {}).pop(who, None)
            released = []
            now = time.time()
            for (r, nid), a in self.asks.items():
                if r != run:
                    continue
                if a.get("options"):
                    continue
                if a.get("claimed_by") == who and self._claim_live(a, now):
                    a["claimed_by"] = None
                    a["claimed_ts"] = 0
                    if a.get("awarded") == who:
                        a["awarded"] = None  # auction reopens to bidders
                    released.append(nid)
                    self._log(run, {"kind": "release", "run": run,
                                    "id": nid, "from": who})
            self.departed.setdefault(run, {})[who] = {"ts": now,
                                                      "note": note}
            self.seq += 1
            entry = {"id": self.seq, "kind": "note", "ts": now,
                     "from": who, "protocol": "general",
                     "body": f"\u23fb {who} clocked out" +
                             (f": {note}" if note else "") +
                             (f" (released claims {released})"
                              if released else "")}
            self.board.setdefault(run, []).append(entry)
            self._log(run, {**entry, "kind": "leave", "run": run})
            return {"left": who, "released": released,
                    "active": sorted(self.present.get(run, {}))}

    def set_status(self, run, who, state, note=""):
        # Advisory only: working|idle never gates anything. Lets peers tell
        # "quiet but around" apart from "gone" (gone = in /lobby left list).
        if state not in ("working", "idle"):
            return None
        with self.lock:
            if who not in self.present.get(run, {}):
                return {"ok": False,
                        "error": "not checked in — /enter first"}
            self.status.setdefault(run, {})[who] = {
                "state": state, "note": note, "ts": time.time()}
            self._log(run, {"kind": "status", "run": run, "from": who,
                            "state": state, "note": note})
            return {"ok": True, "who": who, "state": state}

    def lobby(self, run):
        with self.lock:
            checked = sorted(self.present.get(run, {}))
            left = sorted(self.departed.get(run, {}))
            status = {}
            for who, s in self.status.get(run, {}).items():
                if who in self.present.get(run, {}):
                    status[who] = s["state"] + (
                        f": {s['note']}" if s["note"] else "")
            now = time.time()
            open_n = 0
            for (r, _), a in self.asks.items():
                if r != run or now - a["ts"] >= ASK_TTL:
                    continue
                if a.get("options"):
                    continue  # ballots resolve via /tally, not claims
                if a["mode"] == "auction" and not a["awarded"]:
                    open_n += 1
                elif not self._claim_live(a, now):
                    open_n += 1
            return {"checked_in": checked, "n": len(checked),
                    "open": open_n, "left": left, "status": status}

    def subscribe(self, run, who, topic):
        with self.lock:
            self.subs.setdefault((run, topic), set()).add(who)
            return sorted(self.subs[(run, topic)])

    def unsubscribe(self, run, who, topic):
        with self.lock:
            s = self.subs.get((run, topic), set())
            s.discard(who)
            return sorted(s)

    def transcript(self, run):
        with self.lock:
            # Snapshot copies (see board_since): rendering runs lock-free.
            entries = [dict(e) for e in self.board.get(run, [])]
        lines = [f"# transcript {run} ({len(entries)} entries)"]
        for e in entries:
            head = f"## {e['id']} [{e.get('kind', 'note')}] {e['from']}"
            if e.get("settled"):
                head += " (settled)"
            if e.get("reply_to"):
                head += f" (reply to {e['reply_to']})"
            if e.get("protocol", "general") != "general":
                head += f" [{e['protocol']}]"
            lines.append(head)
            lines.append(e["body"])
            lines.append("")
        return "\n".join(lines)

    def ask(self, run, frm, need, body, hop, mode, protocol,
            options=None):
        with self.lock:
            self.seq += 1
            nid = self.seq
            entry = {"id": nid, "kind": "help", "protocol": protocol,
                     "ts": time.time(), "from": frm,
                     "body": f"NEED {need} [{mode}] (hop {hop}): {body}" +
                             (f" options={options}" if options else "")}
            self.board.setdefault(run, []).append(entry)
            self.asks[(run, nid)] = {"from": frm, "need": need,
                                     "body": body, "ts": entry["ts"],
                                     "claimed_by": None, "claimed_ts": 0,
                                     "mode": mode, "bids": [],
                                     "awarded": None, "protocol": protocol,
                                     "options": options or [], "ballots": {}}
            self._log(run, {"kind": "help", "run": run, **entry})
            # notify need-tag subscribers (except the asker)
            for sub in sorted(self.subs.get((run, need), set())):
                if sub == frm:
                    continue
                self.seq += 1
                msg = {"id": self.seq, "ts": time.time(), "from": frm,
                       "to": sub,
                       "body": f"ASK {nid} needs {need}: "
                               f"{body[:HEAD_LEN]}"[:BODY_MAX]}
                self.inbox.setdefault((run, sub), []).append(msg)
                self._log(run, {"kind": "msg", **msg})
            return nid

    def _claim_live(self, a, now):
        # A claim holds only inside ASK_TTL; older claims are dead helpers
        # whose work anyone may steal. Lazy expiry — no background thread.
        return bool(a["claimed_by"]) and now - a["claimed_ts"] < ASK_TTL

    def open_asks(self, run, protocol=None):
        with self.lock:
            now = time.time()
            out = []
            for (r, nid), a in sorted(self.asks.items()):
                if r != run or now - a["ts"] >= ASK_TTL:
                    continue
                if protocol and a.get("protocol") != protocol:
                    continue
                if a.get("options"):
                    continue  # ballot asks are votable, not claimable
                if a["mode"] == "auction" and not a["awarded"]:
                    out.append({"id": nid, "from": a["from"],
                                "need": a["need"], "body": a["body"],
                                "mode": "auction",
                                "bids": len(a["bids"]),
                                "age_s": round(now - a["ts"], 1)})
                    continue
                if self._claim_live(a, now):
                    continue
                e = {"id": nid, "from": a["from"], "need": a["need"],
                     "body": a["body"], "mode": a["mode"],
                     "age_s": round(now - a["ts"], 1)}
                if a["claimed_by"]:
                    e["stale_claim"] = a["claimed_by"]
                out.append(e)
            return out

    def claim(self, run, nid, who, eta=0, note=""):
        with self.lock:
            a = self.asks.get((run, nid))
            if a is None:
                return {"ok": False, "error": "unknown request id"}
            if a.get("options"):
                return {"ok": False,
                        "error": "ballot ask — vote via /vote, tally via "
                                 "/tally"}
            now = time.time()
            if a["mode"] == "auction" and not a["awarded"]:
                # bidding: one live bid per member, replaceable
                a["bids"] = [b for b in a["bids"] if b["who"] != who]
                a["bids"].append({"who": who, "eta": eta, "note": note,
                                  "ts": now})
                self._log(run, {"kind": "bid", "run": run, "id": nid,
                                "from": who, "eta": eta})
                return {"ok": True, "bid": True,
                        "bids": len(a["bids"]),
                        "hint": "awaiting /award by " + a["from"]}
            if a["awarded"] and a["awarded"] != who:
                return {"ok": False, "awarded_to": a["awarded"]}
            if self._claim_live(a, now):
                return {"ok": False, "claimed_by": a["claimed_by"]}
            stolen = a["claimed_by"] if a["claimed_by"] else None
            a["claimed_by"] = who
            a["claimed_ts"] = now
            self._log(run, {"kind": "claim", "run": run, "id": nid,
                            "from": who,
                            **({"stole_from": stolen} if stolen else {})})
            res = {"ok": True, "request": {
                "id": nid, "from": a["from"], "need": a["need"],
                "body": a["body"]}}
            if stolen:
                res["stole_from"] = stolen
            return res

    def award(self, run, nid, who, winner):
        with self.lock:
            a = self.asks.get((run, nid))
            if a is None:
                return {"ok": False, "error": "unknown request id"}
            if a["from"] != who:
                return {"ok": False, "error": "only the asker awards"}
            if a["mode"] != "auction":
                return {"ok": False, "error": "not an auction-mode ask"}
            if a["awarded"]:
                return {"ok": False, "awarded_to": a["awarded"]}
            if not any(b["who"] == winner for b in a["bids"]):
                return {"ok": False,
                        "error": f"{winner} never bid"}
            a["awarded"] = winner
            a["claimed_by"] = winner
            a["claimed_ts"] = time.time()
            self._log(run, {"kind": "award", "run": run, "id": nid,
                            "from": who, "winner": winner})
            self.seq += 1
            msg = {"id": self.seq, "ts": time.time(), "from": who,
                   "to": winner,
                   "body": f"AWARDED {nid} to you: "
                           f"{a['body'][:HEAD_LEN]}"[:BODY_MAX]}
            self.inbox.setdefault((run, winner), []).append(msg)
            self._log(run, {"kind": "msg", **msg})
            return {"ok": True, "awarded": winner,
                    "bids": len(a["bids"])}

    def fail(self, run, nid, who, body):
        # asker-only: declare the request dead (kind=failure on board).
        with self.lock:
            a = self.asks.get((run, nid))
            if a is None:
                return {"ok": False, "error": "unknown request id"}
            if a["from"] != who:
                return {"ok": False, "error": "only the asker fails"}
            self.seq += 1
            entry = {"id": self.seq, "kind": "failure", "ts": time.time(),
                     "from": who, "protocol": a.get("protocol", "general"),
                     "body": f"FAILED {nid} ({a['need']}): {body}"}
            self.board.setdefault(run, []).append(entry)
            del self.asks[(run, nid)]
            self._log(run, {"kind": "failure", "run": run, **entry})
            return {"ok": True, "id": entry["id"]}

    def create_once(self, run, frm, key, thunk):
        # atomic check-and-create under one RLock hold.
        with self.lock:
            if key:
                hit = self.keys.get((run, frm, key))
                if hit is not None:
                    return {"id": hit, "replay": True}
            nid = thunk()
            if key and nid is not None:
                self.keys[(run, frm, key)] = nid
            return {"id": nid}

    def resolve(self, run, who, verdict, winners, losers, why, protocol):
        with self.lock:
            board = self.board.get(run, [])
            ids = {e["id"] for e in board}
            bad = [i for i in winners + losers if i not in ids]
            if bad:
                return {"ok": False, "error": f"unknown ids {bad}"}
            if not winners and not losers:
                return {"ok": False, "error": "empty verdict — name at "
                                              "least one entry"}
            if set(winners) & set(losers):
                return {"ok": False, "error": "entries cannot win and "
                                              "lose at once"}
            again = [e["id"] for e in board
                     if e["id"] in winners + losers and e.get("settled")]
            if again:
                return {"ok": False,
                        "error": f"already settled {again} — verdicts are "
                                 f"immutable; post a new one instead"}
            self.seq += 1
            nid = self.seq
            entry = {"id": nid, "kind": "verdict", "ts": time.time(),
                     "from": who, "protocol": protocol,
                     "verdict": verdict, "winners": winners,
                     "losers": losers,
                     "body": f"VERDICT {verdict} "
                             f"winners={winners} losers={losers}: {why}"}
            board.append(entry)
            for e in board:
                if e["id"] in winners + losers:
                    e["settled"] = True
            self._log(run, {"kind": "verdict", "run": run, **entry})
            return {"ok": True, "id": nid}

    @staticmethod
    def _words(text):
        # \w is unicode-aware: CJK bodies tokenize instead of vanishing.
        return {w for w in re.findall(r"\w{3,}", text.lower())}

    def near(self, run, body, top=3):
        with self.lock:
            hay = self._words(body)
            scored = []
            for e in self.board.get(run, []):
                w = self._words(e.get("body", ""))
                if not hay or not w:
                    continue
                j = len(hay & w) / len(hay | w)
                if j > 0:
                    scored.append((j, e["id"], e["from"],
                                  e.get("body", "")[:HEAD_LEN]))
            scored.sort(reverse=True)
            return [{"id": i, "score": round(j, 3), "from": f,
                     "head": h} for j, i, f, h in scored[:top]]

    def vote(self, run, nid, who, ranking):
        with self.lock:
            a = self.asks.get((run, nid))
            if a is None:
                return {"ok": False, "error": "unknown request id"}
            opts = a.get("options") or []
            if not opts:
                return {"ok": False,
                        "error": "not a ballot ask (no options=)"}
            if sorted(ranking) != sorted(opts):
                return {"ok": False,
                        "error": f"ranking must permute {opts}"}
            a["ballots"][who] = ranking
            self._log(run, {"kind": "vote", "run": run, "id": nid,
                            "from": who, "ranking": ranking})
            return {"ok": True, "ballots": len(a["ballots"])}

    def tally(self, run, nid, who):
        # Role-free (like /resolve): askers routinely exit before tallying,
        # so any member may close a ballot. Authorship stays honest via from.
        with self.lock:
            a = self.asks.get((run, nid))
            if a is None:
                return {"ok": False, "error": "unknown request id"}
            opts = a.get("options") or []
            if not opts or not a["ballots"]:
                return {"ok": False, "error": "no options or no ballots"}
            scores = {o: 0 for o in opts}
            for ranking in a["ballots"].values():
                for pts, opt in enumerate(reversed(ranking)):
                    scores[opt] += pts
            ranked = sorted(scores, key=lambda o: (-scores[o], o))
            winner = ranked[0]
            self.seq += 1
            fid = self.seq
            fentry = {"id": fid, "kind": "finding", "ts": time.time(),
                      "from": who, "protocol": a.get("protocol", "general"),
                      "claim": f"BALLOT {nid}: {winner} wins "
                               f"({len(a['ballots'])} ballots)",
                      "evidence": f"borda {scores}", "quote": "",
                      "etype": "asserted", "scope": "", "confidence": "high",
                      "body": f"BALLOT {nid}: {winner} wins | "
                              f"borda {scores} | "
                              f"ballots {len(a['ballots'])}"}
            self.board.setdefault(run, []).append(fentry)
            self._log(run, {"kind": "finding", "run": run, **fentry})
            self.seq += 1
            vid = self.seq
            ventry = {"id": vid, "kind": "verdict", "ts": time.time(),
                      "from": who, "protocol": a.get("protocol", "general"),
                      "verdict": "adopt", "winners": [fid],
                      "losers": [],
                      "body": f"VERDICT adopt (ballot): {winner} wins, "
                              f"losers={ranked[1:]}"}
            self.board.setdefault(run, []).append(ventry)
            for e in self.board.get(run, []):
                if e["id"] in (nid, fid):
                    e["settled"] = True  # ballot ask consumed, winner adopted
            del self.asks[(run, nid)]
            self._log(run, {"kind": "verdict", "run": run, **ventry})
            return {"ok": True, "winner": winner, "ranked": ranked,
                    "scores": scores, "finding": fid, "verdict": vid}

    def done(self, run, nid, who, body, protocol="general"):
        with self.lock:
            a = self.asks.get((run, nid))
            if a is None:
                return {"ok": False, "error": "unknown request id"}
            if a.get("options"):
                return {"ok": False,
                        "error": "ballot ask — resolve via /tally"}
            if a["mode"] == "auction" and not a["awarded"]:
                return {"ok": False, "error": "awaiting /award — bids only"}
            if a["claimed_by"] != who:
                return {"ok": False,
                        "error": f"claimed by {a['claimed_by']}"}
            self.seq += 1
            rid = self.seq
            entry = {"id": rid, "kind": "resolution", "ts": time.time(),
                     "from": who, "protocol": protocol,
                     "body": f"RESOLVED {nid} for {a['from']}: {body}"}
            self.board.setdefault(run, []).append(entry)
            del self.asks[(run, nid)]
            self._log(run, {"kind": "resolution", "run": run, **entry})
            # auto-notify the requester (directly via store: already locked)
            self.seq += 1
            msg = {"id": self.seq, "ts": time.time(), "from": who,
                   "to": a["from"],
                   "body": f"RESOLVED {nid}: {body}"[:BODY_MAX]}
            self.inbox.setdefault((run, a["from"]), []).append(msg)
            self._log(run, {"kind": "msg", **msg})
            return {"ok": True, "id": rid}


STORE = Store()


class Handler(BaseHTTPRequestHandler):
    server_version = "SwarmPing/1.0"

    def log_message(self, fmt, *args):  # single-line access log on stderr
        sys.stderr.write(f"{self.address_string()} {fmt % args}\n")
        sys.stderr.flush()

    def _qs(self):
        return parse_qs(urlparse(self.path).query)

    def _one(self, qs, key, default=None):
        v = qs.get(key, [default])[0]
        return v

    def _send_json(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _bad(self, msg):
        self._send_json(400, {"error": msg})

    def _echo(self, nid, body):
        b = body.encode("utf-8")
        out = {"id": nid, "len": len(b),
               "head": body[:HEAD_LEN]}
        if len(b) > BRIEF_BODY_MAX:
            out["warn"] = f"over-brief-discipline ({len(b)} utf-8 bytes " \
                          f"vs {BRIEF_BODY_MAX} advisory) — split into " \
                          f"smaller posts"
        return out

    def _proto(self, qs):
        p = self._one(qs, "protocol", "general")
        return p if p and NAME.match(p) else None

    def _topic(self, qs):
        t = self._one(qs, "topic", "")
        return t if t and NAME.match(t) else None

    def _body_ok(self, body):
        if len(body.encode("utf-8")) > BODY_MAX:
            return False
        return True

    def do_GET(self):
        path = urlparse(self.path).path
        qs = self._qs()
        if path == "/health":
            return self._send_json(200, {"ok": True})
        if path == "/endpoints":
            return self._send_json(200, {"endpoints": [
                "/health",
                "/endpoints",
                "/send?run=R&from=A&to=B|all&body=T[&client_key=K]",
                "/inbox?run=R&who=B",
                "/peek?run=R&who=B",
                "/post?run=R&from=A&body=T[&in_reply_to=N][&protocol=P][&client_key=K]",
                "/append?run=R&id=N&who=A&body=T",
                "/finding?run=R&from=A&claim=T[&evidence=E][&quote=Q][&etype=observed|asserted][&scope=S][&confidence=low|medium|high][&protocol=P][&client_key=K]",
                "/resolve?run=R&who=A&verdict=adopt|reject&winners=ids&losers=ids&why=T[&protocol=P]",
                "/near?run=R&body=T",
                "/vote?run=R&id=N&who=B&ranking=A,C,B",
                "/tally?run=R&id=N&who=A",
                "/board?run=R&since=N[&wait=S][&protocol=P][&kind=K]",
                "/retract?run=R&id=N&who=B",
                "/enter?run=R&who=B",
                "/leave?run=R&who=B[&note=T]",
                "/status?run=R&who=B&state=working|idle[&note=T]",
                "/lobby?run=R[&wait=S]",
                "/ask?run=R&from=A&need=TAG&body=T[&hop=0][&mode=fast|auction][&options=A,B,C][&protocol=P][&client_key=K]",
                "/open?run=R[&protocol=P]",
                "/claim?run=R&id=N&who=B[&eta=M][&note=T]",
                "/award?run=R&id=N&who=A&winner=B",
                "/done?run=R&id=N&who=B&body=T[&protocol=P]",
                "/fail?run=R&id=N&who=A&body=T",
                "/subscribe?run=R&who=B&topic=TAG",
                "/unsubscribe?run=R&who=B&topic=TAG",
                "/transcript?run=R",
            ]})
        if path in ("/send", "/inbox", "/peek", "/post", "/append",
                    "/finding", "/resolve", "/near", "/vote", "/tally",
                    "/board",
                    "/retract", "/enter", "/leave", "/status", "/lobby",
                    "/ask", "/open",
                    "/claim", "/award", "/done", "/fail", "/subscribe",
                    "/unsubscribe", "/transcript"):
            run = self._one(qs, "run", "")
            if not run or not NAME.match(run):
                return self._bad("bad run (want [A-Za-z0-9_-]{1,64})")
        if path == "/send":
            frm, to = self._one(qs, "from", ""), self._one(qs, "to", "")
            body = self._one(qs, "body", "")
            if not frm or not NAME.match(frm):
                return self._bad("bad from (your member id, e.g. from=m1)")
            if not to or (to != "all" and not NAME.match(to)):
                return self._bad("bad to (member id or 'all')")
            if not self._body_ok(body):
                return self._bad("body over 4KB — split into smaller posts")
            if not body:
                return self._bad("bad body (required, non-empty)")
            key = self._one(qs, "client_key", "")
            if key and not NAME.match(key):
                return self._bad("bad client_key (want "
                                 "[A-Za-z0-9_-]{1,64})")
            res = STORE.create_once(run, frm, key,
                                    lambda: STORE.send(run, frm, to, body))
            if res.get("replay"):
                return self._send_json(200, res)
            return self._send_json(200, self._echo(res["id"], body))
        if path in ("/inbox", "/peek"):
            who = self._one(qs, "who", "")
            if not who or not NAME.match(who):
                return self._bad("bad who")
            msgs = STORE.drain(run, who, peek=(path == "/peek"))
            return self._send_json(200, {"messages": msgs})
        if path == "/post":
            frm = self._one(qs, "from", "")
            body = self._one(qs, "body", "")
            if not frm or not NAME.match(frm):
                return self._bad("bad from (your member id, e.g. from=m1)")
            if not self._body_ok(body):
                return self._bad("body over 4KB — split into smaller posts")
            if not body:
                return self._bad("bad body (required, non-empty)")
            try:
                reply_to = int(self._one(qs, "in_reply_to", "0"))
            except ValueError:
                return self._bad("bad in_reply_to (want entry id)")
            protocol = self._proto(qs)
            if protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            key = self._one(qs, "client_key", "")
            if key and not NAME.match(key):
                return self._bad("bad client_key (want "
                                 "[A-Za-z0-9_-]{1,64})")
            res = STORE.create_once(
                run, frm, key,
                lambda: STORE.post(run, frm, body, reply_to, protocol))
            if res.get("replay"):
                return self._send_json(200, res)
            nid = res["id"]
            if nid is None:
                return self._bad(f"unknown reply target {reply_to} "
                                 f"in run {run}")
            return self._send_json(200, self._echo(nid, body))
        if path == "/board":
            try:
                since = int(self._one(qs, "since", "0"))
            except ValueError:
                return self._bad("bad since (want int)")
            try:
                wait = min(max(int(self._one(qs, "wait", "0")), 0),
                           WAIT_MAX)
            except ValueError:
                return self._bad(f"bad wait (want 0-{WAIT_MAX} seconds)")
            protocol = self._proto(qs)
            if "protocol" in qs and protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            if "protocol" not in qs:
                protocol = None
            kind = self._one(qs, "kind", "")
            if kind and kind not in ("note", "finding", "help",
                                     "resolution", "failure", "verdict"):
                return self._bad("bad kind (want note|finding|help|"
                                 "resolution|failure|verdict)")
            if not kind:
                kind = None
            entries = STORE.board_since(run, since, protocol, kind)
            deadline = time.time() + wait
            while not entries and time.time() < deadline:
                time.sleep(0.25)
                entries = STORE.board_since(run, since, protocol, kind)
            return self._send_json(200, {"entries": entries,
                                        "waited": round(wait - max(
                                            deadline - time.time(), 0), 1)})
        if path == "/resolve":
            who = self._one(qs, "who", "")
            verdict = self._one(qs, "verdict", "")
            why = self._one(qs, "why", "")
            def _ids(name):
                try:
                    return [int(x) for x in
                            self._one(qs, name, "").split(",") if x]
                except ValueError:
                    return None
            winners, losers = _ids("winners"), _ids("losers")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if verdict not in ("adopt", "reject"):
                return self._bad("bad verdict (want adopt|reject)")
            if winners is None or losers is None:
                return self._bad("bad winners/losers (want csv ids)")
            if not self._body_ok(why):
                return self._bad("why over 4KB")
            if not why:
                return self._bad("bad why (required, non-empty)")
            protocol = self._proto(qs)
            if protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            res = STORE.resolve(run, who, verdict, winners, losers,
                                why, protocol)
            if res["ok"]:
                out = self._echo(res["id"], why)
                return self._send_json(200, out)
            if res["error"].startswith("already settled"):
                return self._send_json(409, res)
            return self._bad(res["error"])
        if path == "/near":
            body = self._one(qs, "body", "")
            if not body:
                return self._bad("bad body (required)")
            if not self._body_ok(body):
                return self._bad("body over 4KB")
            return self._send_json(200, {"near": STORE.near(run, body)})
        if path == "/vote":
            who = self._one(qs, "who", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            ranking = [r for r in self._one(qs, "ranking", "").split(",")
                       if r]
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not ranking:
                return self._bad("bad ranking (want csv, e.g. A,C,B)")
            res = STORE.vote(run, nid, who, ranking)
            if res["ok"]:
                return self._send_json(200, res)
            return self._send_json(409, res)
        if path == "/tally":
            who = self._one(qs, "who", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            res = STORE.tally(run, nid, who)
            if res["ok"]:
                return self._send_json(200, res)
            return self._send_json(409, res)
        if path == "/retract":
            who = self._one(qs, "who", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if STORE.retract(run, nid, who):
                return self._send_json(200, {"retracted": nid})
            return self._bad(f"entry {nid} not found or not yours")
        if path == "/enter":
            who = self._one(qs, "who", "")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            return self._send_json(
                200, {"checked_in": STORE.enter(run, who)})
        if path == "/lobby":
            try:
                wait = min(max(int(self._one(qs, "wait", "0")), 0),
                           WAIT_MAX)
            except ValueError:
                return self._bad(f"bad wait (want 0-{WAIT_MAX} seconds)")
            first = STORE.lobby(run)
            deadline = time.time() + wait
            cur = first
            while time.time() < deadline:
                time.sleep(0.25)
                cur = STORE.lobby(run)
                if cur != first:  # lobby() returns value-fresh dicts
                    break
            out = dict(cur)
            out["waited"] = round(wait - max(deadline - time.time(), 0),
                                  1)
            return self._send_json(200, out)
        if path == "/leave":
            who = self._one(qs, "who", "")
            note = self._one(qs, "note", "")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not self._body_ok(note):
                return self._bad("note over 4KB")
            return self._send_json(200, STORE.leave(run, who, note))
        if path == "/status":
            who = self._one(qs, "who", "")
            state = self._one(qs, "state", "")
            note = self._one(qs, "note", "")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not self._body_ok(note):
                return self._bad("note over 4KB")
            res = STORE.set_status(run, who, state, note)
            if res is None:
                return self._bad("bad state (want working|idle)")
            if not res.get("ok"):
                return self._bad(res["error"])
            return self._send_json(200, res)
        if path == "/ask":
            frm = self._one(qs, "from", "")
            need = self._one(qs, "need", "")
            body = self._one(qs, "body", "")
            mode = self._one(qs, "mode", "fast")
            try:
                hop = int(self._one(qs, "hop", "0"))
            except ValueError:
                return self._bad("bad hop (want int)")
            if not frm or not NAME.match(frm):
                return self._bad("bad from (your member id, e.g. from=m1)")
            if not need or not NAME.match(need):
                return self._bad("bad need (short tag, e.g. need=verify)")
            if mode not in ("fast", "auction"):
                return self._bad("bad mode (want fast|auction)")
            if hop != 0:
                return self._bad("only hop=0 accepted — help results must "
                                 "not spawn new requests")
            if not self._body_ok(body):
                return self._bad("body over 4KB — split into smaller posts")
            if not body:
                return self._bad("bad body (required, non-empty)")
            protocol = self._proto(qs)
            if protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            options = [o for o in self._one(qs, "options", "").split(",")
                       if o]
            if any(not NAME.match(o) for o in options):
                return self._bad("bad options (csv of short tags)")
            key = self._one(qs, "client_key", "")
            if key and not NAME.match(key):
                return self._bad("bad client_key (want "
                                 "[A-Za-z0-9_-]{1,64})")
            res = STORE.create_once(
                run, frm, key,
                lambda: STORE.ask(run, frm, need, body, hop, mode,
                                  protocol, options))
            if res.get("replay"):
                return self._send_json(200, res)
            nid = res["id"]
            out = self._echo(nid, body)
            out["hint"] = "helpers claim via /claim; watch your /inbox " \
                          "for RESOLVED"
            return self._send_json(200, out)
        if path == "/open":
            protocol = self._proto(qs)
            if "protocol" in qs and protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            if "protocol" not in qs:
                protocol = None
            return self._send_json(200, {"open": STORE.open_asks(
                run, protocol)})
        if path == "/claim":
            who = self._one(qs, "who", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            try:
                eta = max(int(self._one(qs, "eta", "0")), 0)
            except ValueError:
                return self._bad("bad eta (want non-negative int)")
            note = self._one(qs, "note", "")
            if not self._body_ok(note):
                return self._bad("note over 4KB")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            res = STORE.claim(run, nid, who, eta, note)
            if res["ok"]:
                return self._send_json(200, res)
            if res.get("error", "").startswith("ballot ask"):
                return self._bad(res["error"])
            return self._send_json(409, res)
        if path == "/award":
            who = self._one(qs, "who", "")
            winner = self._one(qs, "winner", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not winner or not NAME.match(winner):
                return self._bad("bad winner (member id)")
            res = STORE.award(run, nid, who, winner)
            if res["ok"]:
                return self._send_json(200, res)
            return self._send_json(409, res)
        if path == "/done":
            who = self._one(qs, "who", "")
            body = self._one(qs, "body", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not self._body_ok(body):
                return self._bad("body over 4KB — split into smaller posts")
            if not body:
                return self._bad("bad body (required, non-empty)")
            protocol = self._proto(qs)
            if protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            res = STORE.done(run, nid, who, body, protocol)
            if res["ok"]:
                out = self._echo(res["id"], body)
                out["request"] = nid
                out["resolved"] = nid
                return self._send_json(200, out)
            return self._send_json(409, res)
        if path == "/fail":
            who = self._one(qs, "who", "")
            body = self._one(qs, "body", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not self._body_ok(body):
                return self._bad("body over 4KB")
            if not body:
                return self._bad("bad body (required, non-empty)")
            res = STORE.fail(run, nid, who, body)
            if res["ok"]:
                out = self._echo(res["id"], body)
                out["failed"] = nid
                return self._send_json(200, out)
            return self._send_json(409, res)
        if path == "/subscribe":
            who = self._one(qs, "who", "")
            topic = self._topic(qs)
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if topic is None:
                return self._bad("bad topic (want [A-Za-z0-9_-]{1,64})")
            return self._send_json(200, {"subscribed": STORE.subscribe(
                run, who, topic)})
        if path == "/unsubscribe":
            who = self._one(qs, "who", "")
            topic = self._topic(qs)
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if topic is None:
                return self._bad("bad topic (want [A-Za-z0-9_-]{1,64})")
            return self._send_json(200, {"subscribed": STORE.unsubscribe(
                run, who, topic)})
        if path == "/append":
            who = self._one(qs, "who", "")
            body = self._one(qs, "body", "")
            try:
                nid = int(self._one(qs, "id", ""))
            except (ValueError, TypeError):
                return self._bad("bad id (want int)")
            if not who or not NAME.match(who):
                return self._bad("bad who (your member id)")
            if not self._body_ok(body):
                return self._bad("body over 4KB — split into smaller posts")
            if not body:
                return self._bad("bad body (required, non-empty)")
            res = STORE.append(run, nid, who, body)
            if res["ok"]:
                out = self._echo(res["id"], body)
                out["entry_len"] = res["len"]
                return self._send_json(200, out)
            # same class as /retract's not-found: client-addressable 400
            return self._bad(res["error"])
        if path == "/finding":
            frm = self._one(qs, "from", "")
            claim = self._one(qs, "claim", "")
            evidence = self._one(qs, "evidence", "none")
            quote = self._one(qs, "quote", "")
            etype = self._one(qs, "etype", "asserted")
            scope = self._one(qs, "scope", "")
            confidence = self._one(qs, "confidence", "medium")
            if not frm or not NAME.match(frm):
                return self._bad("bad from (your member id, e.g. from=m1)")
            if not claim:
                return self._bad("bad claim (required, keep it one line)")
            if etype not in ("observed", "asserted"):
                return self._bad("bad etype (want observed|asserted)")
            if confidence not in ("low", "medium", "high"):
                return self._bad("bad confidence (want low|medium|high)")
            for field, name in ((claim, "claim"), (evidence, "evidence"),
                                (quote, "quote"), (scope, "scope")):
                if len(field.encode("utf-8")) > BODY_MAX:
                    return self._bad(f"{name} over 4KB")
            protocol = self._proto(qs)
            if protocol is None:
                return self._bad("bad protocol (want [A-Za-z0-9_-]{1,64})")
            key = self._one(qs, "client_key", "")
            if key and not NAME.match(key):
                return self._bad("bad client_key (want "
                                 "[A-Za-z0-9_-]{1,64})")
            res = STORE.create_once(
                run, frm, key,
                lambda: STORE.finding(run, frm, claim, evidence, quote,
                                      etype, scope, confidence, protocol))
            if res.get("replay"):
                return self._send_json(200, res)
            nid = res["id"]
            if nid is None:
                return self._bad("composed entry over 4KB — shorten fields")
            return self._send_json(200, self._echo(nid, claim))
        if path == "/transcript":
            return self._send_json(200, {"transcript": STORE.transcript(
                run)})
        return self._send_json(404, {"error": "unknown endpoint — "
                                             "see /endpoints"})


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8471
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"swarm-ping listening on 127.0.0.1:{port} "
          f"swarmdir={SWARM_DIR}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
