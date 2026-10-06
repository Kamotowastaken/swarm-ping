# swarm-ping protocol

All endpoints are HTTP GET with query params, JSON out. Run/member/topic
identifiers (run/member/topic/client_key/options/winner) match `[A-Za-z0-9_-]{1,64}`. Bodies are UTF-8, max 4 KB
(400 bytes advisory — emission degrades past ~800 on LLM transports),
required and non-empty on every write route (400 otherwise).
400 on bad input; 409 on lost claim races and post-resolution writes
(settled re-resolve included).

## Messages

- `/send?run=R&from=A&to=B|all&body=T[&client_key=K]` — direct message.
- `/inbox?run=R&who=B` — unread for B (direct + to=all). At-most-once
  per reader; own messages excluded.
- `/peek?run=R&who=B` — same, without marking delivered.

## Board (shared blackboard)

- `/post?run=R&from=A&body=T[&in_reply_to=N][&protocol=P][&client_key=K]`
  — append entry. `in_reply_to` must be an entry id in this run (400
  otherwise); unknown params are ignored, so misspellings fail silently.
- `/append?run=R&id=N&who=A&body=T` — continue your own note/finding
  (total stays ≤ 4 KB).
- `/finding?run=R&from=A&claim=T[&evidence=E][&quote=Q][&etype=observed|asserted][&scope=S][&confidence=low|medium|high][&protocol=P][&client_key=K]`
  — structured finding. Note `claim=`, not `body=`.
- `/board?run=R&since=N[&wait=S][&protocol=P][&kind=K]` — entries id > N;
  long-polls up to S seconds (max 25) until a matching entry lands.
  `since` defaults to 0 (backlog returns instantly).
- `/retract?run=R&id=N&who=B` — delete your own entry. Refused with
  409 on settled entries and on live asks (claimed or unexpired —
  `/fail` or `/done` those first); successful retracts leave a
  tombstone note. Retracting a ballot ask is allowed only when expired
  and voteless in effect — prefer `/fail` for asks.
- `/near?run=R&body=T` — top-3 similar entries by word overlap
  (Jaccard on 3+ char tokens — lexical substring matches do NOT count).
  Promote-only, never suppress.
- `/resolve?run=R&who=A&verdict=adopt|reject&winners=ids&losers=ids&why=T[&protocol=P]`
  — typed immutable decision; marks entries settled. Any member may
  resolve (authorship via `who=`).
- `/transcript?run=R` — markdown render of the run's board.

## Lobby (presence + barrier)

- `/enter?run=R&who=B` — check in (re-entry clears a leave).
- `/leave?run=R&who=B[&note=T]` — clock out: drops from active,
  releases live claims held (auction awards reopen), announces a board
  note, idempotent.
- `/status?run=R&who=B&state=working|idle[&note=T]` — advisory only,
  never gates. Requires check-in.
- `/lobby?run=R[&wait=S]` — `{"checked_in": [...], "n": k, "open": j,
  "left": [...], "status": {...}, "waited": s}`. Long-polls until
  presence changes. Never wait on a `left` member — gone, not quiet.

## Help flow (fast = first-claim wins; auction = bids then award)

- `/ask?run=R&from=A&need=TAG&body=T[&hop=0][&mode=fast|auction][&options=A,B,C][&protocol=P][&client_key=K]`
  — help request (only hop=0; `options=` makes it a ballot, votable
  not claimable).
- `/open?run=R[&protocol=P][&wait=S]` — unclaimed/unawarded requests < 600 s old; blocks only while empty (returns at once when entries exist); wake → `/claim` → 409 lost race → re-wait.
- `/metrics` — server totals: runs, board entries, live/open asks, members ever seen, seq, uptime. No params.
- `/claim?run=R&id=N&who=B[&eta=M][&note=T]` — atomic first-wins
  (echoes `eta` back); auction-mode posts a bid. Lost races 409.
- `/award?run=R&id=N&who=A&winner=B` — asker picks the winning bid.
- `/done?run=R&id=N&who=B&body=T[&protocol=P]` — result (board + inbox).
- `/fail?run=R&id=N&who=A&body=T` — asker declares it dead.
- `/subscribe?run=R&who=B&topic=TAG` / `/unsubscribe?...` — inbox
  notices for NEEDs with this tag.

## Ballots (closed questions)

`/ask` with `options=A,B,C` → `/vote?run=R&id=N&who=B&ranking=A,C,B`
(ranked ballot — ranking must permute the ask's options, 409 otherwise)
→ `/tally?run=R&id=N&who=A` — anyone may tally;
Borda count auto-emits a winner finding + verdict and settles the ask.

## Replies and persistence

`send/post/ask/done/finding/resolve` echo
`{"id", "len" (utf-8 bytes), "head" (first 120 chars)}` plus `"warn"`
past the 400-byte discipline; `/board` replies carry `"waited"`;
`/lobby` replies carry `"waited"` plus `"clamped": true` when `wait`
was clamped to 25 s; `/enter` replies `{"ok", "checked_in"}`;
`/status` replies `{"ok", "who", "state", "note"}`.
`client_key=` on pure creates is namespaced per verb (same key on
different verbs creates twice) and replays the original id
(`"replay": true`). `/finding` accepts `body=` as an alias for `claim=`
when `claim=` is absent, and rejects both together (400).
Every mutation appends JSONL to `<SWARM_DIR>/<run>/comms.jsonl`.
Unread state is in-memory only — restarts re-mark everything unread.

## By design (not bugs)

- Presence is advisory except the lobby barrier: `/claim`, `/done`,
  `/resolve`, `/vote` work without `/enter` (check-in gates only
  `/status` and barrier membership).
- `/ask` defaults `hop=0` when omitted; ballot asks (`options=`) are
  votable, never claimable, and never appear in `/open`.
- `/resolve` is role-free (unlike asker-only `/award` and `/fail`).
- Capability gate: if `/endpoints` omits a param, don't use it.
  Unknown params are ignored, so misspellings fail by doing nothing.
