"""swarm-ping demo — lobby, debate, ballot, help flows. Stdlib only.

Usage:  python ping_server.py 8471   (one terminal)
        SWARM_PING_URL=http://127.0.0.1:8471 python examples/demo.py
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("SWARM_PING_URL", "http://127.0.0.1:8471")
RUN = os.environ.get("SWARM_PING_RUN", "demo")


def get(path, **qs):
    url = BASE + path + "?" + urllib.parse.urlencode(qs)
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        return {"http_error": e.code, "body": e.read().decode()}


def need(res, key, what):
    # error-path accessor: demo failures print the captured HTTP error
    # instead of raising KeyError on a missing key.
    if key not in res:
        raise SystemExit(f"demo failed at {what}: {res!r}")
    return res[key]


def show(label, res):
    print(label + ":", json.dumps(res)[:160])


# lobby: check in, advisory status, barrier
show("enter m1", get("/enter", run=RUN, who="m1"))
show("enter m2", get("/enter", run=RUN, who="m2"))
show("status", get("/status", run=RUN, who="m1", state="working",
                   note="driving demo"))
show("lobby", get("/lobby", run=RUN))

# direct message + drain
show("send", get("/send", run=RUN, **{"from": "m1", "to": "m2",
                   "body": "your turn to vote"}))
show("inbox m2", get("/inbox", run=RUN, who="m2"))

# board debate: post, reply, continue, similarity
p1 = get("/post", run=RUN, **{"from": "m1",
           "body": "phalaenopsis forgives beginners"})
show("post", p1)
pid = need(p1, "id", "post")
show("reply", get("/post", run=RUN, **{"from": "m2",
                   "body": "agreed, with bright-light caveat",
                   "in_reply_to": pid}))
show("append", get("/append", run=RUN, id=pid, who="m1",
                   body="blooms last months"))
show("near", get("/near", run=RUN, body="beginner orchid light"))
show("finding", get("/finding", run=RUN, **{"from": "m1",
                    "claim": "phalaenopsis leads", "evidence": "board",
                    "etype": "observed", "confidence": "high"}))

# ballot: ask, disagreeing votes (tie-break visible), role-free tally
ask = get("/ask", run=RUN, **{"from": "m1", "need": "pick",
           "body": "best beginner orchid",
           "options": "phalaenopsis,cattleya,dendrobium"})
aid = need(ask, "id", "ballot ask")
show("ask id", aid)
show("vote m1", get("/vote", run=RUN, id=aid, who="m1",
                    ranking="phalaenopsis,dendrobium,cattleya"))
show("vote m2", get("/vote", run=RUN, id=aid, who="m2",
                    ranking="dendrobium,phalaenopsis,cattleya"))
show("tally", get("/tally", run=RUN, id=aid, who="m2"))

# error path, exercised: bogus state trips the 400 branch
show("bad state", get("/status", run=RUN, who="m1", state="napping"))

# fast help flow: subscribe first so the NEED notice lands, then
# ask, open, claim, done
show("subscribe", get("/subscribe", run=RUN, who="m1", topic="verify"))
h = get("/ask", run=RUN, **{"from": "m2", "need": "verify",
         "body": "confirm borda math"})
hid = need(h, "id", "help ask")
show("help ask id", hid)
show("open", get("/open", run=RUN))
show("claim", get("/claim", run=RUN, id=hid, who="m1", eta=1))
show("done", get("/done", run=RUN, id=hid, who="m1",
                 body="3/3/0 tie, dendrobium wins (name tiebreak)"))
show("inbox m1", get("/inbox", run=RUN, who="m1"))

# failed ask + transcript + clock-out
f = get("/ask", run=RUN, **{"from": "m1", "need": "moot",
         "body": "withdrawn question"})
show("fail", get("/fail", run=RUN, id=need(f, "id", "moot ask"), who="m1",
                 body="answered elsewhere"))
show("transcript chars",
     len(need(get("/transcript", run=RUN), "transcript", "transcript")))
show("leave m1", get("/leave", run=RUN, who="m1", note="demo done"))
show("leave m2", get("/leave", run=RUN, who="m2", note="demo done"))
show("lobby", get("/lobby", run=RUN))
