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
show("reply", get("/post", run=RUN, **{"from": "m2",
                   "body": "agreed, with bright-light caveat",
                   "in_reply_to": p1["id"]}))
show("append", get("/append", run=RUN, id=p1["id"], who="m1",
                   body="blooms last months"))
show("near", get("/near", run=RUN, body="beginner orchid light"))
show("finding", get("/finding", run=RUN, **{"from": "m1",
                    "claim": "phalaenopsis leads", "evidence": "board",
                    "etype": "observed", "confidence": "high"}))

# ballot: ask, disagreeing votes (tie-break visible), role-free tally
ask = get("/ask", run=RUN, **{"from": "m1", "need": "pick",
           "body": "best beginner orchid",
           "options": "phalaenopsis,cattleya,dendrobium"})
aid = ask["id"]
show("ask", aid)
show("vote m1", get("/vote", run=RUN, id=aid, who="m1",
                    ranking="phalaenopsis,dendrobium,cattleya"))
show("vote m2", get("/vote", run=RUN, id=aid, who="m2",
                    ranking="dendrobium,phalaenopsis,cattleya"))
show("tally", get("/tally", run=RUN, id=aid, who="m2"))

# fast help flow: ask, open, claim, done
h = get("/ask", run=RUN, **{"from": "m2", "need": "verify",
         "body": "confirm borda math"})
show("help ask", h["id"])
show("open", get("/open", run=RUN))
show("claim", get("/claim", run=RUN, id=h["id"], who="m1", eta=1))
show("done", get("/done", run=RUN, id=h["id"], who="m1",
                 body="3/3/0 tie, alpha wins"))

# failed ask + subscription + transcript + clock-out
f = get("/ask", run=RUN, **{"from": "m1", "need": "moot",
         "body": "withdrawn question"})
show("fail", get("/fail", run=RUN, id=f["id"], who="m1",
                 body="answered elsewhere"))
show("subscribe", get("/subscribe", run=RUN, who="m2", topic="verify"))
show("transcript chars", len(get("/transcript", run=RUN)["transcript"]))
show("leave m1", get("/leave", run=RUN, who="m1", note="demo done"))
show("leave m2", get("/leave", run=RUN, who="m2", note="demo done"))
show("lobby", get("/lobby", run=RUN))
