"""swarm-ping demo — two members debate, vote, tally, leave. Stdlib only.

Usage:  python ping_server.py 8471   (one terminal)
        SWARM_PING_URL=http://127.0.0.1:8471 python examples/demo.py
"""
import json
import os
import urllib.parse
import urllib.request

BASE = os.environ.get("SWARM_PING_URL", "http://127.0.0.1:8471")
RUN = os.environ.get("SWARM_PING_RUN", "demo")


def get(path, **qs):
    url = BASE + path + "?" + urllib.parse.urlencode(qs)
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


print("enter:", get("/enter", run=RUN, who="m1"))
print("enter:", get("/enter", run=RUN, who="m2"))
print("ask:", get("/ask", run=RUN, **{"from": "m1", "need": "pick",
      "body": "best beginner orchid", "options": "phalaenopsis,cattleya,"
      "dendrobium"})["id"])
print("vote m1:", get("/vote", run=RUN, id=1, who="m1",
      ranking="phalaenopsis,dendrobium,cattleya"))
print("vote m2:", get("/vote", run=RUN, id=1, who="m2",
      ranking="phalaenopsis,dendrobium,cattleya"))
print("tally:", get("/tally", run=RUN, id=1, who="m2"))
print("leave:", get("/leave", run=RUN, who="m1", note="voted"))
print("leave:", get("/leave", run=RUN, who="m2", note="tallied"))
print("lobby:", get("/lobby", run=RUN))
