# swarm-ping

A zero-dependency coordination server for agents too constrained to join
a framework: no sockets, no file writes, no shared memory — if an agent
can make an HTTP GET request, it can rendezvous, debate, vote, and hand
off work. Python stdlib only. No auth, localhost-only by design.

## Why this exists

Most multi-agent frameworks (CrewAI, AutoGen/Microsoft Agent
Framework, LangGraph) assume capable agents: function calling, shared
memory, persistent processes. Blackboard-architecture research (bMAS,
arXiv:2507.01701) and tuple-space systems assume the same. `swarm-ping`
was built for the opposite corner: **sandboxed subagents whose only
network access is fetching URLs**. The constraints shaped the design:

- **Long-poll as IPC** — `/board` and `/lobby` block until state changes,
  so agents with no sleep primitive and no inbox push still rendezvous
  without busy-spinning.
- **Atomic claims with stealing** — first-claim-wins plus TTL expiry, so
  a crashed helper's work reopens instead of deadlocking.
- **Clock-out with claim release** — `/leave` drops presence *and*
  frees held work; peers check `/lobby`'s `left` list instead of waiting
  on ghosts.
- **Decisions as data** — ranked ballots with Borda tally auto-emitting
  winner findings + immutable verdicts; anyone may tally, so an exited
  asker never blocks the close.
- **Everything audited** — every mutation appends JSONL per run.

This grew out of a real deployment: a 7-member research swarm whose
members debated, stress-tested, and audited the protocol itself across
a dozen instrumented runs — including catching the protocol's own drift
and its members' confabulated evidence. The scars are in the spec.

## Quickstart

```
python ping_server.py 8471
SWARM_PING_URL=http://127.0.0.1:8471 python examples/demo.py
```

`SWARM_PING_URL` points at the server; `SWARM_PING_RUN` names the run
(default `demo` — set it to something unique when pointing at a shared
server, or the demo's `m1`/`m2` entries land in someone else's run).
Two members enter, vote on a ballot, tally it, leave. Then read
`PROTOCOL.md` (full endpoint spec) and `skills/swarm-ping/SKILL.md`
(conductor + member-brief templates for any harness).

## What it is / isn't

- IS: a coordination layer — mailbox, blackboard, barrier, ballots.
  Language-agnostic (curl works), harness-agnostic (any subagent system).
- ISN'T: an agent framework. It doesn't run models, call tools, or plan.
  It won't scale past a roomful of agents, and unread state is
  in-memory (restarts re-mark everything unread — by design, harmless).

## Known limits (tracked as issues)

- Settled flags mutate entries in place — long-pollers never learn an
  old id settled ([#5](https://github.com/Kamotowastaken/swarm-ping/issues/5)).
- Python reprs leak into some wire strings ([#6](https://github.com/Kamotowastaken/swarm-ping/issues/6)).
- Idempotency keys are per-member, not per-verb ([#1](https://github.com/Kamotowastaken/swarm-ping/issues/1)).
- No perf headroom past room scale: file IO under the global lock,
  linear scans, no id index ([#2](https://github.com/Kamotowastaken/swarm-ping/issues/2)).
- Departed members' bids/ballots survive them; asker-only award can
  wedge ([#3](https://github.com/Kamotowastaken/swarm-ping/issues/3)).
- Status-code matrix has deliberate rough edges ([#4](https://github.com/Kamotowastaken/swarm-ping/issues/4)).

## License

MIT — see LICENSE.
