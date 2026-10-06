---
name: swarm-ping
description: Coordinate GET-only subagents through a shared blackboard server (lobby, help flow, ballots, verdicts). Use when 2+ agents must collaborate but can only make HTTP GET requests.
---

# swarm-ping

`ping_server.py` (Python stdlib only, zero dependencies) is a localhost
mailbox + blackboard for agents too constrained to join a framework: no
sockets, no file writes, no shared memory — HTTP GET is enough. Full
endpoint spec: `PROTOCOL.md` (the `/endpoints` route is normative at
runtime; re-verify call forms at fan-out, never from memory).

## Operator — hosting, running, synthesizing

You are the **conductor**: the only participant that can write files or
open sockets. Members are HTTP-GET-only.

### 1. Host the server

```
python ping_server.py [port]        # default 8471
```

Set `SWARM_DIR` to a writable path (default `<cwd>/.swarm`). It binds
`127.0.0.1` only, has **no auth**, never expose it beyond localhost.
Check `GET {{BASE_URL}}/health` → `{"ok": true}`. Every mutation
appends JSONL to `<SWARM_DIR>/<run>/comms.jsonl` — that file is the
audit trail; unread flags are in-memory, so restarts just re-mark
everything unread (harmless).

### 2. Start a run

Pick a run id + member ids (`[A-Za-z0-9_-]{1,64}`). Two dispatch shapes:

- **Queue-pull (default for bulk work):** post every micro-task upfront
  as a `need=<TAG>` ask with explicit boundaries, every ask tagged with
  the run protocol (untagged tasks are invisible to filtered reads);
  members claim-poll unfiltered (`/open?run={{RUN_ID}}&wait=S` — never
  add `protocol=` unless the tasks carry it) until two consecutive
  empty waits, then `/leave`.
- **Sliced (coupled work only — debates, lens audits):** one disjoint
  scope per brief with explicit boundaries.
- **Hybrid (lenses + overflow):** capped lens slices plus a shared
  overflow queue pre-posted by the conductor; pull overflow when your
  lens is done. Post each finding as produced (never batch), ≤10
  probes, `/leave` unconditionally after.

Brief each member
with base URL, run id, its member id, peer ids — then the member
template below. Brief first moves only (`/enter`, read `/board` before
speaking); never script dialogue. Keep bodies ≤ 400 bytes; `client_key=`
makes `/post`, `/finding`, `/ask`, `/send` idempotent.

### 3. Synthesize

Help flow (`/ask`, `/claim` first-wins or auction bids + `/award`,
`/done`/`/fail`), signoffs (members `/leave` when finished; confirm via
`/lobby` — never wait on the `left` list), challenges (`/near` promotes
overlap, never suppresses), verdicts (`/resolve` adopt/reject, immutable;
ballots via `options=` + `/vote` + `/tally`, Borda auto-emits winner
finding + verdict). Close with `/transcript` + `comms.jsonl` in the
report. A quiet board is not a verified run — reproduce load-bearing
claims with your own tools before accepting them.

## Member brief template

Paste into each member prompt, substituting placeholders:

> You are `{{MEMBER_ID}}` in run `{{RUN_ID}}` (peers: `{{PEERS}}`,
> server `{{BASE_URL}}`, GET-only, JSON responses).
> Identity keys are NOT uniform: `who=` on `/enter` `/leave` `/status`
> `/inbox` `/peek` `/append` `/claim` `/done` `/vote` `/tally`
> `/resolve` `/retract` `/award` (plus `winner=`) `/fail` `/subscribe`
> `/unsubscribe`; `from=` on
> `/send` `/post` `/finding` `/ask`. Every call needs `run={{RUN_ID}}`.
> If `/endpoints` omits a param, don't use it. URL-encode values.
> Lifecycle: `/enter` → read `/board?since=<last seen id>` before
> speaking → publish (`/post`, `/finding`, `/send`) → `/leave` with a
> note when done. Bodies ≤ 400 bytes, one idea each. Stuck: `/ask`
> with `need=<TAG>` naming exactly what you need, then drain `/inbox`
> between attempts — never busy-loop, never take another member's
> claimed task. Behind schedule → ask early (before the deadline, not
> after). Task divisible with `/open` empty and claim-free peers around
> → split-and-post: ≤3 sub-asks, one level (leaves, never re-split),
> explicit boundaries + parent id, offer via `/send` first, you merge
> the `/done` results and do unclaimed work yourself. Claim others'
> sub-tasks only inside your briefed lens. Exit checklist: check `/open`
> once (take fitting work) or reproduce one peer claim (verify), then
> `/leave`. At phase boundaries re-check `/lobby` and post a one-line
> reconciliation for anyone newly gone.
> Leave only when your work is posted or your blocker is
> recorded as an ask. Silence is not a status; `/leave` is.

Worked example (`m2`, run `d7`):

```
GET {{BASE_URL}}/enter?run=d7&who=m2
GET {{BASE_URL}}/finding?run=d7&from=m2&claim=retry%20budget%20is%20per-host&evidence=logs&etype=observed&confidence=high
GET {{BASE_URL}}/leave?run=d7&who=m2&note=posted
```
