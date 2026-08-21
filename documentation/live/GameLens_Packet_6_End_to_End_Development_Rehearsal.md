# GameLens Packet 6 — End-to-End Development Rehearsal

**Status:** Planning checkpoint — review before code
**Created:** 2026-08-20
**Branch:** `dev`
**Predecessor:** [Packet 5 — Admin and Run Visibility](./GameLens_Packet_5_Admin_and_Run_Visibility.md)
**Sprint authority:** [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md)
**Production behavior changed:** No
**Production learning data written:** No
**New table authorized:** No

---

## The short version

Packets 2–4 proved the individual pregame and postgame workers. Packet 5 made
their evidence visible. Packet 6 should now prove that the existing pieces can
be run together over an upcoming preseason slate without building a general
workflow platform or depending on one razor-thin execution time.

The first implementation should be one small, development-only, rerunnable
coordinator. Each invocation inspects a bounded date range, runs only work that
is safely eligible at that moment, and returns honest `waiting`, `no_op`,
`success`, `partial_failure`, or `failure` evidence. It does not remain alive
between pregame and postgame clocks.

```text
Run the same bounded rehearsal command when useful
                    |
                    v
          Inspect current game state
             /                 \
   eligible before kickoff   eligible after final data
            |                         |
            v                         v
   existing snapshot + L1    existing grade + L2 + L3
             \                       /
              v                     v
              existing receipts + Packet 5 visibility
```

Running early, late, or more than once must not itself be an error. A game that
is not ready returns a reason and can be reconsidered by a later invocation.

---

## 1. Product goal

Follow real upcoming preseason games through the already implemented Level
1–3 path and make the result visible without touching production learning.

Packet 6 should answer one practical question:

> If Christian runs one bounded development command now, does GameLens safely
> perform the work that is eligible, explain everything else, and allow the
> same slate to continue later without duplicate or contaminated evidence?

This is an operational rehearsal, not Level 4 and not production activation.

---

## 2. Keep the first version small

The coordinator should:

1. accept the approved development runtime, season, season type, stable
   `learning_run_id`, and bounded date range;
2. read the scheduled games and current canonical receipts;
3. reuse the existing Packet 2 snapshot/Level 1 path for safely eligible
   pregame work;
4. reuse the existing Packet 4 coordinator for safely eligible final games;
5. leave waiting, already-complete, missing-capture, and incomplete-data games
   visible with reasons;
6. isolate one failed game from healthy siblings;
7. return one readable slate summary plus per-game stage results; and
8. let Packet 5 display the resulting canonical evidence.

The first version should not introduce a plugin system, workflow DSL, dynamic
dependency graph, queue, daemon, long-running process, or exact-minute timing
framework.

---

## 3. Timing posture

Packet 6 is eligibility-based rather than stopwatch-based.

- Before kickoff, a game may capture only inside the already approved safe
  pregame rules.
- Before that window, the result is `waiting`, not failure.
- A temporary upstream problem remains retryable while the safe window is open.
- After kickoff, an absent canonical snapshot becomes a Known Gap; Packet 6
  must not reconstruct it.
- Postgame work waits until the game is final and the required completed-game
  data is available.
- A later invocation can continue games that were previously waiting.
- An identical retry must not duplicate canonical rows or receipts.

There is no requirement for one process to sleep until kickoff, wait through
the game, or remain alive until the next 8:00 a.m. load. Pregame and postgame
invocations are connected by stable `learning_run_id`, `game_id`, and
`capture_id` evidence rather than one multi-day attempt ID.

---

## 4. Reuse boundaries

| Responsibility | Reuse owner | Packet 6 rule |
|---|---|---|
| Scheduled-game selection | Existing Schedule query and Packet 2 scope rules | Do not create a second schedule model |
| Pregame payload | Existing game service used by Snapshot Capture | Do not reproduce `/game` assembly |
| Snapshot and Level 1 | Packet 2 coordinator plus Packet 3 shared extractor/write boundary | Do not create a second claim path |
| Final game grade | Packet 4 capture-aware grader/storage boundary | Do not call the older unbounded save path |
| Level 2 and Level 3 | Packet 4 bounded workers/coordinator | Preserve their existing order and failure stop |
| Receipts | Existing `stage_runs`, `stage_game_results`, and `postgame_learning_stage_receipts` | Do not consolidate them into a new warehouse |
| Visual QA | Packet 5 protected read service and accepted wireframe | Do not duplicate stage classification in the frontend |

Packet 6 may add a thin callable coordinator and a QA/CLI wrapper. It does not
have authority to create a seventh GameLens development table unless a concrete
grain gap is documented and separately reviewed.

---

## 5. Simple invocation model

The initial command shape may remain provisional until code inspection, but it
should support one bounded manual rehearsal similar to:

```text
season
season_type
learning_run_id
start_date
end_date
development-only confirmation
dry-run or approved development-write mode
```

One invocation may cover one game or many games. Its summary should distinguish:

- games inspected;
- games already complete;
- games newly processed;
- games waiting for a safe boundary;
- known historical gaps;
- retryable failures; and
- non-retryable failures.

Packet 6 should favor a clear result over an exhaustive configuration surface.

---

## 6. Rehearsal checkpoints

### Checkpoint A — read-only slate preview

Before a write is allowed, inspect an upcoming bounded slate and show, per
game, which work would run now and which work would wait.

### Checkpoint B — pregame development rehearsal

Run the approved existing capture/Level 1 boundary for eligible games. Inspect
the canonical captures and receipts through Packet 5.

### Checkpoint C — postgame development rehearsal

After the games are final and the normal completed-game data is available, run
the existing Packet 4 grade/Level 2/Level 3 coordinator for eligible captured
games.

### Checkpoint D — identical retry

Repeat the same bounded invocation. Canonical learning rows must not duplicate,
and existing immutable receipts must reconcile honestly.

### Checkpoint E — visual acceptance

Use the Packet 5 protected endpoint and accepted Admin wireframe to inspect the
week, games, clocks, stages, reasons, and attempts. Code-only success is not
enough for this checkpoint.

---

## 7. Genuine claims remain an operational gate

All seven previously available preseason captures contained zero claims. Those
zero-claim results are valid and must remain valid no-ops.

If the new rehearsal produces genuine Level 1 claims, Packet 6 should run the
already documented Packet 3 populated write/retry reconciliation and then
observe those rows through Levels 2 and 3. It must not introduce a special
claim or alter the extractor to force that condition.

If the new slate again produces zero claims, Packet 6 may still prove the
orchestration behavior, but the first genuine claim-bearing validation remains
on the pre-production gate list.

---

## 8. Level 4 future seam

Packet 6 should leave a straightforward place for a later weekly stage to be
called, but it must not design or implement Level 4 prematurely.

The minimum future-proofing is:

- keep stage identity explicit;
- preserve whether work is game/slate scoped or weekly scoped;
- return structured counts, reasons, and status; and
- avoid hard-coding an assumption that every stage runs once per game.

Packet 7 will own Level 4's weekly eligibility, evidence threshold, output,
storage, and advisory-only behavior. Packet 6 should not create a fake
per-game Level 4 receipt merely to make the coordinator look complete.

---

## 9. Packet 6 does not do

Packet 6 does not:

- create production GameLens tables;
- write learning evidence outside approved development targets;
- change `app.py`, Scheduler, or the production 8:00 a.m. load;
- add a second Scheduler or long-running service;
- change `/game` or the public frontend;
- manufacture claims or reconstruct missed captures;
- reimplement grading, Level 1, Level 2, Level 3, or Calibrated Matchup Lean;
- run or implement Level 4;
- treat preseason rows as regular-season production learning evidence;
- turn waiting into failure merely because the command was run early; or
- require a new table without a separately proven grain gap.

---

## 10. Implementation GO evidence

Packet 6 receives Implementation GO only when:

1. this plan is reviewed before code;
2. the first slice is a read-only eligibility preview;
3. the coordinator calls existing workers rather than reproducing them;
4. a bounded upcoming slate is rehearsed through the available pregame and
   postgame clocks;
5. early/not-ready games wait visibly instead of failing;
6. an individual failure does not hide or block healthy sibling games;
7. an identical retry duplicates no canonical learning row;
8. missing captures remain historical gaps and are never reconstructed;
9. Packet 5 makes the resulting game/stage evidence visually understandable;
10. zero claims remain a valid no-op, while any genuine claims receive the
    existing Packet 3–4 reconciliation;
11. no production, Scheduler, `/game`, frontend, or Level 4 behavior changes;
    and
12. the exact commands, attempt IDs, counts, failures, retries, and remaining
    gates are documented.

Implementation GO authorizes only the development rehearsal coordinator and
its evidence. It does not authorize Level 4 or production activation.

---

## 11. Handoff after Packet 6

After Packet 6 proves the real Level 1–3 operational path, create and review:

- **Packet 7 — Level 4 controlled weekly learning**; then
- **Packet 8 — Production activation and rollback**.

Production wiring remains last.
