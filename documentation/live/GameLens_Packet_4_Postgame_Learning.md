# GameLens Packet 4 — Postgame Outcome plus Levels 2–3

**Status:** Ready to begin on 2026-08-17 — plan reviewed; no Packet 4 code or cloud write has started  
**Created:** 2026-08-16  
**Branch:** `dev`  
**Predecessor:** [Packet 3 — Production-Safe Level 1](./GameLens_Packet_3_Production_Level_1.md)  
**Sprint authority:** [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md)  
**Production behavior changed:** No  
**Separate prerequisite complete:** Calibrated Matchup Lean was released and forward-merged on 2026-08-16; Packet 4 must consume its stored result, not recalculate it  

---

## The short version

Packet 3 preserved what GameLens said before kickoff. Packet 4 connects that
frozen prediction to the completed game without rewriting history.

```text
Frozen pregame capture
        + final score
        -> Model Outcome / Trust grade

Frozen Level 1 claims
        + final score
        + accepted postgame Facts
        -> Level 2 validation
        -> Level 3 pregame-safe feature context
```

Packet 4 is not model retraining and does not change what the user saw. It
creates the traceable postgame evidence that later calibration and Admin views
will inspect.

## Why this packet matters

Without Packet 4, GameLens can preserve a statement but cannot answer:

- what happened in the game;
- whether an individual claim was supported, contradicted, mixed, or
  unavailable;
- which pregame-safe conditions surrounded that claim; or
- exactly where a failed learning attempt stopped.

This packet must make those answers repeatable and diagnosable while reusing
the existing football calculations.

## Packet 3 handoff and deferred validation

Packet 3 received **Implementation GO** on 2026-08-16. Seven genuine canonical
preseason captures all returned zero claims. The cloud zero-claim write/retry,
post-ETL snapshot immutability, and controlled populated-path tests passed.

The first genuine claim-bearing development write/retry and claim-bearing
bounded slate remain pre-production operational validations. They do not block
Packet 4 code. If real populated claims arrive during Packet 4, run the existing
Packet 3 proof before admitting those rows downstream. Do not manufacture
claims or add a second extraction path.

## What already exists and must be reused

Inspect the current `dev` versions before designing changes:

- `services/model_trust_service.py` for Model Outcome and Trust calculation;
- `agg/gamelens_training/update_claim_training_validation.py` for Level 2;
- `agg/gamelens_training/update_claim_training_features.py` for Level 3;
- `services/gamelens_level1_service.py` and
  `services/gamelens_claim_storage.py` for Packet 3 identity/storage behavior;
- `services/gamelens_learning_contract.py` for admission and leakage rules;
- `queries/game_queries.py` and existing metric facts for completed-game
  evidence; and
- `GameLens_dev.stage_runs` plus `GameLens_dev.stage_game_results` for the
  minimum receipt pattern.

The first implementation step is inspection, not rewriting. Existing CLIs
remain available and should become thin wrappers over callable shared workers
where necessary.

## DRY boundary

Packet 4 must use one calculation path for each responsibility:

| Responsibility | Single owner |
|---|---|
| Frozen prediction and claim identity | Packet 2/3 canonical rows |
| Game outcome and trust calculation | Existing Model Trust/Outcome service |
| Claim validation | Existing Level 2 worker |
| Pregame feature enrichment | Existing Level 3 worker |
| Metric definitions and directions | Existing metric registry |
| Selection, gates, ordering, and summary | Thin Packet 4 coordinator |

The coordinator may select and summarize. It must not recalculate football
metrics, reinterpret validation rules, rebuild `/game`, or create a parallel
claim extractor.

## Admission funnel

Every game moves only as far as its evidence allows:

| Available evidence | Required result |
|---|---|
| No canonical capture | `capture_missing`; no grade and no Levels 2–3 |
| Capture, game not final | `waiting_game_final` |
| Capture plus final score | Grade frozen Model Outcome/Trust |
| Grade but no accepted Facts | Preserve grade; Levels 2–3 wait |
| Capture, final score, accepted Facts, zero Level 1 claims | Grade game; Levels 2–3 visible `no_op / zero_claims` |
| Capture, final score, accepted Facts, claims | Run Level 2, then Level 3 |
| Level 2 ordinary unavailable rows | Persist them as data; Level 3 may proceed |
| Level 2 stage failure | Stop Level 3 for that game and record the boundary |

Preseason remains development/shadow evidence only. It must not enter a
production learning cohort, Admin production population, or weekly report.

## Required identities and lineage

Every grade, validation, feature row, and receipt must trace to the same chain:

```text
pipeline_run_id
  -> learning_run_id
    -> capture_id + source payload hash
      -> game_id
        -> claim_key when the row is claim-level
```

An existing postgame row that cannot trace to the canonical capture must be
reported as a conflict and quarantined. It must not silently overwrite the new
grade or be accepted merely because the game ID matches.

## Required failure traceability

A top-level `failure` or `partial_failure` is not sufficient. For every
attempted game and stage, the durable receipt and terminal/admin summary must
answer:

1. Which game failed?
2. Which stage failed?
3. Which capture and upstream data version were used?
4. What specifically failed?
5. Is retry safe, and from which boundary?

Retain or expose these fields:

| Area | Required evidence |
|---|---|
| Run | `attempt_id`, overall status, start/finish, duration |
| Stage/game | `stage_name`, `game_id`, status, failed boundary |
| Canonical identity | `learning_run_id`, `capture_id`, `claim_key` when applicable |
| Input lineage | `pipeline_run_id`, payload hash, source/target tables, versions |
| Reconciliation | input, output, inserted, unchanged, conflict, unavailable, rejected counts |
| Failure | stable reason code, readable message, retryable flag, non-secret exception class |
| Logs | Cloud Logging execution/trace reference when an exception occurs |
| Isolation | successful sibling games/stages remain committed and visible |

Truthful zeros are data. Zero claims, zero unavailable rows, or zero inserted
rows must display as zero—not as blank, unknown, or failure.

Dedicated new error columns or tables are not automatically required. First
reuse the existing stage receipt pattern. Add the smallest schema change only
if the required questions cannot be answered accurately from current fields.

## Storage posture

- All Packet 4 rehearsal writes remain in `GameLens_dev`.
- Reuse existing grade and claim-training tables when their grain and lineage
  are compatible.
- Do not create a second claim table or duplicate Level 2/3 result store merely
  for orchestration.
- Any required capture-lineage additions must be additive and idempotent.
- Writes must be game-scoped and retry-safe; one game cannot delete another
  game's results.
- Preseason rows remain isolated from future regular-season cohorts.

The code inspection must identify the exact existing outcome table and whether
it can safely carry `capture_id` and source hash before a schema decision is
made.

## Visual QA requirement

Every dry run and deliberate write should print a compact per-game funnel:

| Game | Capture | Final | Facts | Claims | Grade | L2 | L3 | Final status/reason |
|---|---|---|---|---:|---|---|---|---|

The summary must also show totals by stage and a small sample of changed rows.
For writes, display before/projected/after counts and reconcile them. This is
both Christian's visual QA surface and the future Admin contract; it is not a
second calculation system.

## Small implementation slices

### Slice 1 — inspect and freeze contracts

- inspect existing outcome, Level 2, and Level 3 code and schemas;
- identify shared callable functions and current CLI-only boundaries;
- map each existing result field to the required lineage chain;
- identify only genuine gaps; and
- update this plan before code if the current implementation differs from the
  assumptions above.

### Slice 2 — capture-aware game grade

- select one canonical capture plus final score;
- grade the frozen sections through the existing outcome/trust calculation;
- persist or reconcile one game-scoped result;
- quarantine identity conflicts; and
- prove identical retry changes zero rows.

### Slice 3 — callable Level 2 worker

- adapt the current validation CLI to accept a bounded game/capture selection;
- preserve the CLI as a wrapper over the same worker;
- use accepted Facts only;
- preserve unavailable results and reasons; and
- reconcile claims in against validations out.

### Slice 4 — callable Level 3 worker

- run only after Level 2 completes without a stage failure;
- use frozen pregame fields and registry metadata only;
- reject final score, actual outcome, validation result, and actual metric gaps
  as feature inputs;
- preserve formula/feature versions; and
- reconcile eligible validations against feature rows.

### Slice 5 — bounded coordinator and receipts

- apply the admission funnel per game;
- call the three shared workers in order;
- preserve successful games when a sibling fails;
- write attempt and per-game/per-stage receipts;
- emit the visual diagnostic funnel; and
- keep `app.py` unchanged.

### Slice 6 — development-cloud proof

- read-only inventory;
- dry-run one game for each available evidence state;
- one deliberate development write;
- identical retry;
- partial-failure isolation proof;
- read-back reconciliation; and
- documentation closure with exact IDs and counts.

## Required tests

- capture missing;
- game not final;
- final score with no Facts;
- Facts with no final score;
- valid final game with zero claims;
- valid final game with populated claims through controlled fixtures;
- unavailable Level 2 metric;
- Level 2 stage failure stops Level 3;
- Level 3 rejects all postgame target leakage;
- existing outcome identity conflict is quarantined;
- identical retry changes zero rows;
- one failed game preserves successful siblings;
- dev-only storage refusal in production runtime;
- terminal summary and durable receipts reconcile; and
- historical Level 2/3 CLIs still use the same calculations.

## Packet 4 GO evidence

Packet 4 receives Implementation GO only when:

1. this plan was reviewed before code;
2. existing calculation owners remain single-source/DRY;
3. game grading uses the frozen canonical capture;
4. Level 2 requires final score plus accepted Facts;
5. Level 3 uses pregame-safe inputs only;
6. zero claims and unavailable validations remain visible, truthful results;
7. duplicate runs change zero rows;
8. identity conflicts fail visibly without overwriting evidence;
9. a per-game failure does not discard successful siblings;
10. every failure answers the five traceability questions;
11. all rehearsal writes remain in development;
12. production ETL, `/game`, frontend, and Scheduler remain unchanged; and
13. exact evidence and any deferred real-data gates are documented.

## What Packet 4 does not do

Packet 4 does not:

- wire learning into the 8:00 a.m. production run;
- run Level 4 calibration;
- expose the learning ledger through Admin;
- change Matchup Lean, confidence, claim language, or the frontend;
- reimplement or recalculate the already released Calibrated Matchup Lean rule;
- create production learning datasets; or
- merge learning work to `main`.

Those constraints remain outside Packet 4; later capabilities require their own packet or explicit release decision.

## Review questions before implementation

- Does the admission funnel match the intended product behavior?
- Is a final game with zero claims correctly treated as a visible no-op rather
  than an error?
- Can every outcome, validation, and feature row trace to the canonical
  capture?
- Will the summary make a partial failure diagnosable without reading code?
- Are we reusing the existing workers rather than reproducing their logic?
- Are all writes still development-only?

After these answers are confirmed, begin with Slice 1 inspection only.

