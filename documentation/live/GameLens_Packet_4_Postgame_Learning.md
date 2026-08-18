# GameLens Packet 4 — Postgame Outcome plus Levels 2–3

**Status:** DAL–SEA grade insert/retry passed; bounded read-only Level 2 adapter implemented on 2026-08-18; no production behavior change  
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

## Slice 1 contract inspection — 2026-08-17

The current `dev` code was inspected against the live README, Sprint, completed
Packet 1–3 contracts, architecture handoff, and the older August readiness plan.
The architecture is sound, but the existing database wrappers are historical or
production-shaped and must not be called unchanged by Packet 4.

### Reusable calculation owners

| Responsibility | Reused callable path | Inspection decision |
|---|---|---|
| Model Outcome | `services/game_service.py::build_model_outcome(...)` | Reuse with the frozen snapshot's `matchup_lean` and `header`, plus the accepted final score |
| Model Trust | `services/model_trust_service.py::build_model_trust(...)` | Reuse with frozen `game_profile`, `team_comparison`, and `matchup_lean`; do not rebuild those sections |
| Level 2 claim semantics | `validate_training_row(...)`, `add_game_level_scores(...)`, and their comparison helpers | Preserve; wrap a bounded selected row set rather than copying the validation rules |
| Level 3 feature semantics | `build_feature_updates(...)` and the registry-backed feature helpers | Preserve; pass only frozen pregame claim fields and keep the CLI over the same callable worker |
| Capture and claim identity | Packet 2/3 snapshot, contract, Level 1, and claim-storage services | Reuse unchanged |

### Gaps confirmed by inspection

1. `save_model_results(...)` is not a Packet 4 storage boundary. It writes
   directly to `Analytics.game_model_outcomes` and
   `Analytics.game_model_trust_details`, checks only `game_id`, silently
   returns when a game row already exists, and carries no `learning_run_id`,
   `capture_id`, payload hash, or upstream input version.
2. The current Level 2 CLI selects an entire `run_id`; it does not select by
   capture/game. Its BigQuery staging table is hard-coded into `Analytics`,
   and its update path reports no inserted/unchanged/conflict reconciliation.
3. The current Level 3 calculation is already pregame-only, which is the
   important safety result. Its loader and BigQuery wrapper are still
   whole-`run_id` and production-default, so they need the same bounded,
   development-only adapter boundary as Level 2.
4. `GameLens_dev.stage_game_results` already carries the core attempt, stage,
   game, capture, learning cohort, counts, reason, and upstream reference. It
   does not currently carry enough structured detail for failed boundary,
   payload hash, source/target tables, inserted/unchanged/conflict/unavailable/
   rejected counts, retryability, exception class, or a log reference.
5. The existing metric conductor returns a truthful in-memory summary, including
   Facts completion and row count, but it does not persist a durable execution
   identifier. Packet 4 therefore needs an explicit, inspectable postgame Facts
   input version in its receipt; it must not pretend the pregame capture's
   pipeline lineage is the postgame Facts execution.

### Frozen implementation boundary

- Add a thin capture-aware grading service that loads one canonical snapshot,
  revalidates its identity and hash, and calls the two existing outcome/trust
  calculation owners with the frozen sections.
- Do not call `get_game_details(...)` for grading and do not call
  `save_model_results(...)`; both would re-read or write through the live
  product path.
- Use a development counterpart of the existing logical outcome store only
  after a read-only schema inventory. Its logical key must include the stable
  learning cohort and canonical capture, and conflicting immutable lineage must
  quarantine rather than overwrite.
- Refactor Level 2 into a callable bounded worker over selected
  `learning_run_id + capture_id + game_id` claim rows; keep the historical CLI
  as a wrapper over the same calculations.
- Keep `build_feature_updates(...)` as the Level 3 calculation owner, but give
  its loader/writer the same bounded selection and development-only refusal.
  Pass a pregame-field allowlist so validation and outcome targets cannot enter
  the feature worker accidentally.
- Extend the existing receipt pattern additively instead of creating a second
  orchestration ledger. Exact columns remain gated by the read-only cloud
  schema inventory.
- Preserve the existing zero-claim behavior: grade the game, then record Level 2
  and Level 3 as visible `no_op / zero_claims`.

### Read-only inventory owed before the first schema write

Inspect the actual schemas and selected-game rows for:

- `Analytics.game_model_outcomes`;
- `Analytics.game_model_trust_details`;
- `GameLens_dev.claim_training_examples`;
- `GameLens_dev.stage_runs`; and
- `GameLens_dev.stage_game_results`.

This inventory is read-only. It must confirm whether the repository's inferred
outcome shape matches the live tables and identify the smallest additive
development migration. No production table is altered in Packet 4.

### Slice 2 implementation checkpoint — 2026-08-17

Commit `7aa623e` adds
`services/gamelens_postgame_grading.py` and focused tests. The service:

- accepts one canonical snapshot plus a final score;
- refuses non-development captures during Packet 4;
- revalidates capture status, source payload hash, the pregame leakage guard,
  game identity, and frozen section shapes;
- lazily calls the existing `build_model_outcome(...)` and
  `build_model_trust(...)` owners;
- returns `learning_run_id`, `capture_id`, `game_id`, the source payload
  hash, a deterministic final-score hash, grade version, Model Outcome, and
  Model Trust; and
- performs no BigQuery read, write, route call, product-payload rebuild, or
  production-table mutation.

Focused local evidence:

```text
7 tests passed through unittest
Python compilation passed
```

The tests cover frozen-section reuse, canonical lineage and hashes,
development-only refusal, source-hash mismatch, game-identity mismatch,
postgame leakage, malformed frozen section shape, and incomplete final score.

### Read-only handoff inventory runner — 2026-08-17

Commit `6354f72` added
`qa_gamelens_packet4_schema_inventory.py` and five focused tests. Commit `474ff41`
then corrected the launch boundary after the first local invocation safely
stopped before cloud access: the runner now requires explicit
`--dev-read-only` confirmation without loading writable deployment
configuration. The command
inspects metadata and optional selected-game row counts for the complete
Packet 4 handoff:

- `GameLens_dev.pregame_snapshots`;
- `Scores.scores`;
- `Analytics.game_team_metric_facts_2026`;
- existing `Analytics.game_model_outcomes` and
  `Analytics.game_model_trust_details`;
- `GameLens_dev.claim_training_examples`;
- `GameLens_dev.stage_runs` and `stage_game_results`; and
- the proposed `GameLens_dev.game_model_outcomes` target.

The proposed target is optional during inventory because it is expected not to
exist yet. Every required source must be available. The tool reads table
metadata and parameterized per-game counts only, rejects mutation keywords
locally, and performs no BigQuery write.

Run from the existing Packet 2/3 development environment:

```bash
python qa_gamelens_packet4_schema_inventory.py \
  --dev-read-only \
  --game-id 20260815_DAL@SEA
```

Combined local evidence for the grader and inventory runner:

```text
27 tests passed through unittest
Python compilation passed
```

### Inventory evidence and storage decision — 2026-08-17

The read-only inventory completed at
`2026-08-17T20:31:14.740384+00:00` with all eight required tables available.
For `20260815_DAL@SEA`, it found one canonical Packet 2 capture, 130 accepted
Facts rows, zero Packet 3 claim rows, and one game-stage receipt. The optional
`GameLens_dev.game_model_outcomes` target did not exist, as expected.

The inventory proves that the production `Analytics.game_model_outcomes` and
`Analytics.game_model_trust_details` tables are not valid Packet 4 targets:
they are keyed by game context and lack `learning_run_id`, `capture_id`,
source-payload hash, final-score hash, and grade version. Altering them would
mix the shadow learning ledger with the live product result path.

The first inventory also exposed the legacy `Scores.scores.gameID` spelling.
Its schema was captured, but the selected-game count was skipped. Commit
`5373cd0` added read-only `gameID` alias support. The corrected inventory
completed at `2026-08-17T22:59:45.563321+00:00` and found exactly two DAL–SEA
score rows, the expected one-row-per-team grain. A normalized comparison
confirmed this was the only evidence difference between inventory versions.

Commit `5373cd0` also adds the smallest approved development boundary:

- one `GameLens_dev.game_model_outcomes` row per
  `learning_run_id + capture_id`;
- canonical `game_id`, pregame pipeline lineage, season context,
  source-payload hash, final-score hash, and grade version;
- the reused Model Outcome and Model Trust results stored together as JSON;
- insert-only MERGE behavior;
- identical retry reported as unchanged;
- immutable lineage or result disagreement quarantined before write; and
- setup code that refuses non-development runtime.

Level 2 and Level 3 remain in the existing
`GameLens_dev.claim_training_examples` rows. No second claim table or trust
detail table is introduced.

The setup receipt then verified
`nfl-stream-406420.GameLens_dev.game_model_outcomes` with 13 fields,
`graded_at` daily partitioning, clustering by
`learning_run_id + game_id + grade_version`, and the
`learning_run_id + capture_id` merge key. This created/verified the empty
development ledger only; no grade row was inserted.

Commit `3980310` adds `qa_gamelens_packet4_dry_grade.py`. It reads exactly one
canonical snapshot and two score rows, rebuilds the established final-score
shape, calls the frozen-capture grader and existing outcome/trust builders, and
asks the storage boundary for a projection only. The command requires explicit
`--dev-read-only` confirmation and has no grade-write path.

The real DAL–SEA dry execution passed:

- final score: DAL 17, SEA 7;
- canonical capture:
  `capture_b387ab5d3545e2c322827756`;
- source payload hash:
  `d9ddda20f7fe42291acd4268dd871368def66b604c28b151aad86e7bb71922b1`;
- final-score hash:
  `1f51b9658143b067c7f2db42a9e26f7dd5dbd4f7132bcd9ba291d3190aa8a636`;
- Model Outcome: `No Pick`, because the frozen preseason payload did not name
  a predicted team;
- Model Trust: neutral, with no visible matchup advantage or signal set;
- storage projection: zero existing, one projected, zero conflicts; and
- `write_performed = false`.

This is truthful preseason evidence: DAL winning does not retroactively create
a prediction that the frozen payload never made.

Commit `49b7ff6` adds a separate deliberate write runner. It requires a complete
development runtime, explicit `--confirm-dev-write`, and an attempt ID. It
uses the same read/grade path, calls the insert-only storage boundary, reads the
row back, and reports inserted/unchanged/conflict counts. The dry runner remains
permanently write-free.

### Game-grade write/retry evidence — 2026-08-17

The deliberate DAL–SEA development write and identical retry both passed.
Attempt `packet4_grade_dal_sea_first_20260817` found zero existing rows and
inserted exactly one. Attempt `packet4_grade_dal_sea_retry_20260817` found that
one row unchanged, inserted zero, reported zero conflicts, and performed no
write. The ledger remained at one row.

Both attempts preserved the same canonical evidence:

- `learning_run_id = gamelens_2026_preseason_v1`;
- `capture_id = capture_b387ab5d3545e2c322827756`;
- `pipeline_run_id = observed_prod_2026_asof_20260814_f1272_w1460_r2073`;
- source payload SHA-256
  `d9ddda20f7fe42291acd4268dd871368def66b604c28b151aad86e7bb71922b1`;
- final-score SHA-256
  `1f51b9658143b067c7f2db42a9e26f7dd5dbd4f7132bcd9ba291d3190aa8a636`;
- DAL as the actual winner; and
- the frozen `No Pick` outcome and neutral Model Trust result.

A normalized field-level comparison removed only attempt and reconciliation
metadata; every remaining grade/evidence field was identical. This closes the
game-grade persistence slice and authorizes the Level 2 adapter slice.

### Slice 3 bounded Level 2 adapter — 2026-08-18

Commit `1814052` adds `services/gamelens_level2_validation.py` and
`qa_gamelens_packet4_level2.py`. The adapter owns only the missing boundary:

- selects one `learning_run_id + capture_id + game_id` claim set;
- refuses cross-cohort, cross-capture, cross-game, missing-key, and duplicate
  claim inputs;
- requires the final score and accepted postgame Facts before validation;
- returns visible `deferred / waiting_accepted_facts` and
  `no_op / zero_claims` states;
- lazily delegates comparison and scoring to the existing Level 2
  `build_actual_index(...)`, `validate_training_row(...)`, and
  `add_game_level_scores(...)` functions;
- preserves ordinary unavailable validations as data;
- reconciles claims in, validations out, unavailable, rejected, and conflicts;
  and
- is permanently read-only in this QA slice.

The controlled populated fixture also ran through the existing Level 2 module:
one claimed DAL `total_yards` edge produced one validated row, a 100-yard
actual gap, `dominant_edge`, and the existing
`good_reasoning_correct_outcome` QA label. Thirty-six focused Packet 4 tests
pass, including the prior grader, storage, inventory, dry-grade, and write-runner
coverage. Python compilation passes.

### Next implementation slice

Confirm the repository build for `1814052`, then run the bounded DAL–SEA Level
2 preview:

```bash
python qa_gamelens_packet4_level2.py \
  --dev-read-only \
  --game-id 20260815_DAL@SEA \
  > packet4_level2_preview.json
```

The known real input has 130 accepted Facts rows and zero Packet 3 claims, so
the honest expected result is `status = no_op`, `reason = zero_claims`, zero
validations, and `write_performed = false`. Review that receipt before adding
the development-only Level 2 update/reconciliation boundary. Do not invent
claims to force a populated cloud result; the controlled fixture covers that
calculation path until a genuine claim-bearing capture exists.

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

