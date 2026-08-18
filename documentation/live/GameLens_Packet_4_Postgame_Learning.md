# GameLens Packet 4 — Postgame Outcome plus Levels 2–3

**Status:** DAL–SEA grade, Levels 2–3, one-game coordinator receipt/retry, and three-game Slice 6 inventory proofs passed; bounded multi-game and partial-failure proof remains; no production behavior change  
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
`good_reasoning_correct_outcome` QA label. Thirty-eight focused Packet 4 tests
pass, including the prior grader, storage, inventory, dry-grade, and write-runner
coverage. Python compilation passes.

The first real DAL–SEA Level 2 preview correctly returned
`no_op / zero_claims`, zero validations, zero conflicts, and no write. It
reported 94 rows under the original `accepted_fact_rows` label. Reconciliation
against the earlier inventory showed that the source table still contains all
130 accepted Facts rows: 94 are eligible for the existing Level 2 calculation
after its `value IS NOT NULL` and directional-comparison filters. No source data
was missing and no validation was skipped because the capture contains zero
claims.

Commit `6eb4a9d` removes that reporting ambiguity. The read-only preview now
reports both `accepted_fact_rows_total = 130` and
`eligible_actual_fact_rows = 94`, and refuses internally inconsistent Facts
gate/count combinations.

The corrected real preview then passed with exactly those counts, the canonical
capture and learning cohort, DAL 17–7, zero claims, zero validations, zero
conflicts/rejections/unavailable rows, `no_op / zero_claims`, and
`write_performed = false`. This closes the read-only Level 2 gate.

### Slice 3 Level 2 development update boundary — 2026-08-18

Commit `14a86bc` adds `services/gamelens_level2_storage.py`,
`run_gamelens_packet4_level2_write.py`, and focused tests. It updates only
existing `GameLens_dev.claim_training_examples` rows and introduces no table.
The boundary:

- requires a development runtime plus explicit `--confirm-dev-write`;
- selects and updates by cohort, claim, capture, and game identity;
- cannot insert a missing claim row;
- updates only rows whose `validation_result` is still null;
- treats an identical replay as unchanged;
- quarantines conflicting prior Level 2 evidence and incomplete claim/validation
  reconciliation before mutation;
- preserves Level 2 retry idempotency after Level 3 later changes the shared
  feature-stage metadata; and
- reads the bounded rows back after an update and requires full reconciliation.

Forty-nine focused Packet 4 tests pass. Controlled rows prove one initial
update, an identical zero-update retry, conflict refusal, cross-boundary SQL
scope, and compatibility with later Level 3 stage metadata. The real DAL–SEA
capture remains an honest zero-claim case, so its cloud attempts must execute
the boundary and still perform no write.

Both real DAL–SEA Level 2 boundary attempts then passed. The first attempt and
retry preserved the same canonical capture, DAL 17–7 score, 130 total accepted
Facts, and 94 Level 2-eligible actual rows. Each reported zero claims,
validations, selected rows, updates, conflicts, and rejections with
`no_op / zero_claims` and `write_performed = false`. This closes Level 2 for
the available real zero-claim evidence. The first genuine populated update and
retry remain on the pre-production validation list.

### Slice 4 bounded Level 3 preview — 2026-08-18

Commit `861bd57` adds `services/gamelens_level3_features.py`,
`qa_gamelens_packet4_level3.py`, and focused tests. It retains the existing
`build_feature_updates(...)` calculation owner while replacing the historical
whole-run boundary with one `learning_run_id + capture_id + game_id` adapter.

The adapter:

- runs only after Level 2 returns `completed` or the truthful zero-claim
  `no_op`;
- blocks Level 3 after a real Level 2 failure or incomplete stage;
- permits ordinary Level 2 `unavailable` validation rows to continue;
- projects an explicit calculation allowlist before calling the existing
  worker;
- strips final score, winner, Model Outcome/Trust, validation labels, actual
  sides/teams/gaps, QA outcome reads, and all other postgame targets;
- reconciles claim keys and counts against feature outputs; and
- remains permanently write-free in this preview slice.

The controlled populated fixture also ran through the real Level 3 module. One
claim produced one registry-backed feature row under the existing formula
version, while `postgame_fields_admitted = 0`. Fifty-seven focused Packet 4
tests pass, including Level 2-failure blocking, unavailable-row continuation,
target stripping, identity refusal, and the complete earlier Packet 4 suite.

The real DAL–SEA preview then passed. It preserved
`learning_run_id = gamelens_2026_preseason_v1`,
`capture_id = capture_b387ab5d3545e2c322827756`, DAL's 17–7 final score, 130
total accepted Facts, and 94 Level 2-eligible actual rows. The Level 2 gate
returned `no_op / zero_claims`; Level 3 likewise returned
`no_op / zero_claims` with zero claims, feature rows, rejections, and postgame
fields admitted. It performed no write.

### Slice 4 Level 3 development update boundary — 2026-08-18

Commit `5cd25d7` adds `services/gamelens_level3_storage.py`,
`run_gamelens_packet4_level3_write.py`, and focused tests. It reuses the
existing Level 3 calculation and adds only the missing capture-aware storage
boundary. The boundary:

- requires a development runtime plus explicit `--confirm-dev-write`;
- reads and updates one `learning_run_id + capture_id + game_id` claim set;
- updates only an existing claim whose `feature_formula_version` is null;
- cannot insert a missing claim or alter the target table schema;
- treats an identical replay as unchanged;
- quarantines conflicting prior feature evidence and incomplete
  claim/feature reconciliation before mutation;
- uses only the pregame-safe Level 3 output fields; and
- reads the bounded rows back and requires full reconciliation.

Sixty-eight focused Packet 4 tests pass. Controlled rows prove the populated
first update, identical zero-update retry, conflict refusal, missing-feature
refusal, development-only gate, and update-only capture/game-scoped SQL. The
real DAL–SEA capture remains an honest zero-claim case, so both cloud attempts
must execute the boundary and still perform no write.

The first real boundary commands stopped before staging or mutation. The
constructor found that the 117-field Packet 3 claim schema already contained
the original Level 3 score and formula fields but lacked 15 newer
registry/hierarchy/support metadata destinations emitted by the existing Level
3 worker. Both attempts raised the same explicit missing-field error; neither
could write BigQuery. Their redirected JSON files may be empty and can be
overwritten by the corrected rerun.

Commit `11378ba` adds `setup_gamelens_level3_columns.py` and a dev-only,
idempotent schema setup function. It does not create a second claim table. It
adds only missing nullable Level 3 output fields to the existing
`GameLens_dev.claim_training_examples` table, refuses incompatible existing
types, verifies the resulting schema, refuses production before client access,
and reports added fields plus before/after counts. Seventy-two focused Packet 4
tests pass.

The real schema and Level 3 receipts then passed together:

- schema setup added exactly the reviewed 15 nullable fields, moving the
  existing claim table from 117 to 132 fields;
- both Level 3 attempts preserved
  `learning_run_id = gamelens_2026_preseason_v1` and
  `capture_id = capture_b387ab5d3545e2c322827756`;
- both preserved DAL 17–7, 130 total accepted Facts, and 94 eligible actual
  rows;
- both passed the Level 2 `no_op / zero_claims` gate;
- both reported zero claims, features, selected rows, updates, conflicts,
  rejections, unavailable rows, and postgame fields admitted; and
- both performed no learning write.

This closes the available real-data Level 3 proof. A genuine populated feature
update/retry remains on the pre-production validation list.

### Slice 5 bounded coordinator and durable receipts — 2026-08-18

Commit `9b21f88` adds `services/gamelens_packet4_coordinator.py`,
`services/gamelens_packet4_receipts.py`, the setup/run CLIs, and focused tests.
The coordinator is deliberately thin: it calls the already-proven grade,
Level 2, and Level 3 boundaries in order and performs no football calculation.

It:

- accepts an explicit bounded game list and development attempt ID;
- preserves the canonical cohort/capture identity between stages;
- stops downstream work after the failed boundary;
- preserves a successful sibling game when another game fails;
- distinguishes learning-data writes from receipt writes;
- emits the required per-game visual funnel;
- records stable reasons, retryability, exception class/message, lineage,
  counts, timestamps, failed boundary, and optional log reference; and
- writes one immutable attempt receipt plus one receipt per game/stage.

The dedicated `GameLens_dev.postgame_learning_stage_receipts` table is audit
storage, not a second claim or feature table. It is necessary because the
existing Packet 2 receipt schemas cannot carry Packet 4's full failure and
reconciliation contract without breaking their exact schema verification.
Receipt MERGE is insert-only by deterministic logical key, so an exact retry
of the same attempt preserves the first receipts and inserts zero. Eighty-four
focused Packet 4 tests pass, including controlled sibling isolation and
receipt-storage failure behavior.

### One-game coordinator cloud proof — 2026-08-18

The repository build, receipt table, first coordinator run, and identical
retry all passed:

- `GameLens_dev.postgame_learning_stage_receipts` verified with 31 fields,
  `recorded_at` partitioning, and the intended attempt/game/stage logical key;
- attempt `packet4_coordinator_dal_sea_20260818` completed one requested game
  with zero failed games;
- DAL–SEA preserved the canonical `learning_run_id`, capture ID, pipeline run,
  and source-payload hash across grade, Level 2, and Level 3;
- grade returned `success`; Levels 2–3 returned `no_op / zero_claims`;
- the first run inserted four receipts and the exact-attempt retry matched four
  unchanged receipts with zero inserts;
- both runs performed zero learning writes; and
- the terminal funnel, stage order, and durable receipt counts reconciled.

This closes the one-game Slice 5 handoff. The receipt table remains audit
storage only; the grade ledger and claim table remain the evidence owners.

### Slice 6 three-game inventory — 2026-08-18

The read-only inventory passed with all nine source/target tables available
and zero required tables unavailable. Its three games establish the intended
admission states:

- DET–CIN has one canonical capture, two final-score rows, 126 Facts rows,
  zero claim rows, and no Packet 4 development grade;
- DAL–SEA has one canonical capture, two final-score rows, 130 Facts rows,
  zero claim rows, and one fully populated Packet 4 development grade;
- CAR–ARI has two final-score rows and 126 Facts rows but no canonical capture;
  its one older production outcome row is not frozen Packet 4 evidence and
  cannot substitute for the missing capture; and
- the Packet 3 claim table remains honestly empty.

This proves that capture—not score, Facts, or an older outcome—is the first
Packet 4 admission gate.

### Build-artifact hygiene

Packet 4 CLIs print JSON to standard output; they do not create files unless
an operator redirects output with `>`. Cloud coordination will emit structured
logs and durable BigQuery receipts, not repository JSON files. Repository
`.gitignore` already excludes `*.json`. Packet 4 additionally hardens
`.dockerignore` and `.gcloudignore` so local receipt/snapshot JSON cannot enter
a Docker image or locally submitted Cloud Build context. The local
`packet4_*.json` proof files remain temporary operator evidence and should be
deleted after Packet 4 documentation records the final attempt IDs and counts.

### Next implementation slice

The healthy DET–CIN plus DAL–SEA first run passed: two games completed, six
stages ran in order, DET–CIN inserted its one missing grade, DAL–SEA matched its
existing grade, both Levels 2–3 returned `no_op / zero_claims`, and seven
receipts were inserted without conflict. The exact retry changed zero learning
rows, and both games again completed successfully, but receipt reconciliation
failed closed with `Packet4ReceiptConflictError`.

The failure exposed a receipt-material bug rather than a football or learning
error. The first execution truthfully recorded DET–CIN as one grade insert;
the retry truthfully recorded it as one unchanged grade. The receipt comparer
incorrectly treated those expected execution-effect changes as immutable
evidence. No receipt was overwritten and no duplicate grade was created.

Commit `8de1cf2` fixes that boundary narrowly: `inserted_count`,
`updated_count`, `unchanged_count`, and `write_performed` may differ between a
first execution and its exact retry, while status, reason, lineage, input and
output counts, conflicts, unavailable/rejected counts, failed boundary,
retryability, exception details, and log reference remain material and
conflict-protected. Two regression tests cover the allowed effect transition
and a still-rejected stable-count change. Replaying the observed two JSON
summaries through the corrected comparer produces seven unchanged receipts,
zero inserts, and zero conflicts.

Pull `dev` and rerun only the exact same two-game attempt ID. Require two
completed games, zero failed games, zero learning writes, seven unchanged
receipts, zero receipt inserts, and no receipt write. After that proof is
reviewed, use DAL–SEA plus CAR–ARI for the natural partial-failure write/retry.
CAR–ARI must fail at the capture/grade boundary and skip both downstream stages
while DAL–SEA remains successful.

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
