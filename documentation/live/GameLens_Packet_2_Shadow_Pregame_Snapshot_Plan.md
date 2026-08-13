# GameLens Packet 2 — Shadow Pregame Snapshot Plan

**Status:** Complete — **Packet 2 GO**; snapshot correctness, per-game observability, historical audit backfill, idempotency, and Schedule coverage proof all passed
**Created:** 2026-08-10  
**Revised:** 2026-08-13  
**Branch:** `dev`  
**Production behavior changed:** No  
**Production data written:** No  
**Production data read:** Yes — explicitly read-only Schedule and Analytics evidence  
**Development data written:** Yes — six canonical snapshots, four attempt receipts, and nine provenance-marked per-game audit rows

## Packet 2 observability amendment — 2026-08-13

The six canonical snapshots and their exact live-`/game` parity remain valid.
This amendment does not rebuild or replace any snapshot and does not reverse
the capture-correctness result. It closes a separate operational blind spot
before Packet 3 begins.

`GameLens_dev.stage_runs` deliberately has one row per coordinator invocation.
Its `game_id` describes the **requested scope**, so a slate invocation correctly
has `game_id=NULL`. That attempt-level row can say “six checked, five captured,
one skipped,” but it cannot identify which game failed, waited, skipped, or
returned a no-op. A partial failure would therefore be difficult to diagnose
from durable BigQuery evidence alone.

Packet 2 will add `GameLens_dev.stage_game_results` at this grain:

```text
one row per attempt_id + stage_name + game_id
```

The required fields are `attempt_id`, `stage_name`, `game_id`,
`capture_id`, `learning_run_id`, `season`, `season_type`, `status`,
`reason`, `eligible`, `rebuilt`, per-game input/output counts,
`upstream_run_id`, `recorded_at`, `is_backfill`, and
`backfill_source`. The attempt summary remains in `stage_runs`; the new
table supplies the missing per-game detail. It is an audit ledger, not a
second snapshot store.

### The four known development attempts

“Four attempts” means the four existing rows already visible in
`GameLens_dev.stage_runs`, not four missing games and not four new runs:

| Attempt | Scope and verified result | Per-game ledger rows |
|---|---|---:|
| `snapshot_20260812T184849Z_1830177d` | Explicit DET–CIN proof; failed at the JSON streaming boundary; no snapshot saved | 1 failure |
| `snapshot_20260812T191038Z_4f4725f5` | Corrected DET–CIN proof; canonical snapshot saved | 1 success |
| `snapshot_20260812T213819Z_83ef2f7e` | Identical DET–CIN retry; canonical snapshot already existed | 1 no-op |
| `snapshot_20260813T140739Z_e7a26844` | Six-game slate; DET–CIN no-op plus five successful new captures | 6 results |

The controlled historical backfill therefore creates **nine per-game audit
rows** from evidence already verified in this document. It does not rerun any
capture, mutate the four attempt summaries, or create post-kickoff snapshots.
The backfill must be explicit, development-only, provenance-marked, and
idempotent.

### Packet 3 boundary and missed-capture rule

Packet 3's canonical input is `pregame_snapshots`, joined by
`capture_id`/`game_id`; it must not treat either receipt table as prediction
evidence. The receipt tables answer what the coordinator attempted. The
snapshot table answers what was actually frozen before kickoff.

A scheduled game without a canonical snapshot before kickoff is not eligible
for Level 1 extraction or later learning. Its pregame state cannot be
reconstructed honestly after the game starts. The coverage audit will compare
Schedule with `pregame_snapshots` and the latest per-game result, reporting:

- `captured` when a canonical snapshot exists;
- `upcoming` when kickoff is still in the future and capture remains possible;
- `capture_missing` when kickoff passed without a canonical snapshot, with a
  separate latest-attempt status and reason.

Games from 2026-08-06 predate the Packet 2 capture proof. If Schedule confirms
such games and no canonical snapshot exists, they are recorded as
`capture_missing` with a pre-Packet-2 reason. They are **not** retroactively
snapshotted.

### Amendment exit criteria

Packet 2's observability amendment closes only when:

1. the new table is created idempotently in `GameLens_dev`;
2. every checked future game automatically writes one durable result for
   success, no-op, failure, waiting, or skipped;
3. retrying a detail write does not create a duplicate logical key;
4. the nine verified historical detail rows are backfilled without running
   Snapshot Capture again;
5. a read-only Schedule coverage audit identifies captured, upcoming, and
   missed games, including the pre-Packet-2 August 6 gap;
6. all focused and Packet 2 regression tests pass; and
7. the six existing snapshots, hashes, and payloads remain unchanged.

The amendment is now proven in the development dataset. Packet 3 may proceed
against the established rule that only canonical `pregame_snapshots` feed
Level 1; neither receipt table is prediction evidence.

## Observability implementation checkpoint — 2026-08-13

The amendment is implemented on `dev` in deliberately separate commits:

| Commit | Responsibility |
|---|---|
| `8f6a56a` | Reopen and define the Packet 2 observability gate |
| `2d4a5bc` | Add the `stage_game_results` schema, setup, and idempotent storage contract |
| `d1fcab2` | Persist every checked game's final coordinator outcome |
| `c79e562` | Add the verified nine-row backfill and read-only coverage audit |
| `15cee38` | Extend the populated snapshot handoff fixture to prove the new receipt contract |

The relevant Packet 1/2 regression set passes **66 tests** across the learning
contract, snapshot queries, canonical game-service capture, storage/setup,
coordinator, populated handoff, historical backfill, and coverage
classification. The repository currently has no GitHub Actions workflow, so
there is no separate hosted CI result.

The development-cloud migration and proof completed in five separate
stop-and-review gates without invoking Snapshot Capture:

| Gate | Observed evidence | Result |
|---|---|---|
| Table setup | `GameLens_dev.pregame_snapshots`, `stage_runs`, and `stage_game_results` all returned `verified` in `US` | Pass |
| Backfill dry run | Four existing attempt receipts verified; nine unique logical keys; six canonical capture references verified; status counts: six success, two no-op, one failure; `write_requested=false` | Pass |
| Controlled backfill | Nine per-game rows inserted and zero previously existing; every row marked with `backfill_source=packet_2_verified_evidence_2026-08-13` | Pass |
| Identical backfill retry | Zero rows inserted and all nine logical rows found existing: `1 + 1 + 1 + 6` across the four attempts | Pass |
| Read-only coverage audit | Seven scheduled games checked for August 6–13; six captured and one capture missing; source Schedule was `League.schedule`, while snapshot and result evidence came from `GameLens_dev` | Pass |

The coverage audit completed at `2026-08-13T17:06:50.790502Z`. All six
August 13 games were backed by canonical snapshots and per-game results. The
only gap was final game `20260806_CAR@ARI`: no capture, no attempt, and the
explicit reason `before_packet_2_capture_program`. It remains visible as
`capture_missing`; no post-kickoff snapshot was invented.

The runtime remained `GAMELENS_ENVIRONMENT=dev`. Production Schedule access
was read-only, writes remained locked to `GameLens_dev`, the four
`stage_runs` summaries remained intact, and the six canonical snapshots were
not rebuilt or modified. All seven amendment exit criteria are satisfied.

### Approved one-game development evidence boundary — 2026-08-12

The first real one-game proof may explicitly read the existing production
`League.schedule`, `Analytics.team_metrics_windowed_2026`, and
`Analytics.team_metric_rankings_2026` evidence while writing only to
`GameLens_dev`. This is a read-production/write-development shadow boundary,
not a production capture release.

The production evidence option is scoped to the read-only Snapshot evidence
loader and is accepted only when the capture runtime remains `dev`.
`BigQuerySnapshotStorage` remains independently locked to `GameLens_dev`.
The saved `evidence_context.source_lineage` records the schedule dataset,
Analytics dataset, source environment, and read-only access mode for QA.

The one-game proof also requires an explicit `game_id`. Candidate discovery may
still observe the full today-plus-two-day window, but only the requested game
may proceed to canonical lookup, evidence loading, response building, or save.
A requested game that is absent from the window stops as a safe no-op. The
slate-shaped proof remains a later, separate invocation without that filter.

The first cloud capture attempt safely stopped before creating a canonical
snapshot because the BigQuery streaming insert API rejected Python mappings
for the native `JSON` columns (`response_payload is not a record`). The failure
receipt was saved, the one-game restriction held, and no internal `/game` HTTP
call occurred. The storage adapter now serializes both native JSON documents at
the streaming boundary and continues to normalize them back to Python/JSON
objects on read-back. A focused storage test covers the real insert shape so
the fake client cannot hide this API boundary again. The corrected cloud
rerun, saved-row read-back, live-API parity comparison, and identical retry all
passed as recorded below.

## Development-cloud evidence checkpoint — 2026-08-12

At this 2026-08-12 checkpoint, the one-game proof was **GO** while Packet 2
remained open for the multi-game Thursday slate rehearsal. That remaining gate
passed on 2026-08-13 as recorded in the final evidence checkpoint below. No
production route, table, scheduler, frontend, Level, or outcome behavior
changed.

### Completed one-game evidence

| Gate | Observed evidence | Result |
|---|---|---|
| Development setup | `GameLens_dev.pregame_snapshots` and `GameLens_dev.stage_runs` were created in `US`; running the setup twice returned `verified` without replacement | Pass |
| Read-only source boundary | Schedule reads used `League`; metric/ranking reads used `Analytics`; access mode was recorded as `read_only`; writes remained locked to `GameLens_dev` | Pass |
| Upstream evidence inventory | Facts: 110 rows; Windowed Metrics: 118 rows; Rankings: 106 rows; two teams represented; latest source/ranking date: 2026-08-06 | Pass |
| Exact one-game restriction | Six games were discovered for 2026-08-13; only `20260813_DET@CIN` was checked and permitted to proceed | Pass |
| Corrected canonical capture | Attempt `snapshot_20260812T191038Z_4f4725f5` captured exactly one row with capture ID `capture_080e1b4f3af8317bfb216ce0` | Pass |
| Canonical identity | `learning_run_id=gamelens_2026_preseason_v1`, `season_type=Preseason`, scheduled kickoff `2026-08-13T23:00:00Z`, canonical row count `1` | Pass |
| Honest early-season state | DET and CIN had no prior rankings in the observed 2026 evidence; the saved response recorded `no_ranking_rows_found`, empty `lens_tags`, and unavailable comparison sections without inventing evidence | Pass |
| Saved payload integrity | Stored and recalculated SHA-256 both equal `50d5e218401037e71de6fbec354d816c26d79cd0348bce8dc22a1f2c93de4acb` | Pass |
| Live `/game` parity | Authenticated live endpoint returned HTTP 200; live and saved hashes matched; exact semantic difference count was `0` across every product field | Pass |
| Identical retry | Attempt `snapshot_20260812T213819Z_83ef2f7e` returned `no_op` / `canonical_capture_exists`, performed no rebuild, made zero internal HTTP calls, and preserved canonical row count `1 → 1` | Pass |
| Retry receipt | `game_id=20260813_DET@CIN`, `season_type=Preseason`, input `1`, output `0`, and upstream reference `observed_prod_2026_asof_20260806_f110_w118_r106` | Pass |
| Runtime evidence | Successful capture: 15,457 ms; identical retry: 2,139 ms; `peak_memory_mb` was unavailable in the local Windows runtime and remained null rather than being guessed | Pass with stated limitation |

The first failed attempt remains useful audit evidence: attempt
`snapshot_20260812T184849Z_1830177d` wrote one failure receipt and zero
snapshot rows. The JSON streaming-boundary correction was committed as
`7e3a944` (`Fix snapshot JSON streaming insert`).

The successful snapshot's API response and canonical row already contained the
correct `game_id` and `season_type`. The later QA issue was isolated to the
separate batch-level `stage_runs` receipt, where the coordinator explicitly
sent both fields as null. Commits `6e024b8` (`Populate snapshot receipt
context`) and `d7509de` (`Preserve retry receipt lineage`) corrected the
receipt rules and added direct retry coverage. The original receipt remains
unchanged as historical evidence; the new no-op receipt proves the corrected
behavior.

Current development state after the retry:

- one canonical DET–CIN snapshot row;
- one failed-attempt receipt, one successful-capture receipt, and one corrected
  no-op retry receipt;
- exact saved/live response parity established for the selected game; and
- production sources read only, with no production writes.

## Final development-cloud evidence checkpoint — 2026-08-13

Packet 2's snapshot-correctness proof receives **GO**. The read-only preflight, full six-game shadow
rehearsal, canonical snapshot audit, slate receipt inspection, and
authenticated live-`/game` comparison all passed before kickoff.

### Completed slate evidence

| Gate | Observed evidence | Result |
|---|---|---|
| Read-only preflight | Six scheduled Preseason Week 1 games discovered; DET–CIN already canonical; five eligible uncaptured games; production `League` and `Analytics` explicitly read-only; write target locked to `GameLens_dev` | Pass |
| Upstream evidence | Facts 110, Windowed Metrics 118, Rankings 106; latest metric and ranking dates both 2026-08-06 | Pass |
| Slate attempt | `snapshot_20260813T140739Z_e7a26844` | Pass |
| Coordinator result | `success`; six discovered/checked/eligible, five captured, one skipped, zero failed, zero waiting | Pass |
| Existing-capture protection | DET–CIN returned `no_op / canonical_capture_exists` with `rebuilt=false` | Pass |
| Sequential save/read-back integrity | Every new result reported `saved_response_parity=matched`; final rows were six games, six capture IDs, and six canonical rows | Pass |
| Internal HTTP boundary | `internal_http_game_calls=0` | Pass |
| Slate receipt | `game_id=NULL`, `season_type=Preseason`, input/output `6 / 5`, correct upstream reference, no failure reason | Pass |
| Runtime | 28,017 ms for the six-game slate; local Windows `peak_memory_mb` remained unavailable/null and was not guessed | Pass with stated measurement limitation |
| Evidence variety | Five games honestly recorded no prior ranking rows; ARI–LV preserved available 2026-08-06 evidence and its complete de-duplicated `lens_tags` array | Pass |
| Full-slate live parity | Six authenticated HTTP 200 responses; six saved/live hashes matched; zero mismatches and zero field-level differences | Pass |
| Production safety | Production Schedule/Analytics were read only; all writes remained in `GameLens_dev`; no production product behavior changed | Pass |

### Canonical snapshot and live-parity evidence

| Game | Capture ID | Saved/live SHA-256 | Differences |
|---|---|---|---:|
| DET–CIN | `capture_080e1b4f3af8317bfb216ce0` | `50d5e218401037e71de6fbec354d816c26d79cd0348bce8dc22a1f2c93de4acb` | 0 |
| GB–PIT | `capture_98bca4de1e28583c67c25204` | `f98d24d95057648e354a140b8b315599fd00278d0695b9021365dbd471f5bf9f` | 0 |
| IND–NE | `capture_c1effcb077007166b4ccd2bd` | `53dc9415d7c8661302b2b14c3153760a4f05b5e791e271ec3b7a128b8ec97ff7` | 0 |
| ARI–LV | `capture_3a04ea363187904257e2afa3` | `1550b0bdaac21947a378ecd7d04870b2b9d0ee866b5885a293b2b95afa8d1262` | 0 |
| LAC–HOU | `capture_a868adbaa49a7f50aeaf3818` | `ae8d121b44e136b106accd372c9c32ac35439172f27b5bc9d3b472facbb49d3d` | 0 |
| TEN–SF | `capture_80967deb4c5a4eec7454051d` | `98a05985eddff7c205a4a7db458f50d01559a0e4482d638f75cfac01debd0e2b` | 0 |

The comparison included every product field, null, list, and nested section.
Only HTTP transport and JSON formatting were outside the semantic comparison.
The comparison itself wrote nothing.

## Packet 2 in plain English

Packet 1 wrote and tested the safety rulebook. Packet 2 is the first time we
build the machinery that follows that rulebook.

Packet 2 will use one Snapshot Capture coordinator invocation to find the
eligible uncaptured slate, load shared evidence once, and build each normal
pregame GameLens response directly through the shared Python builder. It will
save and release one matchup payload at a time, read each saved result back,
and prove that an identical retry does not create a second copy.

The saved response is the important part. When `/game` eventually uses this
system, opening the frontend must **not** start Level 1 or rebuild the matchup.
The scheduled backend will have already done that work. `/game` will only read
the saved result and return it.

Preseason is useful for this first rehearsal because it lets us test the
plumbing and honest missing-data states without allowing preseason results to
enter production learning.

## Local implementation checkpoint — 2026-08-12

The required Packet 2 foundation was first implemented and locally tested on
`dev` in commit `28ffd4c` (`Implement Packet 2 shadow capture foundation`). At
that checkpoint, no BigQuery resource had been created and no real scheduled
game had been captured. The later development-cloud evidence is recorded in
the checkpoint above. No Cloud Run revision was deployed.

### Files implemented

- `services/gamelens_learning_contract.py` now enforces the exact
  `seasonType` rules and keeps `gameWeek` descriptive.
- `services/game_service.py` now separates evidence loading from the one shared
  response builder and adds fail-closed read-only pregame mode. The public
  `get_game_details(game_id)` signature and `/game` route are unchanged.
- `queries/game_queries.py` exposes mapping-only metric/ranking helpers so the
  batch path reuses the existing evidence shapes and `lens_tags` contract.
- `queries/gamelens_snapshot_queries.py` adds candidate discovery, immediate
  schedule recheck, and slate-shaped metric/ranking reads with the same strict
  pregame cutoffs as the live path.
- `services/gamelens_snapshot_capture.py` adds the thin sequential coordinator,
  upstream-summary readiness gate, stable retry check, exact field-level JSON
  diff, response/evidence split, tag validation, read-back verification, and
  readable stage result.
- `services/gamelens_snapshot_storage.py` adds the append-only development
  storage adapter and treats a BigQuery streaming-buffer restriction as
  `waiting/retryable`.
- `setup_gamelens_snapshot_tables.py` adds the dev-only idempotent
  create/verification path. It was run twice against Google Cloud development
  resources and verified the same two tables without destructive replacement.
- Focused coverage is in
  `tests/_gcp_stubs.py`,
  `tests/services/test_gamelens_learning_contract.py`,
  `tests/services/test_game_service_pregame_capture.py`,
  `tests/queries/test_gamelens_snapshot_queries.py`,
  `tests/services/test_gamelens_snapshot_capture.py`,
  `tests/services/test_gamelens_snapshot_handoff.py`, and
  `tests/services/test_gamelens_snapshot_storage.py`.

### Required development schemas implemented and verified

`GameLens_dev.pregame_snapshots`:

```text
capture_id STRING REQUIRED
learning_run_id STRING REQUIRED
game_id STRING REQUIRED
environment STRING REQUIRED
season STRING
season_type STRING
game_week STRING
game_status STRING
scheduled_kickoff TIMESTAMP REQUIRED
captured_at TIMESTAMP REQUIRED
capture_status STRING REQUIRED
payload_sha256 STRING REQUIRED
response_payload JSON REQUIRED
evidence_context JSON REQUIRED
lens_tags STRING REPEATED
ranking_context_available BOOLEAN
ranking_context_reason STRING
metric_source_date DATE
ranking_as_of_date DATE
metric_pipeline_run_id STRING
model_version STRING
ruleset_version STRING
```

The table is configured for daily partitioning on `captured_at` and clustering
by `game_id`, then `season_type`.

`GameLens_dev.stage_runs`:

```text
attempt_id STRING REQUIRED
stage_name STRING REQUIRED
status STRING REQUIRED
game_id STRING
season STRING
season_type STRING
input_count INTEGER
output_count INTEGER
started_at TIMESTAMP REQUIRED
finished_at TIMESTAMP REQUIRED
duration_ms INTEGER
upstream_run_id STRING
reason STRING
```

### Local evidence completed

- 51 focused Packet 2 capture/query/storage/handoff tests pass for the corrected
  implementation.
- The preceding foundation checkpoint recorded 97 existing and new `unittest`
  regression tests with cloud clients replaced by local mocks; failures: 0,
  errors: 0. The later JSON boundary, receipt-context, retry-lineage, and
  mixed-season safeguards are covered by the current focused suite.
- The deterministic parity test feeds identical fixed evidence to the live and
  capture entry paths and requires exact Python/JSON semantic equality across
  the complete response. No product fields or numeric differences are ignored.
- A populated handoff-contract test passes representative registry-backed
  evidence through the shared builder, JSON save/read-back shape, and the
  existing Level 1 extractor. It verifies the six historical claim types,
  populated Core Areas/categories, complete `lens_tags`, and explicit
  `learning_run_id` lineage without running Levels 1–4 or calling BigQuery.
- Pregame mode tests prove no final-score query and no outcome-writer call,
  even if final-score evidence and a final game status are supplied directly.
- A two-game loader test performs one metric query and one league-wide ranking
  query for the shared season/window group. The ranking cutoff preserves the
  live query's exact league-wide `as_of_date` selection order.
- Coordinator tests prove pre-build retry no-op, one-at-a-time
  build/save/read-back order, zero internal `/game` HTTP calls, independent
  per-game failure, schedule identity recheck, malformed-tag failure,
  saved-payload field-level diff, readable receipts, and safe
  `waiting/retryable` buffer handling without a Metric Pipeline rerun.
- Focused shadow-boundary tests prove an explicit production evidence source
  reads the production Schedule and Analytics tables only from a dev capture
  runtime, records that lineage beside the response, and cannot be enabled as
  a production capture mode. One-game tests prove that an explicit `game_id`
  reduces a larger discovered window to exactly one checked/built/saved game
  before evidence loading; a missing requested game is a safe no-op.
- The setup test proves repeat execution uses `exists_ok=True`, verifies the
  schemas, preserves matching objects, and refuses production before any
  create call.
- Python compilation and `git diff --check` pass.

### Review findings and deliberately deferred work

The response-building seam is `GameDetailsEvidence` →
`build_game_details_from_evidence(...)`. Both live and capture entry paths call
that builder; no football, matchup, claim-language, or response logic was
copied. The batch loader reuses the canonical row-to-evidence mapping helpers
and retrieves `lens_tags` only through the existing rankings evidence.

The optional production-hardening track remains unimplemented: no GCS
manifests or cross-store recovery, concurrency coordination, quarantine
machinery, readiness polling/backoff, generalized `pipeline_runs`, Admin,
frontend, production scheduler, production dataset, or production route work
was added.

### Final required work completed

The Thursday rehearsal, canonical-row inspection, slate receipt review, runtime
capture, six-game authenticated live parity comparison, per-game ledger,
historical backfill, identical retry, and Schedule coverage audit all passed on
2026-08-13. Packet 2 is complete.

Production behavior remains unchanged. Packet 3 (production-safe Level 1) is
next, but its companion document must be created and reviewed before Packet 3
implementation begins.

## What Packet 2 is not

Packet 2 does not:

- automatically launch Snapshot Capture after the daily 8:00 a.m. production data run; Packet 2 rehearses that handoff manually in development first;
- create production Level 1 claims;
- change the live production `/game` response;
- change the production frontend;
- grade a finished game;
- run Levels 2–4; or
- apply new calibration rules.

## Packet 2 completion scope

Packet 2 is intentionally a **small dev/shadow proof**, not the final production
platform. This document preserves the production-hardening ideas already
discussed, but separates them from the work required to finish Packet 2.

| Track | Meaning | Can it block Packet 2 completion? |
|---|---|---|
| **Required — Packet 2 completion** | The minimum implementation and evidence needed to prove safe pregame snapshot capture for one game and a slate | Yes |
| **Optional — production hardening** | Useful resilience, audit, cost, concurrency, Admin, and release work that may be attempted only after the required proof is stable | No |

An optional item that is unfinished, skipped, or fails must be recorded for a
later packet; it must not turn a successful required rehearsal into a Packet 2
failure. Optional work must also not delay the one-game proof or expand Packet 2
into production wiring.

### Required implementation for Packet 2

Packet 2 is complete only when all of the following are true:

1. The Packet 1 phase rule accepts exact `Regular Season` and `Postseason`
   production eligibility, keeps `Preseason` shadow-only, and safely skips
   blank or unknown `seasonType` values.
2. `GameLens_dev` and the required development tables can be created or
   verified repeatably without destructive replacement.
3. One coordinator invocation finds the eligible uncaptured slate and makes
   zero internal HTTP calls to `/game`.
4. The existing Python response logic builds in explicit read-only pregame mode,
   without querying final scores or reaching any outcome writer.
5. Shared schedule/metric/ranking evidence is reused across the slate rather
   than running the existing helper/query stack independently for every game.
6. Matchups are built, saved, verified, and released sequentially so the
   completed slate is not retained in memory.
7. BigQuery saves separate `response_payload` and `evidence_context`, while
   preserving relevant `lens_tags` as arrays.
8. A saved response can be read back and compared with the canonical builder
   output without running Level 1.
9. During the crossover period, that saved `response_payload` is semantically
   identical to the live `/game/<game_id>` JSON body for the same scheduled
   game and same pregame evidence.
10. An identical retry is detected before response rebuilding and creates no
    second canonical snapshot.
11. A simple Snapshot Capture stage receipt explains success, no-op, waiting,
    partial failure, or failure.
12. The one-game proof and slate rehearsal record enough runtime/memory evidence
    to judge whether the design is safe.
13. No production table, route, frontend, scheduler, Level 1, or outcome behavior
    changes.

### Optional production-hardening track

The following decisions remain preserved, but they are not Packet 2 completion
gates:

- immutable GCS `payload.json`, `evidence_context.json`, and
  `manifest.json` copies;
- hashes and cross-store GCS/BigQuery finalization;
- resumable recovery after one store succeeds and the other fails;
- simultaneous retry/concurrency protection;
- quarantine handling for genuinely conflicting payloads;
- automated bounded readiness polling and backoff;
- complete postponed/rescheduled-game audit lifecycle;
- detailed BigQuery bytes, slot-millisecond, and per-step timing accounting;
- a generalized `pipeline_runs` ledger;
- generalized `stage_runs` support for future Levels 1–4;
- a real Admin or frontend inspection surface; and
- production trigger, dataset, Cloud Run execution, and retention decisions.

These items can be implemented during Packet 2 if they remain small and do not
delay the required proof. Otherwise they move intact to a later hardening or
release packet.

## Decisions agreed before implementation

### 1. `seasonType` decides the season phase — Required

The exact schedule values are:

- `Preseason`
- `Regular Season`
- `Postseason`

Those exact values decide whether a game may be captured and how its evidence
may be used. `gameWeek` remains descriptive and may contain labels such as
`Hall of Fame Weekend`, `Week 1`, `Wild Card`, `Conference Championship`, or
`Super Bowl`. A new `gameWeek` label must not crash or silently change season
phase behavior.

| `seasonType` | Snapshot behavior | Learning behavior |
|---|---|---|
| `Preseason` | Dev/shadow rehearsal only | Excluded from production Levels 1–4 and weekly learning |
| `Regular Season` | Production eligible after release approval | Regular-season weekly learning |
| `Postseason` | Production eligible after release approval | Reviewed as postseason evidence; not automatically mixed into regular-season conclusions |
| Blank or unknown | Safe skip with a plain reason | No learning |

Regular-season and postseason games use the same capture machinery. Every
saved record retains `season_type`, so the learning reports can keep their
conclusions separate without creating two competing pipelines.

### 2. Candidate discovery is separate from capture permission — Required minimum

The today-plus-two-day Schedule window identifies games to inspect; it does not
mean every discovered game should be frozen immediately. Packet 2 keeps three
ideas separate:

- the **candidate window** finds upcoming games;
- the **capture policy** decides when a candidate is ready to freeze; and
- the **canonical snapshot** is the first successful capture after that policy
  says the game may be captured.

For Packet 2, the capture policy is a manually launched dev rehearsal before
kickoff. The eventual production capture time remains a later release decision.
This prevents a Thursday game from accidentally becoming permanently canonical
on Tuesday merely because it entered the lookahead window.

Immediately before saving each required proof snapshot, the coordinator must
re-read or verify the authoritative Schedule identity and confirm that the game
is still `Scheduled`, the current time is strictly before kickoff, and the
kickoff has not changed. If any value changed, Packet 2 must skip safely and
record the reason.

Preserving complete audit history and automatically deriving the new current
identity for postponed or rescheduled games remains an **optional
production-hardening item**.

### 3. GameLens receives its own BigQuery datasets — Required

The new learning/snapshot tables will not be added to the increasingly busy
`Analytics` dataset.

```text
GameLens_dev   development and shadow records
GameLens       eventual production records
```

Both datasets must use the same BigQuery location as the existing source
datasets. Packet 2 should add a small repeatable Python setup/verification
script so the development dataset and tables can be created safely without
manual console work. That script must be idempotent: running it again should
verify or preserve matching objects rather than destroy data.

Packet 2 creates or uses only `GameLens_dev`. Creating or writing `GameLens`
belongs to a later explicit production release step.

### 4. BigQuery serves the saved result — Required; GCS hardening — Optional

BigQuery stores two related but deliberately separate JSON documents:

- `response_payload` is the exact pregame response the future `/game` read
  path can return without rebuilding it.
- `evidence_context` contains the fuller internal lineage, available
  metric/ranking evidence, and complete relevant `lens_tags` needed by later
  learning and Admin work.

Separating them protects the public response contract while preserving useful
learning metadata. Packet 2 must compare the saved `response_payload` with the
canonical pregame builder output; adding `evidence_context` must not quietly
change the frontend response schema.

For Packet 2 completion, a capture becomes canonical after the BigQuery row is
written once, read back successfully, and verified against the stable capture
identity and `response_payload`. Packet 2 must not immediately update, delete,
or replace that newly streamed row.

#### Optional GCS and cross-store hardening

GCS may also store immutable `payload.json`, `evidence_context.json`, and
`manifest.json` copies for audit and recovery. When this optional track is
implemented, the fuller resumable save order is:

1. Derive the stable capture identity and hashes.
2. Create the immutable GCS objects with no-overwrite/generation protection.
3. Read and verify the GCS manifest and hashes.
4. Append/finalize the BigQuery canonical row without immediately updating or
   deleting a just-streamed row.
5. Read back the BigQuery row and verify identity plus hashes.

If GCS succeeds and the BigQuery finalize fails, a retry reuses and verifies
the identical GCS objects before completing BigQuery. It does not rebuild the
response or quarantine its own incomplete attempt as a conflicting payload.
A manifest/hash mismatch or a genuinely different payload remains a conflict
and is quarantined. Concurrent retries must use the same deterministic
identity and no-overwrite checks so only one identical capture can become
canonical.

The first successful BigQuery capture after the capture policy permits it is
canonical for the required Packet 2 proof. A retry checks for that complete
canonical capture before rebuilding the response or writing another row.
`/game` does not run Level 1 and does not rebuild the analysis because a user
opened the page.

GCS presence, manifest verification, cross-store hashes, resumable recovery,
concurrent retries, and conflict quarantine remain optional hardening and do
not block Packet 2 completion.

Packet 2 and the aligned Packet 1 contract use
`GameLens_dev.pregame_snapshots` and `GameLens_dev.stage_runs` for the
required shadow proof. `pipeline_runs` is preserved as optional generalized
run-ledger work. These locations do not alter Packet 1's safety rules.

### 5. `lens_tags` is protected metadata — Required

`lens_tags` is a useful product and learning contract, not decoration. For
example, one passing metric can carry `passing-efficiency`, `explosiveness`,
and `strong-signal` together.

The existing metric path is already authoritative and usable:

```text
metric_registry.py
→ Facts (`REPEATED STRING`)
→ Windowed Metrics (reattached from the registry)
→ Rankings (preserved)
```

The live `/game` response exposes tags only for its featured metric subset, so
Snapshot Capture must not treat the public response as the complete tag source.
It reuses the `lens_tags` arrays on the ranking/metric evidence already loaded
for the slate; it does not run a separate tag query per game or per metric.

Packet 2 must:

- preserve each relevant metric's existing `lens_tags` inside `evidence_context`;
- save a de-duplicated snapshot-level `lens_tags` array for simple BigQuery
  filtering and later Admin/frontend exploration;
- preserve the established shape: Python `list[str]`, BigQuery
  `REPEATED STRING`, and JSON array;
- never flatten, stringify, rename, or drop the tags; and
- report malformed tag data instead of silently changing it.

Packet 3 can attach the relevant tags to individual Level 1 claims. Later,
Level 4 can compare tagged evidence week over week.

### 6. Snapshot Capture leaves a readable receipt — Required minimum

`stage_runs` is the simple review surface for the upstream Metric Pipeline
reference and Packet 2 Snapshot Capture result. It must make the required
rehearsal understandable without reading Cloud logs.

Example:

```text
Metric Pipeline: succeeded
Snapshot Capture: succeeded — 1 game saved
Level 1: not started
Level 2: waiting for final score and accepted Facts
Level 3: waiting for Level 2
Level 4: not scheduled
```

For Packet 2, the receipt only needs the run/attempt identity, game when
applicable, stage name, status, counts, timestamps/duration, upstream reference,
and a plain reason. Generalizing this table for Levels 1–4, query-cost details,
and a future Admin view remains optional. Level 1 will later link back to the
Metric Pipeline and snapshot records it used rather than copying full upstream
reports into every claim.

### 7. Shared evidence and memory-safe slate processing — Required

The current `get_game_details(game_id)` path is correct for one live request,
but it was not designed as a slate-wide capture loop. Its helpers can fetch
overlapping evidence—for example, the header is loaded directly and can be
loaded again while fetching metrics and rankings.

Packet 2 must not simply place that current query stack inside a loop and call
it once per game. It also must not make one internal HTTP request to `/game`
for every matchup. The Flask route is a serving boundary, not the batch engine.

The capture design must separate **loading evidence** from **building the
response**:

1. Find scheduled games in the lookahead window.
2. Check existing canonical snapshots and remove already-captured games before
   doing expensive response work.
3. Load reusable schedule, metric, and ranking evidence in bounded batch
   queries for the remaining games/teams.
4. Pass the loaded header, metrics, rankings, `lens_tags`, and other
   pregame-safe context directly to the shared Python response-building logic
   in an explicit pregame-capture mode—without Flask, authentication, network
   calls, or HTTP `/game` requests.
5. Build one game's response in memory without hidden re-fetches, then save it,
   verify it, and release that completed payload before building the next game.
6. After all games are attempted, save the overall pipeline/stage receipts.

This remains one canonical response builder. Packet 2 may add an evidence
loader/context object and allow the current service functions to accept
already-loaded data. It must not copy matchup, scoring, or claim logic into a
second builder.

The current all-purpose `/game` path also checks final-score data and contains
the completed-game outcome/save path. Snapshot capture must not query final
scores, build Model Outcome, or call any outcome writer. Its explicit pregame
mode should return the same pregame product sections while making the
postgame-only path unreachable. This is both a safety boundary and one less
unnecessary query during batch capture.

#### Required minimum runtime evidence

For both the one-game proof and rehearsal slate, record:

- eligible games found;
- games skipped because a canonical snapshot already exists;
- confirmation of zero internal HTTP `/game` calls;
- total run duration;
- peak memory or the best available observed memory evidence; and
- Cloud Run memory/timeout outcome if the rehearsal runs there; and
- live `/game` crossover parity result: matched or mismatched, with a
  field-level diff when mismatched.

#### Required crossover response parity

Until the live `/game` route reads `GameLens_dev.pregame_snapshots`, two
paths temporarily exist:

1. the current live API path calls `get_game_details(game_id)`; and
2. Snapshot Capture calls the shared Python builder with already-loaded
   pregame evidence and saves its result.

For the same scheduled game, same pregame cutoff, and same underlying evidence,
the saved `response_payload` must be semantically identical to the JSON body
returned by `GET /game/<game_id>`.

The comparison must include every product field, value, null, list, and nested
section—including the header, Game Profile, Core Area Comparison, Matchup Lean,
Model Trust, Team Comparison, ranking context, claim-language context, and
matchup breakdown. The capture path may avoid the live path's final-score query,
but for a scheduled pregame it must still emit the same pregame response shape
and values, including the same `final_score` and `model_outcome` null or
unavailable representation.

Only transport differences may be ignored: HTTP status/headers, JSON object key
ordering, and whitespace/serialization formatting. `evidence_context` and the
snapshot-level `lens_tags` column are stored beside `response_payload`; they
must not be injected into the frontend response merely to satisfy this test.

Required proof has two layers:

- a deterministic focused test feeds the same fixed evidence into the live and
  capture entry paths and requires exact semantic JSON equality; and
- the dev rehearsal captures one scheduled game, immediately obtains the live
  dev `/game` JSON while the evidence is unchanged, and records an exact
  semantic comparison plus field-level diff.

Any product-field mismatch blocks Packet 2 GO. The response builder must be
reconciled; the test must not hide a difference with broad field exclusions or
approximate numeric tolerances.

The following measurements are useful but **optional** for Packet 2 completion:

- BigQuery query-job count;
- total bytes processed;
- total slot milliseconds;
- response-build time; and
- save/read-back time.

The acceptance rule is not an invented fixed query number. Focused tests must
show that shared evidence is loaded once per coordinator/slate context and that
per-game builders do not secretly re-fetch it. Observed query-job growth is
useful optional confirmation, not a fixed Packet 2 completion threshold.

The shadow runner must not execute inside a user's `/game` request and must
make zero internal HTTP `/game` calls. Thursday's
rehearsal may be launched manually against dev, where timing can be observed
without changing production traffic. The later production trigger will run
after the daily data/metric work and before users request the saved result.

### 8. Source readiness and BigQuery buffer safety — Required minimum

The existing Metric Pipeline is ordered Facts → Windowed Metrics → Rankings,
but it is not transactional. A successful Facts write can remain even when a
later stage fails. Snapshot Capture therefore must not treat “some rows exist”
as proof that the pregame evidence is ready.

Before building any required proof response, the coordinator must verify that:

- the known upstream Metric Pipeline attempt did not end in a failed or partial
  state;
- the required Facts, Windowed Metrics, and Rankings evidence can be read
  consistently for the rehearsal;
- expected early-season absence is distinguishable from an upstream failure;
  and
- a BigQuery buffer restriction is treated as a safe `waiting/retryable`
  result, never as permission for a blind full rerun.

Automated same-run lineage reconciliation, repeated stability polling, bounded
backoff, and detailed readiness-wait accounting remain optional production
hardening. The required Packet 2 behavior may stop safely and tell Christian
when to retry.

Expected early-season missing rankings may produce an honest unavailable
section. A failed, partial, or internally inconsistent Metric Pipeline must
leave Snapshot Capture in a visible `waiting` or `blocked` state and must not
freeze a canonical response.

#### Known BigQuery immediate-rerun limitation

GameLens has already encountered a different timing failure from the earlier
Cloud Run memory failure: an immediate manual repeat tried to replace a newly
streamed Schedule row while it was still inside BigQuery's streaming buffer.
The row was present exactly once and correct; after the buffer cleared, the
approved rerun succeeded without code or data repair.

Packet 2 must preserve that lesson:

- completion of one write is not permission to immediately `UPDATE`, `DELETE`,
  or replace the same newly streamed rows;
- snapshot attempts and stage receipts should be append/finalize oriented,
  avoiding a stream-then-immediate-mutate design;
- a BigQuery streaming-buffer restriction is recorded as
  `waiting/retryable`, not as corrupt data;
- the required proof stops safely, reports `waiting/retryable`, and does not
  blindly rerun the Metric Pipeline or rebuild an already-saved response;
- automatic bounded backoff and exact condition rechecking are optional
  hardening; and
- detailed readiness-wait duration and retry-count reporting are optional
  measurements.

No arbitrary fixed sleep is considered proof of readiness. Required Packet 2
behavior may stop safely before kickoff; automated waiting on observable
conditions belongs to the optional hardening track.

## Proposed saved records

### `GameLens_dev.pregame_snapshots` — Required

One row represents one canonical game snapshot. It should contain:

- required `learning_run_id`, `capture_id`, and `game_id`, plus the observed
  upstream Metric Pipeline run identity;
- environment, season, exact `season_type`, game week, and scheduled kickoff;
- capture timestamp and canonical capture state;
- exact ready-to-serve `response_payload` in a BigQuery JSON field;
- separate internal `evidence_context` JSON with source lineage, available
  metric/ranking evidence, and per-metric tags;
- de-duplicated snapshot-level `lens_tags` as `REPEATED STRING`;
- metric/ranking source dates and ranking availability;
- Facts, Windowed Metrics, and Rankings source/run references when available;
- available model/ruleset/feature/formula versions;
- a stable response-payload hash for retry comparison;
- optional evidence/manifest hashes and GCS object locations; and
- successful-capture metadata only. Attempt timing, counts, and plain skip,
  waiting, or error reasons belong in the required `stage_runs` receipt rather
  than creating incomplete snapshot rows for games that were not captured.

The table's game/capture identity must prevent duplicate canonical rows.

### `GameLens_dev.pipeline_runs` — Optional generalized ledger

One row summarizes the whole shadow Learning Pipeline attempt:

- run identity, environment, season, and included season types;
- started/finished timestamps and duration;
- overall status: success, no-op, partial failure, or failure;
- games checked, eligible, captured, skipped, quarantined, and failed;
- total query count, bytes processed, and slot milliseconds when available;
- the upstream Metric Pipeline run reference; and
- warnings plus a plain failure/no-op reason.

### `GameLens_dev.stage_runs` — Required minimum receipt

One row is one stage receipt. The minimum fields are:

- attempt/run ID;
- season and `season_type`;
- game ID when the stage is game-specific;
- stage name;
- status;
- input/output row or game counts;
- started/finished timestamps and duration;
- upstream run/stage reference; and
- plain warning, waiting, skip, or failure reason.

For an explicitly restricted one-game attempt, the receipt records that
requested `game_id`. For a slate attempt, `game_id` remains null because the
receipt summarizes multiple games. `season_type` is recorded only when every
checked candidate supplies the same value; mixed or unavailable season types
remain null rather than being guessed.

Packet 2 writes the upstream Metric Pipeline reference and Snapshot Capture
receipt. Query-cost columns and the generalized Levels 1–4/Admin shape are
optional and may remain null or be deferred.

### GCS raw copy — Optional production hardening

Proposed location:

```text
gamelens_learning/<environment>/<learning_run_id>/<game_id>/<capture_id>/
```

It contains immutable `payload.json`, `evidence_context.json`, and `manifest.json` objects.

## Thursday preseason slate rehearsal — completed 2026-08-13

The rehearsal proves plumbing and honest empty states, not prediction quality.
The first preseason game for a team may have no prior season-to-date evidence,
so Core Area Advantage, Matchup Lean, Team Comparison, or ranking sections may
be unavailable. That is acceptable when the saved response explains why.

### One-game proof — completed 2026-08-12

All required one-game steps below passed. Optional GCS hardening was deferred
and does not block Packet 2.

1. Refresh the normal dev schedule/data inputs.
2. Verify the upstream Metric Pipeline stages and source references are
   complete, consistent, and settled enough to read.
3. Select one scheduled preseason game before kickoff.
4. Build its response from explicitly loaded, read-only pregame evidence.
5. Recheck status, kickoff, and schedule identity immediately before finalize.
6. Save one canonical BigQuery row without immediately mutating it.
7. Read it back through a simple dev snapshot-reading script, service function,
   or equivalent inspection path.
8. Compare the saved `response_payload` with the canonical response builder
   output and inspect the separate `evidence_context`.
9. Immediately obtain the current live `/game/<game_id>` JSON while the pregame
   evidence is unchanged and require exact semantic equality with the saved
   `response_payload`; save a field-level diff if it does not match.
10. Repeat the capture and prove the retry performs no rebuild and creates no
    second canonical BigQuery row.
11. Review the simple stage receipt and required runtime/memory/parity evidence.
12. If optional GCS hardening is attempted, test it only after the BigQuery proof
    is already passing.

### Slate-shaped proof — completed 2026-08-13

The rehearsal ran the same selection/loading path for the
eligible Thursday slate or a read-only equivalent. Confirm that:

- one coordinator invocation handles the eligible uncaptured slate;
- each game is built and saved sequentially through the shared Python builder,
  with no internal HTTP `/game` calls and no collection of all finished
  payloads retained in memory;
- each eligible uncaptured game receives one independent snapshot result;
- one game's missing data does not stop the other games;
- reusable evidence is loaded in batches rather than re-fetched through each
  game's helper stack;
- the overall result can be success, no-op, partial failure, or failure; and
- the required runtime/memory evidence is understandable before any production
  sizing decision; detailed query-cost evidence is reviewed when collected.

The 2026-08-13 Schedule remained the expected six-game slate and all six games
were eligible before kickoff. The observed result was:

| Slate field | Observed value |
|---|---:|
| `games_discovered` | 6 |
| `games_checked` | 6 |
| `games_eligible` | 6 |
| `games_captured` | 5 new snapshots |
| `games_skipped` | 1 existing DET–CIN snapshot |
| `games_failed` | 0 |
| `internal_http_game_calls` | 0 |
| Canonical snapshot rows after rehearsal | 6 |
| Receipt `game_id` | null, by design for a multi-game slate |
| Receipt `season_type` | `Preseason` |
| Receipt input/output counts | 6 / 5 |

These values were observed rather than forced. The rehearsal created no
duplicates, did not rebuild DET–CIN, made no internal `/game` call, preserved
independent per-game results, wrote only to `GameLens_dev`, and produced a
truthful slate receipt. The subsequent authenticated comparison matched all six
saved responses to live `/game` with zero field differences.

## What Christian should be able to inspect

| View | Evidence |
|---|---|
| Required saved-response read-back | The exact saved pregame sections, including honest unavailable explanations |
| Required BigQuery snapshot row | `learning_run_id`, game/capture identity, phase, timestamps, state, source references, read-only source lineage, `lens_tags`, `response_payload`, and `evidence_context` |
| Required stage receipt | Metric Pipeline reference and Snapshot Capture status, counts, timing, and plain reason |
| Required live-API parity evidence | Saved `response_payload` and current live `/game` JSON match exactly by semantic content for the same pregame evidence |
| Required retry evidence | Same capture identity, zero rebuild work, and unchanged canonical-row count |
| Optional GCS inspection | Immutable payload, evidence context, and manifest when that hardening track is implemented |
| Optional frontend/Admin view | A richer human inspection surface after the simple read-back proof works |

## Test procedures

### Required Packet 2 tests

- no games scheduled produces a successful no-op;
- games present but none eligible produces a plain skip/no-op;
- exact `Preseason`, `Regular Season`, and `Postseason` handling;
- unfamiliar `gameWeek` with a valid `seasonType`;
- blank/unknown `seasonType` safe skip;
- scheduled game strictly before kickoff;
- exactly-at-kickoff rejection;
- status/kickoff identity recheck immediately before saving;
- first preseason game with honest missing prior-team evidence;
- expected missing rankings differs from failed/partial upstream work;
- `lens_tags` remain arrays in `evidence_context` and the snapshot-level field;
- populated shared-builder evidence survives JSON save/read-back and remains
  compatible with the existing Level 1 extractor across Core Area, category,
  metric, Game Profile, and Team Comparison claim types;
- malformed tags or payload fail only the affected game with a plain reason;
- pregame mode performs no final-score query and no Model Outcome write;
- one coordinator uses shared evidence and makes zero internal HTTP `/game`
  calls;
- matchups build/save/release sequentially;
- identical retry skips before rebuild and creates no duplicate canonical row;
- one game failure does not discard other slate results;
- BigQuery streaming-buffer restriction stops as `waiting/retryable` without
  an immediate mutation or blind Metric Pipeline rerun;
- saved `response_payload` reads back equal to the canonical builder output;
- fixed-evidence live and capture paths produce semantically identical response
  JSON;
- one real scheduled dev capture matches the immediately obtained current live
  `/game` JSON, with any mismatch producing a field-level diff and blocking
  GO;
- simple `stage_runs` receipt is understandable; and
- no production table, route, scheduler, Level, outcome, or frontend change.

### Optional hardening tests

These tests remain documented but do not block Packet 2 completion:

- postponed/rescheduled game preserves full audit history and derives the new
  current capture identity;
- GCS objects use no-overwrite/generation protection;
- GCS success followed by BigQuery failure resumes without rebuilding;
- BigQuery success followed by manifest failure resumes safely;
- payload, evidence, and manifest hashes agree across stores;
- simultaneous identical retries create one canonical capture;
- genuinely conflicting retry is quarantined;
- automated bounded readiness polling stops safely before kickoff;
- query count, bytes processed, slot milliseconds, and per-step timings are
  captured;
- generalized `pipeline_runs` and Levels 1–4 receipts work; and
- dev frontend/Admin inspection uses the saved snapshot.

## DRY boundary

Packet 2 may add:

- a thin shadow capture service;
- a batch evidence loader/context object;
- a BigQuery/GCS storage adapter;
- an idempotent Python dataset/table setup script;
- optional parameters that let the canonical game service reuse already-loaded
  header/metric/ranking evidence; and
- run/stage observability.

It must reuse the existing Schedule data, GameLens response-building logic,
Packet 1 identity/eligibility contract, configured environment clients, metric
metadata, and summary patterns. It must not copy matchup, model, calibration,
or claim logic.

## Packet 2 completion decision

**Final decision: GO — completed 2026-08-13.**

Every required Packet 2 question is answered yes:

- phase tests passed and preseason evidence remained isolated from production learning;
- one coordinator processed the slate through the shared Python builder with zero internal HTTP `/game` calls;
- responses were built, saved, read back, verified, and released one at a time;
- shared slate evidence was reused instead of re-fetching the full helper/query stack for each matchup;
- pregame mode avoided final-score queries and every outcome write;
- `response_payload` and `evidence_context` remained separate;
- relevant `lens_tags` survived as arrays, including the populated ARI–LV evidence case;
- saved payloads were readable without running Level 1;
- all six saved responses exactly matched authenticated live `/game` JSON with no excluded product fields or approximate tolerances;
- the identical retry and the later slate skip both protected the existing canonical DET–CIN row;
- focused tests cover partial upstream and BigQuery-buffer safe-stop behavior without rerunning the Metric Pipeline;
- one-game and slate receipts were readable and correctly identified;
- one-game and slate runtime evidence was recorded, with unavailable local Windows peak memory stated honestly; and
- production remained unchanged.

The optional hardening track remains deferred by design. GCS redundancy,
concurrency coordination, Admin/frontend inspection, production datasets,
Scheduler wiring, route cutover, retention, and final Cloud Run sizing belong
to later release work and do not weaken the Packet 2 snapshot-correctness GO.

## Decisions intentionally left for later release packets

Packet 2 gathers evidence but does not decide:

- the exact production trigger/wiring after the 8:00 a.m. run;
- the final Cloud Run service-versus-job execution choice for the production
  slate;
- production dataset/table creation;
- live `/game` cutover to saved snapshots; or
- retention rules for disposable dev rehearsal data.

Those decisions must use Packet 2's observed correctness, timing, query-cost,
and retry evidence rather than assumptions.

## Documentation handoff

This file is the completed Packet 2 evidence record. It preserves the
implementation and schemas, 66 relevant Packet 1/2 regression tests, one-game
capture and retry, receipt correction, six-game shadow rehearsal, six
canonical identities and hashes, exact authenticated live-API parity,
nine-row per-game audit backfill, zero-insert identical retry, August 6–13
coverage proof, runtime evidence, source/write boundaries, production impact,
and deliberately deferred optional hardening.

The next action is to review
[GameLens Packet 3 — Production-Safe Level 1 Plan](./GameLens_Packet_3_Production_Level_1.md).
Packet 3 consumes the approved canonical snapshot rather than rebuilding
GameLens, remains game-scoped and idempotent, preserves zero-claim processing,
and avoids production wiring until its own evidence gate is ready.

