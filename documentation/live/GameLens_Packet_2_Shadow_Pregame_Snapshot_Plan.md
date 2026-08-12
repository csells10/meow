# GameLens Packet 2 — Shadow Pregame Snapshot Plan

**Status:** Local implementation complete; development-cloud proof awaiting review
**Created:** 2026-08-10  
**Revised:** 2026-08-12  
**Branch:** `dev`  
**Production behavior changed:** No  
**Production data written:** No

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

The required Packet 2 foundation is implemented and locally tested on `dev` in
commit `28ffd4c` (`Implement Packet 2 shadow capture foundation`). This is a
review checkpoint, not Packet 2 GO. No BigQuery dataset/table was created, no
Cloud Run revision was deployed, and no real scheduled-game rehearsal ran.

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
  create/verification path. It has been tested with fakes but deliberately not
  run against Google Cloud.
- Focused coverage is in
  `tests/_gcp_stubs.py`,
  `tests/services/test_gamelens_learning_contract.py`,
  `tests/services/test_game_service_pregame_capture.py`,
  `tests/queries/test_gamelens_snapshot_queries.py`,
  `tests/services/test_gamelens_snapshot_capture.py`,
  `tests/services/test_gamelens_snapshot_handoff.py`, and
  `tests/services/test_gamelens_snapshot_storage.py`.

### Required development schemas implemented locally

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

- 45 focused Packet 2 tests pass for the corrected local implementation.
- The preceding foundation checkpoint recorded 97 existing and new `unittest`
  regression tests with cloud clients replaced by local mocks; failures: 0,
  errors: 0. The two focused corrections are covered by the current 45-test
  Packet 2 suite.
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

### Remaining required work before Packet 2 GO

1. Review and approve the local code/schema checkpoint.
2. Deliberately run the setup path against development and verify a second run
   is a non-destructive verification. Do not create `GameLens` production
   resources.
3. Use an observed successful Facts → Windowed Metrics → Rankings summary and
   verify the actual development evidence is readable and internally
   consistent. A failed/partial summary or buffer restriction must stop the
   rehearsal; it must not trigger a blind pipeline rerun.
4. Capture one real scheduled development game, read the row back, and compare
   every saved `response_payload` field immediately with the live dev
   `/game/<game_id>` JSON while evidence is unchanged. Any mismatch blocks GO
   and must retain its field-level diff.
5. Repeat the identical capture and record the same identity, zero response
   rebuild, and unchanged canonical-row count.
6. Run the eligible rehearsal slate and record counts, receipt, duration, best
   available peak-memory evidence, zero internal HTTP calls, and sequential
   per-game outcomes.
7. Add the real row counts, parity result, runtime/memory result, setup/rehearsal
   commits, and final GO/NO-GO decision here.

Production behavior remains unchanged. Packet 3 (Level 1 claim extraction)
remains the next packet and must not begin until these required Packet 2 gates
are reviewed and pass.

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

Packet 2 writes the upstream Metric Pipeline reference and Snapshot Capture
receipt. Query-cost columns and the generalized Levels 1–4/Admin shape are
optional and may remain null or be deferred.

### GCS raw copy — Optional production hardening

Proposed location:

```text
gamelens_learning/<environment>/<learning_run_id>/<game_id>/<capture_id>/
```

It contains immutable `payload.json`, `evidence_context.json`, and `manifest.json` objects.

## Thursday preseason rehearsal

The rehearsal proves plumbing and honest empty states, not prediction quality.
The first preseason game for a team may have no prior season-to-date evidence,
so Core Area Advantage, Matchup Lean, Team Comparison, or ranking sections may
be unavailable. That is acceptable when the saved response explains why.

### One-game proof

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
9. Immediately obtain the live dev `/game/<game_id>` JSON while the pregame
   evidence is unchanged and require exact semantic equality with the saved
   `response_payload`; save a field-level diff if it does not match.
10. Repeat the capture and prove the retry performs no rebuild and creates no
    second canonical BigQuery row.
11. Review the simple stage receipt and required runtime/memory/parity evidence.
12. If optional GCS hardening is attempted, test it only after the BigQuery proof
    is already passing.

### Slate-shaped proof

After the one-game proof passes, run the same selection/loading path for the
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

## What Christian should be able to inspect

| View | Evidence |
|---|---|
| Required saved-response read-back | The exact saved pregame sections, including honest unavailable explanations |
| Required BigQuery snapshot row | `learning_run_id`, game/capture identity, phase, timestamps, state, source references, `lens_tags`, `response_payload`, and `evidence_context` |
| Required stage receipt | Metric Pipeline reference and Snapshot Capture status, counts, timing, and plain reason |
| Required live-API parity evidence | Saved `response_payload` and live dev `/game` JSON match exactly by semantic content for the same pregame evidence |
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
- one real scheduled dev capture matches the immediately obtained live dev
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

Packet 2 receives GO when Christian can answer yes to every **required** question:

- Did the phase tests pass, with preseason isolated from production evidence?
- Did one coordinator process the slate through the shared Python builder with
  zero internal HTTP `/game` calls?
- Were responses built, saved, verified, and released one at a time?
- Did the implementation reuse shared evidence rather than repeat the full
  helper/query stack for every matchup?
- Did pregame mode avoid final-score queries and every outcome write?
- Are `response_payload` and `evidence_context` separate?
- Did relevant `lens_tags` survive as arrays without a new per-game tag path?
- Can the saved payload be read back without running Level 1?
- For the same game and pregame evidence, does the saved `response_payload`
  exactly match the semantic JSON content returned by the live `/game` path,
  with no excluded product fields or approximate-value tolerance?
- Did an identical retry skip before rebuilding and create zero duplicate
  canonical rows?
- Did a partial upstream state or BigQuery buffer restriction stop safely
  without freezing inconsistent evidence or blindly rerunning the Metric
  Pipeline?
- Can Christian understand the simple Snapshot Capture receipt?
- Did the one-game and slate proofs provide understandable runtime/memory
  evidence?
- Is production unchanged?

The optional hardening checklist is reviewed separately. Unfinished optional
items are recorded for later; they do not change a required GO into NO-GO.

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

When Packet 2 is implemented, this file must be updated with exact files,
schemas, required focused tests, live-API parity evidence, observed
runtime/memory and row counts, optional hardening completed or deferred,
production impact, commit, and the
one next packet. Packet 3
does not begin until that review is understandable.
