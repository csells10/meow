# GameLens Packet 2 — Shadow Pregame Snapshot Plan

**Status:** Planning review revised; no implementation started  
**Created:** 2026-08-10  
**Revised:** 2026-08-11  
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

## What Packet 2 is not

Packet 2 does not:

- automatically launch Snapshot Capture after the daily 8:00 a.m. production data run; Packet 2 rehearses that handoff manually in development first;
- create production Level 1 claims;
- change the live production `/game` response;
- change the production frontend;
- grade a finished game;
- run Levels 2–4; or
- apply new calibration rules.

## Decisions agreed before implementation

### 1. `seasonType` decides the season phase

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

### 2. Candidate discovery is separate from capture permission

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

Immediately before finalizing each snapshot, the coordinator must re-read or
verify the authoritative Schedule identity and confirm that the game is still
`Scheduled`, the current time is strictly before kickoff, and the kickoff has
not changed. A postponed or rescheduled game keeps its old attempt as audit
history and receives a new capture identity for the new schedule identity; the
old snapshot must not be served as current.

### 3. GameLens receives its own BigQuery datasets

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

### 4. BigQuery serves the saved result; GCS preserves the raw copy

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

GCS stores immutable `payload.json`, `evidence_context.json`, and
`manifest.json` copies for audit and recovery. A capture becomes canonical
only after all required objects exist, their recorded hashes match, and the
final BigQuery row has been read back successfully.

The resumable save order is:

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

The first successful capture after the capture policy permits it is canonical.
A retry checks for that complete canonical capture before rebuilding the
response or writing another object. `/game` does not run Level 1 and does not
rebuild the analysis because a user opened the page.

Packet 2 supersedes Packet 1's provisional `Analytics.gamelens_*` table
locations with `GameLens_dev.pregame_snapshots`, `pipeline_runs`, and
`stage_runs`. Packet 1 should later receive a matching documentation cleanup;
this location change does not alter its safety rules.

### 5. `lens_tags` is protected metadata

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

- preserve each relevant metric's existing `lens_tags` inside the frozen response;
- save a de-duplicated snapshot-level `lens_tags` array for simple BigQuery
  filtering and later Admin/frontend exploration;
- preserve the established shape: Python `list[str]`, BigQuery
  `REPEATED STRING`, and JSON array;
- never flatten, stringify, rename, or drop the tags; and
- report malformed tag data instead of silently changing it.

Packet 3 can attach the relevant tags to individual Level 1 claims. Later,
Level 4 can compare tagged evidence week over week.

### 6. Every stage leaves a readable receipt

`stage_runs` is the simple review surface for the Metric Pipeline, Snapshot
Capture, and Levels 1–4. It should make the current state understandable
without reading Cloud logs.

Example:

```text
Metric Pipeline: succeeded
Snapshot Capture: succeeded — 1 game saved
Level 1: not started
Level 2: waiting for final score and accepted Facts
Level 3: waiting for Level 2
Level 4: not scheduled
```

Level 1 will later link back to the Metric Pipeline and snapshot records it
used. It should not copy the full Facts, Windowed Metrics, and Rankings run
report into every claim.

### 7. Query reuse and cost control are Packet 2 work, not later cleanup

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

#### Required cost and runtime evidence

For both the one-game proof and the rehearsal slate, record:

- eligible games found;
- games skipped because a canonical snapshot already exists;
- BigQuery query-job count;
- total bytes processed;
- total slot milliseconds when available;
- response-build time;
- save/read-back time;
- total run duration; and
- peak memory for the one-game proof and slate rehearsal; and
- Cloud Run memory/timeout outcome when the rehearsal runs there.

The acceptance rule is not an invented fixed query number. The evidence must
show that repeated header/evidence lookups were removed and that query growth
is batch-oriented, rather than multiplying the full current query stack by
every additional game.

The shadow runner must not execute inside a user's `/game` request and must
make zero internal HTTP `/game` calls. Thursday's
rehearsal may be launched manually against dev, where timing can be observed
without changing production traffic. The later production trigger will run
after the daily data/metric work and before users request the saved result.

### 8. Source readiness and BigQuery settling are explicit gates

The existing Metric Pipeline is ordered Facts → Windowed Metrics → Rankings,
but it is not transactional. A successful Facts write can remain even when a
later stage fails. Snapshot Capture therefore must not treat “some rows exist”
as proof that the pregame evidence is ready.

Before building any response, the coordinator must verify that:

- the upstream Metric Pipeline run reports the required stages successful;
- Facts, Windowed Metrics, and Rankings references/as-of dates belong to the
  same accepted run state;
- expected early-season absence is distinguishable from a failed or partial
  upstream build; and
- the observed table state stays consistent through a bounded readiness check.

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
- retry uses bounded backoff and rechecks the exact row/table condition before
  continuing; it does not blindly rerun the whole Metric Pipeline or rebuild
  an already-hashed response; and
- the rehearsal records readiness-wait duration and retry count separately
  from response-build time.

No arbitrary fixed sleep is considered proof of readiness. The implementation
must wait on observable stage/table conditions and preserve a clear timeout
that stops safely before kickoff.

## Proposed saved records

### `GameLens_dev.pregame_snapshots`

One row represents one canonical game snapshot. It should contain:

- `capture_id`, `learning_run_id`, `pipeline_run_id`, and `game_id`;
- environment, season, exact `season_type`, game week, and scheduled kickoff;
- candidate-discovery timestamp, capture-policy decision, capture timestamp, and state;
- exact ready-to-serve `response_payload` in a BigQuery JSON field;
- separate internal `evidence_context` JSON with source lineage, available metric/ranking evidence, and per-metric tags;
- de-duplicated snapshot-level `lens_tags` as `REPEATED STRING`;
- metric/ranking source dates and ranking availability;
- Facts, Windowed Metrics, and Rankings source/run references when available;
- model, ruleset, feature, and formula versions;
- response-payload hash, evidence-context hash, and manifest hash;
- GCS payload/manifest object locations; and
- a plain skip, quarantine, or error reason when appropriate.

The table's game/capture identity must prevent duplicate canonical rows.

### `GameLens_dev.pipeline_runs`

One row summarizes the whole shadow Learning Pipeline attempt:

- run identity, environment, season, and included season types;
- started/finished timestamps and duration;
- overall status: success, no-op, partial failure, or failure;
- games checked, eligible, captured, skipped, quarantined, and failed;
- total query count, bytes processed, and slot milliseconds when available;
- the upstream Metric Pipeline run reference; and
- warnings plus a plain failure/no-op reason.

### `GameLens_dev.stage_runs`

One row is one stage receipt. The minimum fields are:

- attempt/run ID;
- season and `season_type`;
- game ID when the stage is game-specific;
- stage name;
- status;
- input/output row or game counts;
- started/finished timestamps and duration;
- query count and bytes processed when the stage queried BigQuery;
- upstream run/stage reference; and
- plain warning, waiting, skip, or failure reason.

Packet 2 writes Metric Pipeline reference and Snapshot Capture receipts. The
same shape is reserved for Levels 1–4 so Admin can eventually show every step
consistently.

### GCS raw copy

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
6. Save it through the resumable GCS-then-BigQuery protocol.
7. Read it back through the dev snapshot-reading path.
8. Compare the saved `response_payload` with the canonical response builder
   output and inspect the separate `evidence_context`.
9. Repeat the capture and prove the retry performs no rebuild and creates no
   second canonical row/object.
10. Review stage receipts, readiness waits/retries, and query/runtime
    measurements.

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
- the recorded query/runtime evidence is understandable before any production
  sizing decision.

## What Christian should be able to inspect

| View | Evidence |
|---|---|
| Dev frontend or saved-response preview | The exact saved pregame sections, including honest unavailable explanations |
| BigQuery snapshot row | Game identity, phase, timestamps, state, source references, versions, `lens_tags`, payload, and GCS locations |
| BigQuery pipeline/stage receipts | Metric Pipeline and Snapshot Capture status, counts, timing, query cost, and plain reasons |
| GCS objects | One immutable payload and manifest for each successful canonical capture |
| Retry evidence | Same capture identity, zero rebuild work, and unchanged row/object counts |

## Required edge-case checks

- no games scheduled;
- games present but none eligible;
- scheduled game before kickoff;
- exactly-at-kickoff rejection;
- candidate discovered before the capture policy permits freezing;
- status/kickoff recheck immediately before canonical finalize;
- postponed or rescheduled game retains audit history but uses a new current
  capture identity;
- exact `Preseason`, `Regular Season`, and `Postseason` handling;
- unfamiliar `gameWeek` label with a valid `seasonType`;
- blank/unknown `seasonType` safe skip;
- first preseason game with no prior team evidence;
- missing rankings caused by honest early-season absence;
- partial or inconsistent Facts → Windowed Metrics → Rankings state blocks capture;
- BigQuery streaming-buffer restriction produces bounded waiting/retry rather
  than a blind full rerun;
- missing or malformed `lens_tags`;
- blank/malformed payload;
- pregame mode performs no final-score query;
- identical retry skipped before response rebuild;
- GCS success followed by BigQuery-finalize failure resumes without rebuilding;
- simultaneous identical retries produce one canonical capture;
- conflicting retry quarantined;
- one game failing without losing other slate results;
- no Model Outcome write; and
- no production table, route, or frontend change.

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

## Stop/go questions

Packet 2 receives GO only when Christian can answer yes to all of these:

- Can I see when a game became a candidate, when capture was permitted, and exactly when it was frozen?
- Can I open the saved payload without searching logs?
- Can the dev preview/read path use the saved payload without running Level 1?
- Did the retry skip before rebuilding and create zero duplicates?
- Is `seasonType` authoritative even when `gameWeek` is unfamiliar?
- Is preseason clearly excluded from production evidence?
- Are regular season and postseason captured by one system but reviewed
  separately for learning?
- Are the exact `response_payload` and richer `evidence_context` stored
  separately?
- Are the existing Facts/Windowed/Rankings `lens_tags` reused without a
  separate per-game tag retrieval path, preserved as arrays inside
  `evidence_context`, and easy to inspect?
- Can I see Metric Pipeline and Snapshot Capture receipts in `stage_runs`?
- Did one coordinator process the slate through the shared Python builder with
  zero internal HTTP `/game` calls?
- Were responses saved and released one at a time instead of retaining the
  completed slate in memory?
- Did the implementation remove repeated evidence/header fetches rather than
  postpone that work?
- Can I see query count, bytes processed, runtime, readiness-wait duration,
  and retry count for one game and the rehearsal slate?
- Did partial upstream data or a BigQuery streaming-buffer restriction stop
  safely without freezing inconsistent evidence or blindly rerunning the
  entire Metric Pipeline?
- Can an interrupted GCS/BigQuery save resume to one verified canonical
  snapshot?
- Did the capture avoid every outcome write?
- Is production still unchanged?

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
schemas, focused tests, query-job/bytes/runtime measurements, observed
row/object counts, production impact, commit, and the one next packet. Packet 3
does not begin until that review is understandable.
