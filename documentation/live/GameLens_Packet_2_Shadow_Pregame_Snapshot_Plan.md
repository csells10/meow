# GameLens Packet 2 — Shadow Pregame Snapshot Plan

**Status:** Planning review revised; no implementation started  
**Created:** 2026-08-10  
**Revised:** 2026-08-10  
**Branch:** `dev`  
**Production behavior changed:** No  
**Production data written:** No

## Packet 2 in plain English

Packet 1 wrote and tested the safety rulebook. Packet 2 is the first time we
build the machinery that follows that rulebook.

Packet 2 will take an eligible scheduled game, build the normal pregame
GameLens response once, save it, read it back, and prove that an identical
retry does not create a second copy.

The saved response is the important part. When `/game` eventually uses this
system, opening the frontend must **not** start Level 1 or rebuild the matchup.
The scheduled backend will have already done that work. `/game` will only read
the saved result and return it.

Preseason is useful for this first rehearsal because it lets us test the
plumbing and honest missing-data states without allowing preseason results to
enter production learning.

## What Packet 2 is not

Packet 2 does not:

- connect the Learning Pipeline to the 8:00 a.m. production run;
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

### 2. GameLens receives its own BigQuery datasets

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

### 3. BigQuery serves the saved result; GCS preserves the raw copy

- BigQuery stores the searchable, ready-to-serve snapshot and operational
  metadata used by `/game` and Admin.
- GCS stores immutable `payload.json` and `manifest.json` copies for audit and
  recovery.
- The first valid capture is canonical. A retry checks for that capture before
  rebuilding the response or writing another object.
- `/game` does not run Level 1 and does not rebuild the analysis because a user
  opened the page.

### 4. `lens_tags` is protected metadata

`lens_tags` is a useful product and learning contract, not decoration. For
example, one passing metric can carry `passing-efficiency`, `explosiveness`,
and `strong-signal` together.

Packet 2 must:

- preserve each metric's `lens_tags` inside the frozen response;
- save a de-duplicated snapshot-level `lens_tags` array for simple BigQuery
  filtering and later Admin/frontend exploration;
- preserve the established shape: Python `list[str]`, BigQuery
  `REPEATED STRING`, and JSON array;
- never flatten, stringify, rename, or drop the tags; and
- report malformed tag data instead of silently changing it.

Packet 3 can attach the relevant tags to individual Level 1 claims. Later,
Level 4 can compare tagged evidence week over week.

### 5. Every stage leaves a readable receipt

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

### 6. Query reuse and cost control are Packet 2 work, not later cleanup

The current `get_game_details(game_id)` path is correct for one live request,
but it was not designed as a slate-wide capture loop. Its helpers can fetch
overlapping evidence—for example, the header is loaded directly and can be
loaded again while fetching metrics and rankings.

Packet 2 must not simply place that current query stack inside a loop and call
it once per game.

The capture design must separate **loading evidence** from **building the
response**:

1. Find scheduled games in the lookahead window.
2. Check existing canonical snapshots and remove already-captured games before
   doing expensive response work.
3. Load reusable schedule, metric, and ranking evidence in bounded batch
   queries for the remaining games/teams.
4. Pass the loaded header, metrics, rankings, and other pregame-safe context to
   the existing response-building logic in an explicit pregame-capture mode.
5. Build each game's response in memory without hidden re-fetches of the same
   evidence.
6. Save the snapshot, tags, manifest, and stage receipt.

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
- Cloud Run memory/timeout outcome when the rehearsal runs there.

The acceptance rule is not an invented fixed query number. The evidence must
show that repeated header/evidence lookups were removed and that query growth
is batch-oriented, rather than multiplying the full current query stack by
every additional game.

The shadow runner must not execute inside a user's `/game` request. Thursday's
rehearsal may be launched manually against dev, where timing can be observed
without changing production traffic. The later production trigger will run
after the daily data/metric work and before users request the saved result.

## Proposed saved records

### `GameLens_dev.pregame_snapshots`

One row represents one canonical game snapshot. It should contain:

- `capture_id`, `learning_run_id`, `pipeline_run_id`, and `game_id`;
- environment, season, exact `season_type`, game week, and scheduled kickoff;
- capture timestamp and state;
- complete pregame payload in a BigQuery JSON field;
- de-duplicated `lens_tags` as `REPEATED STRING`;
- metric/ranking source dates and ranking availability;
- Facts, Windowed Metrics, and Rankings source/run references when available;
- model, ruleset, feature, and formula versions;
- payload hash;
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

It contains immutable `payload.json` and `manifest.json` objects.

## Thursday preseason rehearsal

The rehearsal proves plumbing and honest empty states, not prediction quality.
The first preseason game for a team may have no prior season-to-date evidence,
so Core Area Advantage, Matchup Lean, Team Comparison, or ranking sections may
be unavailable. That is acceptable when the saved response explains why.

### One-game proof

1. Refresh the normal dev schedule/data inputs.
2. Select one scheduled preseason game before kickoff.
3. Build its response from explicitly loaded, read-only pregame evidence.
4. Save it as shadow evidence in `GameLens_dev` plus the dev GCS location.
5. Read it back through the dev snapshot-reading path.
6. Compare saved visible sections with the canonical response builder output.
7. Repeat the capture and prove the retry performs no rebuild and creates no
   second canonical row/object.
8. Review the stage receipt and query/runtime measurements.

### Slate-shaped proof

After the one-game proof passes, run the same selection/loading path for the
eligible Thursday slate or a read-only equivalent. Confirm that:

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
- exact `Preseason`, `Regular Season`, and `Postseason` handling;
- unfamiliar `gameWeek` label with a valid `seasonType`;
- blank/unknown `seasonType` safe skip;
- first preseason game with no prior team evidence;
- missing rankings;
- missing or malformed `lens_tags`;
- blank/malformed payload;
- pregame mode performs no final-score query;
- identical retry skipped before response rebuild;
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

- Can I see exactly which game was captured and when?
- Can I open the saved payload without searching logs?
- Can the dev preview/read path use the saved payload without running Level 1?
- Did the retry skip before rebuilding and create zero duplicates?
- Is `seasonType` authoritative even when `gameWeek` is unfamiliar?
- Is preseason clearly excluded from production evidence?
- Are regular season and postseason captured by one system but reviewed
  separately for learning?
- Are `lens_tags` preserved as arrays and easy to inspect?
- Can I see Metric Pipeline and Snapshot Capture receipts in `stage_runs`?
- Did the implementation remove repeated evidence/header fetches rather than
  postpone that work?
- Can I see query count, bytes processed, and runtime for one game and the
  rehearsal slate?
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
