# GameLens Backend August Readiness Roadmap

**Last revised:** 2026-07-27
**Status:** Canonical working roadmap
**Code source of truth:** Current `meow/main`

This document replaces the earlier August plan and implementation-map drafts. It is the single practical roadmap for backend readiness.

The standard is intentionally modest:

> Make the 2026 data path dependable, test each change locally, and preserve the claim-learning work already completed.

GameLens is not being redesigned. Existing metric builders, `/game` logic, Levels 1–4, Claim Health, and historical QA results should be reused.

---

## 1. What August readiness means

The August must-have is:

```text
Valid scheduled NFL data
→ dependable source tables
→ Facts
→ Windowed Metrics
→ Rankings
→ useful pregame-safe /game response
```

Levels 1–4 are the next learning track. They are important, but they do not need to become scheduled production jobs before `/game` is ready.

### Source and runtime map

```mermaid
flowchart TD
    A["Schedule API"] --> B["League.schedule"]
    C["Stats API"] --> D["Analytics.game_metrics_flat"]
    E["Scores API"] --> F["Scores.scores"]
    B --> G["Game/team metric facts"]
    D --> G
    G --> H["Windowed metrics"]
    H --> I["Metric rankings"]
    B --> J["/game"]
    F --> J
    H --> J
    I --> J
```

Required metric build order:

```text
Analytics.game_team_metric_facts_{season}
→ Analytics.team_metrics_windowed_{season}
→ Analytics.team_metric_rankings_{season}
```

### Claim-learning map

```text
Pregame-safe /game analysis
→ Level 1 claim extraction
→ final box score and completed-game Facts
→ Level 2 postgame validation
→ Level 3 feature enrichment
→ Claim Health
→ Level 4 calibration review when the sample is large enough
```

### Product-usage map

```text
User selects a game
→ frontend navigates to the matchup
→ GET /game/<game_id>
→ response is displayed
```

A user selection, a saved game-level outcome, and a claim-training row are different records.

User interest may later help prioritize teams, games, and explanations. It is not evidence that a football claim was correct and should not raise model confidence.

---

## 2. Current truths

These are the working assumptions this roadmap protects.

| Area                     | Current behavior                                                                                                                                                                                       | Roadmap decision                                                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| Schedule                 | Schedule ingestion supplies `League.schedule`, including game identity, status, season, teams, and dates.                                                                                              | Preserve it as the shared game identity source.                                                                          |
| Stats                    | The Stats loader appends parsed rows to `Analytics.game_metrics_flat` and updates the box-score-loaded marker. Its return value represents successfully processed **games**, not inserted metric rows. | Validate before insertion and name the count truthfully.                                                                 |
| Stats retry behavior     | A marker failure can leave inserted metric rows eligible for another retry.                                                                                                                            | Prevent a retry from blindly appending the same game again.                                                              |
| Facts duplicate handling | The Facts builder de-duplicates the grain `season + game + team + metric`.                                                                                                                             | Useful protection, but not a substitute for safe ingestion because corrected duplicate values have no reliable ordering. |
| Scores                   | The Scores loader can construct two rows even when its home/away line-score objects are empty.                                                                                                         | Validate meaningful score content; `len(rows) == 2` is insufficient.                                                     |
| `/game` final score      | `/game` reads final-score data directly from `Scores.scores`.                                                                                                                                          | Scores safety belongs in the August source track.                                                                        |
| Targeted runs            | `app.py` can pass `load_date` through the configured calls, but the loader signatures are not consistently aligned.                                                                                    | Normalize this small interface while each loader is already being edited.                                                |
| Scheduler errors         | The current scheduler catches individual API errors and can still return a success response.                                                                                                           | Return a truthful success, no-op, partial failure, or failure.                                                           |
| Legacy aggregation       | `app.py` still calls the annual 2025 aggregate after Stats reports activity.                                                                                                                           | Replace only this hook after the new metric conductor is tested.                                                         |
| Facts builder            | `run_build_game_team_metric_facts()` already builds, validates, dry-runs, and writes the season table.                                                                                                 | Reuse it.                                                                                                                |
| Windowed builder         | `run_build_windowed_metrics()` already builds, validates, dry-runs, and writes the season table.                                                                                                       | Reuse it.                                                                                                                |
| Rankings builder         | The existing rankings job produces `team_metric_rankings_{season}`.                                                                                                                                    | Reuse it after confirming its current callable entry point on `main`.                                                    |
| `/game` pregame safety   | Team metrics and rankings are selected strictly before the target game date.                                                                                                                           | Preserve this rule.                                                                                                      |
| Level 2                  | Validation uses completed-game Facts and merges by `run_id + claim_key`.                                                                                                                               | Reuse it; it is already broadly retry-friendly.                                                                          |
| Claim Health             | Admin reads claim-training examples for a selected `run_id`.                                                                                                                                           | Keep Claim Health in the learning path, separate from click tracking.                                                    |
| Final-game GET           | A final `/game/<game_id>` request may save a game-level outcome and trust details.                                                                                                                     | Do not confuse this side effect with Level 1 or user-event logging.                                                      |

---

## 3. The 80% scope decision

### Build now

* A pure box-score validator with focused tests
* Stats acceptance, marker, and retry safety
* Score validation and game-scoped retry safety
* One small Facts → Windowed → Rankings conductor
* A deliberate 2026 schema/build/runtime smoke test
* Final activation through current `app.py`

### Build after the August path is stable

* Game-scoped cumulative Level 1 production writes
* Levels 2–3 learning orchestration
* Claim Health pointed at the production learning run

### Do not build yet

* A new `ready/degraded/blocked` readiness subsystem
* A generalized multi-season scheduler
* A pipeline-run ledger table
* Mandatory immutable pregame snapshot storage
* Automatic daily Level 4 policy changes
* User-click analytics
* A `/game` SQL view or table-valued function
* A redesign of final-game model-outcome storage
* Broad refactors, renames, or formatting cleanup

For August, one explicit configured active season (`2026`) is acceptable. Historical or multi-season backfills can call the builders manually with an explicit season. Dynamic season discovery can wait until it solves a real problem.

---

## 4. Safety contract

1. Start every implementation packet from current `meow/main`.
2. Patch `app.py` last and preserve all current routes and authentication behavior.
3. Reject incomplete source payloads before marking a game loaded.
4. Do not rely on downstream de-duplication to make source retries safe.
5. Rebuild the three metric tables only after accepted Stats data exists.
6. Do not make Levels 1–4 prerequisites for `/game`.
7. Never reuse a historical QA `run_id` for production learning writes.
8. Never replace an entire cumulative production Level 1 run to update one game.
9. Keep Level 4 focused on claim-language support, not winner confidence.
10. Do not automatically apply Level 4 recommendations to runtime language.
11. Avoid a “readiness” test that silently calls final-game `get_game_details()` and writes model outcomes.
12. Stop after each locally proven, meaningful commit.

The season tables are intentionally rebuilt in full by the existing builders. That is acceptable after source validation. Claim-training history is different: production reruns must be game-scoped.

---

## 5. Code-change working agreement

When we implement a packet:

* If one Python function changes, provide the complete replacement function.
* If several tightly coupled functions change, provide the complete file.
* Identify the filename and exact function being replaced.
* Include focused tests with the behavior change.
* Do not mix cleanup with the requested behavior.
* Christian applies the change locally, runs it, examines the result, and commits before the next packet.
* Each commit should make one new behavior true.

This roadmap specifies behavior and test boundaries. Exact code is chosen only after reading the full current function on `main`.

---

# Track A — August must-have

## Packet 0 — Baseline and branch

**Goal:** Know the starting point before changing behavior.

Before Packet 1:

* update or compare the local branch with `meow/main`;
* record the current commit;
* run the closest existing ingestion tests;
* confirm the current Stats, Scores, config, rankings, and `app.py` function signatures;
* create a focused working branch.

No production or BigQuery change occurs.

---

## Packet 1 — Safe source ingestion

### Packet 1A — Pure Stats box-score validator

**Goal:** Decide whether a Tank01 Stats response is safe to parse and load.

**Create:**

```text
api_calls/api_utils/validate_nfl_boxscore.py
tests/api_calls/test_validate_nfl_boxscore.py
```

The validator should be pure: payload in, structured acceptance/rejection result out. It should not call Tank01, BigQuery, or the scheduler.

**Test at least:**

* valid final game with both teams and meaningful stats;
* non-final game;
* final status with an empty or skeleton body;
* only one team represented;
* missing game or team identity;
* malformed stat content;
* legitimate zero-valued statistics.

**Behavior impact:** None. Production does not import it yet.

**Done when:**

```text
payload
→ accepted
or
→ rejected with a useful reason
```

**Commit:**

```text
Add NFL boxscore payload validator and tests
```

### Packet 1B — Stats acceptance and retry safety

**Goal:** Publish only complete Stats data and keep retries from creating ambiguous duplicates.

**Update:**

```text
api_calls/api_call_nfl_stats.py
```

**Required sequence:**

```text
fetch response
→ validate
→ parse
→ confirm meaningful rows for both teams
→ insert or safely reconcile this game
→ confirm stored data
→ mark box score loaded
→ count the game as processed
```

**Required behavior:**

* A rejected response inserts nothing and leaves the game retryable.
* A marker is never written before valid Stats rows are confirmed.
* A marker-write failure is not counted as full success.
* A retry checks/reconciles existing rows for that game instead of blindly appending another copy.
* Existing complete rows plus a missing marker can be repaired without duplicating the game.
* The returned integer continues to mean processed games unless implementation proves a small result object is necessary.
* Variable names and log messages say `games`, not `rows`.
* The loader accepts the scheduler's optional `load_date` contract consistently, even if the parameter only influences eligibility indirectly.

The exact retry implementation may use a game-scoped existence/quality check, merge, or game-scoped replacement. Choose the smallest approach that fits the current loader.

**Local tests:**

1. Invalid response → no insert and no marker.
2. Valid response → insert confirmed before marker.
3. Insert failure → no marker and no success count.
4. Marker failure → run reports failure/incomplete.
5. Retry with complete existing rows → no duplicate insert.
6. Rejected game remains eligible for a later retry.

**Behavior impact:** Scheduled Stats acceptance changes.

**Commit:**

```text
Gate NFL stats loading and make retries safe
```

### Packet 1C — Score validation and retry safety

**Goal:** Prevent empty final-score rows and repeated game rows from entering `Scores.scores`.

**Update:**

```text
api_calls/api_call_nfl_scores.py
```

A small pure validation function can live in this file unless the current code shows genuine reuse elsewhere. Do not create a generalized validation framework.

**Required behavior:**

* Require a final game and meaningful home/away score content.
* Require both team types and the game identity.
* Do not treat two constructed dictionaries as proof of valid source data.
* Reject empty `lineScore` objects.
* Make the two game rows safe to retry through a game-scoped merge, replacement, or verified no-op.
* Align the optional `load_date` signature with the scheduler.

**Local tests:**

1. Empty home/away line scores → no write.
2. One missing team → no write.
3. Valid final score → exactly one home and one away row.
4. Same game retried → still exactly one valid row per team.
5. Valid correction → the stored game can be repaired without affecting other games.

**Behavior impact:** Scheduled score acceptance changes.

**Commit:**

```text
Validate and safely retry NFL score loads
```

---

## Packet 2 — Metric pipeline conductor

**Goal:** Call the existing season builders in the only valid order.

**Create:**

```text
services/gamelens_pipeline_orchestrator.py
tests/services/test_gamelens_pipeline_orchestrator.py
```

**Required order:**

```text
Facts
→ Windowed Metrics
→ Rankings
```

Confirm the current callable entry point in `build_metric_rankings.py` before implementation.

The conductor should:

* accept one explicit season;
* call the existing builders without copying their logic;
* stop when a stage raises or returns an invalid result;
* return a small plain summary containing the season, stage statuses, and row counts;
* log the failed stage;
* contain no Level 1–4 work.

Do not add a run-ledger table, custom workflow engine, retry queue, or generalized task framework.

**Local tests:**

1. Verify Facts → Windowed → Rankings call order.
2. Facts failure prevents both later builders.
3. Windowed failure prevents Rankings.
4. The returned summary names completed, skipped, and failed stages truthfully.
5. Existing builders complete a historical 2025 dry run/read-only comparison.

**Behavior impact:** None until `app.py` calls the conductor.

**Commit:**

```text
Add GameLens metric pipeline conductor
```

---

## Packet 3 — 2026 operational checkpoint

**Goal:** Prove the existing metric system can support 2026 before scheduling it.

This is primarily a deliberate local/BigQuery checkpoint, not a new subsystem.

### Schema and build checks

* Use the existing metric-table setup script.
* Compare the 2025 and 2026 schemas.
* Confirm `lens_tags` remains a repeated string field.
* Confirm expected partitioning/clustering if the setup script uses it.
* Run the three builders manually for 2026.
* Record source and output row counts.
* Inspect a small sample from each output table.

Required tables:

```text
Analytics.game_team_metric_facts_2026
Analytics.team_metrics_windowed_2026
Analytics.team_metric_rankings_2026
```

### Runtime smoke check

Use an eligible scheduled 2026 game and verify:

* the schedule/header resolves;
* both teams obtain the expected pregame-safe metrics when history exists;
* rankings appear when prior ranking history exists;
* legitimate early-season absence is explained or safely omitted;
* the response renders without changing matchup logic.

Do not create a new readiness service unless this smoke test exposes a recurring problem that existing tests cannot express.

**Behavior impact:** Only the deliberate 2026 table setup/build.

**Commit:** None unless code or the existing setup script actually changes.

---

## Packet 4 — Activate through `app.py`

**Goal:** Replace the legacy annual aggregate hook with the tested metric conductor.

**Update last:**

```text
app.py from current meow/main
```

**Required behavior:**

```text
run Schedule → Stats → Scores
→ no accepted Stats games: successful no-op for metric builds
→ accepted Stats games: run Facts → Windowed → Rankings for configured 2026
→ return truthful stage results
```

Keep this practical:

* Use one explicit configured active season for August.
* Preserve every blueprint, route, auth check, and scheduler entry point on current `main`.
* Remove only the old `aggregate_nfl_metrics_2025` hook.
* Rename Stats variables/logs from inserted rows to processed games.
* Do not report full success when an ingestion or metric stage failed.
* A Scores failure is reported, but valid Stats can still feed the metric pipeline.
* A metric failure keeps already accepted source data and identifies the failed builder.
* Do not put Levels 1–4 in this scheduler change.

**Local tests:**

1. Stats returns zero games → conductor not called.
2. Stats returns accepted games → conductor called once for 2026.
3. Stats fails → scheduler does not report full success.
4. Scores fails after valid Stats → metric behavior and partial failure are both truthful.
5. Conductor fails → response/log identifies the failed stage.
6. Targeted `load_date` no longer causes incompatible loader calls.
7. `/health`, `/games`, `/game`, Admin, user-management, and all other current routes remain registered.

**Behavior impact:** Activates scheduled Facts → Windowed → Rankings.

**Commit:**

```text
Wire the GameLens metric pipeline into scheduled ETL
```

### Track A is complete when

* incomplete Stats and Scores payloads cannot be published as complete;
* source retries do not create uncontrolled duplicate game rows;
* Stats reports processed games rather than pretending to report metric rows;
* Facts, Windowed Metrics, and Rankings run in order for 2026;
* failures stop only the stages that depend on them;
* the scheduler returns truthful results;
* existing routes and `/game` logic remain intact;
* the old annual 2025 aggregate hook is gone.

---

# Track B — Claim learning after Track A

Track B preserves and productionizes the existing learning system. It is not an August `/game` blocker.

## Packet 5 — Game-scoped production Level 1

**Goal:** Build a cumulative, replay-safe season claim history.

### First implementation choice

Use postgame reconstruction from pregame-safe inputs for the first production version:

```text
data_date < game_date
→ rebuild the game's /game-style claim set
→ write Level 1 rows for that game
```

This is sufficient for the first useful system. Immutable scheduled pregame snapshots can be added later if exact “what this code version displayed” auditability becomes important.

Avoid calling the current final-game `get_game_details()` in a way that silently writes `game_model_outcomes`. Use a pure payload builder or an explicit persistence control.

### Locked write rules

* Production receives its own versioned `run_id`, such as `prod_2026_v1`.
* Historical QA and calibration `run_id` values remain untouched.
* Rerunning Game A replaces or merges only Game A.
* Game B remains unchanged.
* Whole-run replacement stays available only for intentional historical QA batches.

**Local tests:**

1. Write Game A.
2. Write Game B.
3. Rerun Game A.
4. Prove Game B is unchanged.
5. Prove Game A is not duplicated.
6. Prove a historical QA run is untouched.

**Commit:**

```text
Add game-scoped production Level 1 writes
```

---

## Packet 6 — Levels 2–3 and Claim Health

**Goal:** Validate and enrich the cumulative production claim run.

```text
Eligible Level 1 claims
→ Level 2 postgame validation
→ Level 3 feature enrichment
→ Claim Health reads the production run
```

**Rules:**

* Level 2 waits for completed-game Facts.
* Preserve its `run_id + claim_key` merge behavior.
* Level 3 does not run when Level 2 fails.
* Learning failures do not roll back source or metric tables.
* Reruns do not duplicate claim rows.
* Claim Health names the selected production run explicitly.

Begin with copied/historical test data, dry runs, row counts, and bucket comparisons before production writes.

**Commit:**

```text
Orchestrate replay-safe Level 2 and Level 3 learning
```

---

## Level 4 remains manual and reviewable

For the first production season:

```text
collect an adequate claim sample
→ calculate Level 4 summaries
→ review sample size and stability
→ intentionally approve any runtime-language change
```

Do not create a daily Level 4 scheduler yet. A handful of new games is not enough evidence to alter language rules, and Level 4 is not required for `/game` availability.

---

# Track C — Deferred improvements

These are useful ideas, not forgotten work.

| Item                             | Why it waits                                                                |
| -------------------------------- | --------------------------------------------------------------------------- |
| Immutable pregame snapshots      | Reconstruction is adequate for the first production learning pass.          |
| Explicit game-view events        | Product analytics should not delay football data readiness.                 |
| Model-outcome persistence repair | Real retry/side-effect debt, but separate from the core metric path.        |
| Pipeline run ledger              | Structured logs and return summaries are enough until stable fields emerge. |
| Admin daily snapshots            | Live Claim Health can remain while production learning stabilizes.          |
| `/game` SQL simplification       | Current Python shaping and guardrails already work.                         |
| Frontend fallback cleanup        | Worth documenting, but not an August backend blocker.                       |
| Dynamic multi-season scheduling  | One configured 2026 season is enough for the immediate goal.                |

---

## 6. Commit map

| Order | Commit                                       | Production behavior changes?     |
| ----: | -------------------------------------------- | -------------------------------- |
|     0 | Update this roadmap                          | No                               |
|     1 | Add NFL boxscore payload validator and tests | No                               |
|     2 | Gate NFL Stats loading and make retries safe | Yes: Stats acceptance            |
|     3 | Validate and safely retry NFL score loads    | Yes: score acceptance            |
|     4 | Add GameLens metric pipeline conductor       | No                               |
|     5 | 2026 operational checkpoint                  | Only deliberate manual data work |
|     6 | Wire the metric pipeline into scheduled ETL  | Yes: scheduled metric builds     |
|     7 | Add game-scoped production Level 1 writes    | Later: learning storage          |
|     8 | Orchestrate replay-safe Levels 2–3           | Later: learning updates          |

There is intentionally no August commit for a run ledger, click analytics, snapshot storage, or automated Level 4 policy.

---

## 7. Local testing ladder

Use the same ladder for each packet:

1. Read the full current function/file.
2. Run syntax and import checks.
3. Run focused unit tests with no cloud writes.
4. Run mocked orchestration tests where relevant.
5. Run an existing builder in dry-run/read-only mode.
6. Review row counts, samples, and warnings.
7. Perform one deliberate BigQuery write only after the dry run is understood.
8. Review `git diff`.
9. Commit the one proven behavior.
10. Record the result and stop.

If a result is confusing, diagnose it before adding more code.

---

## 8. Session restart checklist

At the start:

* read this roadmap;
* name the current packet;
* confirm the latest completed commit;
* compare with current `meow/main`;
* inspect the complete affected function;
* state whether the packet is inert or changes production;
* state the exact local test that proves success.

At the end:

* record the test command and result;
* record important row counts;
* record the commit hash/message;
* mark the packet complete or blocked;
* name only the next packet.

---

## 9. Decisions that should not drift

* Schedule, Stats, and Scores have separate source destinations.
* Stats success means processed games, not inserted metric rows.
* Facts → Windowed Metrics → Rankings is the required builder order.
* `/game` uses historical data strictly before the target game date.
* Early-season ranking absence can be legitimate.
* `/game` availability and Levels 1–4 success are separate.
* Level 1 contains pregame-safe claims.
* Level 2 compares claims with completed-game Facts.
* Level 3 enriches claim rows with explanatory features.
* Level 4 calibrates claim-language support, not winner confidence.
* Claim Health belongs to the claim-learning path.
* Frontend selection is not currently a durable learning event.
* `app.py` is the final activation seam.

---

## 10. Next action

Begin with Packet 0, then complete Packet 1A only:

```text
Confirm current source functions on meow/main
→ create the pure Stats validator
→ add focused tests
→ run locally
→ commit
→ stop
```

No `app.py` change. No BigQuery write. No Levels 1–4 change.
