# GameLens Packet 4 — Controlled 2025 Replay Plan

**Prepared:** 2026-08-01  
**Repository:** `csells10/meow`  
**Working branch:** `dev`  
**Status:** `PLAN ONLY — NO REPLAY INFRASTRUCTURE OR CLOUD TEST HAS BEEN RUN`  
**Related handoff:** `documentation/August/GameLens_Packet_4_Pause_And_Resume_Handoff_20260801.md`

---

## 1. Decision in one paragraph

Packet 4 should be proven with one completed 2025 game before the first completed 2026 preseason game becomes the debugger. The rehearsal will call the real `nfl-games-app-dev` Cloud Run service, use the real `POST /` scheduler seam, fetch real Tank01 data, write only to isolated development copies, run the real Facts → Windowed Metrics → Rankings conductor for season 2025, and then compare the rebuilt development outputs with frozen 2025 baselines. The replay must be date-scoped, truthful about failures, safe to run twice, and physically unable to write to production BigQuery datasets or the production raw-response bucket.

This is a **new proof path**, not a replacement for the pause-and-resume handoff:

- The handoff records where the 2026 activation work stopped and what local tests already passed.
- This document describes how to remove the remaining cloud uncertainty using stable 2025 data.
- Passing this rehearsal proves the machinery.
- It does not automatically authorize a production deployment, enable Scheduler, merge `dev` into `main`, or prove that a future 2026 provider payload is identical.

---

## 2. Why the current dev endpoint must not be called yet

Current `dev` behavior has several replay hazards:

1. `app.py` hard-codes `ACTIVE_NFL_SEASON = "2026"`. A 2025 `load_date` would still send the metric conductor to the 2026 tables.
2. The Stats loader accepts `load_date`, but its current processing query is not guaranteed to restrict the backlog to that date.
3. The historical Schedule loader does not currently recreate a past schedule date. The development schedule must therefore be pre-seeded.
4. BigQuery source, status, and derived destinations are hard-coded across several modules.
5. Raw API responses can use the production GCS bucket and overwrite similarly named objects.
6. The `/test` route does not return the full pipeline summary and can report HTTP 200 without evaluating the summary. The controlled test must use `POST /`.
7. Individual ingestion failures can collapse into zero accepted Stats games and resemble a valid no-op unless failure details are preserved.
8. `cloudbuild-dev.yaml` currently deploys the dev service without persisting the intended 900-second Cloud Run request timeout.

Therefore:

> Do not send a historical `POST /` request to the current dev revision. Build and prove the isolation seam first.

---

## 3. Non-negotiable safety contract

The replay is allowed only when all of these statements are true:

- The service name is exactly `nfl-games-app-dev`.
- The active season returned by the service is `2025`.
- Every writable BigQuery object resolves to a development dataset.
- The raw-response bucket is development-only.
- The runtime service account can write to the development datasets and bucket.
- The runtime service account cannot write to the production datasets or production raw bucket.
- The chosen replay date contains exactly one eligible completed game in the development schedule.
- Stats and Scores are both restricted to that replay date or selected game.
- The production Scheduler remains unchanged and the dev trigger remains disabled unless a separate approved deployment step requires a manual build.
- No step touches `main`, production Cloud Run traffic, production Scheduler, Levels 1–4, historical QA `run_id` values, claim formulas, or model language.
- `lens_tags` remains a Python `list[str]`, BigQuery `REPEATED STRING`, and JSON array.
- A failure or partial result is inspected before any rerun.

### Immediate stop signs

Stop without improvising if any check shows:

- `League`, `Scores`, or `Analytics` as a writable destination instead of the approved dev dataset;
- the production raw-response bucket;
- season `2026` in a 2025 rehearsal response;
- more than one eligible Stats game for the selected date;
- a production service URL;
- an unexpected non-empty backlog outside the replay date;
- missing baseline tables or views;
- `lens_tags` flattened or stringified;
- a stage that wrote before a later stage failed;
- a local branch divergence or unrelated working-tree change that would be overwritten.

---

## 4. Isolation layout

Use writable BigQuery table clones for working copies and frozen baselines. Recreate views separately because views are not table clones.

The exact dataset names should be confirmed before creation. The working names below make the safety boundary obvious:

| Dataset | Working objects used by dev service | Frozen comparison objects |
| --- | --- | --- |
| `League_dev` | `schedule`, `boxscore_status`, `games_to_process` view | `schedule_baseline`, `boxscore_status_baseline` |
| `Scores_dev` | `scores`, `score_status`, `scores_to_process` view | `scores_baseline`, `score_status_baseline` |
| `Analytics_dev` | `game_metrics_flat`, `game_team_metric_facts_2025`, `team_metrics_windowed_2025`, `team_metric_rankings_2025` | matching `*_baseline` tables |

The development views must reference only working development tables. They must not point back to production datasets.

Use a separate development raw-response bucket. A suggested name may be chosen later, but it must be verified to be development-only before deployment.

### Why keep frozen baselines?

The conductor replaces the three active derived tables. If the working table itself is the only clone, the known-good historical result disappears during the test. Frozen baseline clones let us compare:

- row counts;
- stable-grain duplicate counts;
- schemas;
- null behavior;
- `lens_tags` type and values;
- per-game and per-team content;
- aggregate fingerprints where practical.

The previously recorded 2025 derived counts were:

| Table | Recorded rows |
| --- | ---: |
| Facts | 36,532 |
| Windowed Metrics | 138,532 |
| Rankings | 426,086 |

These are **breadcrumbs, not eternal constants**. Re-query and record the production/frozen baseline immediately before the rehearsal.

---

## 5. The easy-chunk plan

Each chunk ends at a safe stopping point. Do not combine chunks merely to make the work feel faster.

| Chunk | Name | Cloud writes? | Safe stopping point |
| --- | --- | ---: | --- |
| R0 | Re-establish truth | No | Branch, docs, tests, and current targets recorded |
| R1 | Fail-closed runtime targeting | No | Local tests prove dev cannot resolve production write targets |
| R2 | Targeted replay behavior | No | Local tests prove one date cannot drain the full backlog |
| R3 | Truthful response and season | No | Local tests prove `POST /` reports 2025 and failures honestly |
| R4 | Build the isolated sandbox | Yes, dev only | Datasets, bucket, service account, clones, and views exist |
| R5 | Choose and rewind one game | Yes, dev only | Exactly one game is eligible in working clones |
| R6 | Deploy and preflight dev | Yes, dev only | Health/config proof passes; ingestion has not run |
| R7 | Run the positive replay once | Yes, dev only | One captured response and no automatic retry |
| R8 | Verify rebuilt data | Read-only verification | Results match defined acceptance checks |
| R9 | Prove idempotency | Yes, dev only | Same request produces no duplicates and safe no-op/rebuild behavior |
| R10 | Close Packet 4 evidence | Documentation only | Roadmap/handoff updated with result and next decision |

---

# Part A — Local harness work

## Chunk R0 — Re-establish the truth

### Goal

Confirm the repository state and preserve the evidence already collected.

### Steps

From the repository root:

~~~bash
git switch dev
git status --short --branch
git fetch origin
git log -5 --oneline --decorate
git log -1 --oneline origin/dev
~~~

If local `dev` is only behind and the working tree is understood:

~~~bash
git pull --ff-only origin dev
~~~

Read in this order:

1. `GameLens_August_Readiness_Roadmap_How_To.md`
2. `GameLens_Backend_August_Readiness_Roadmap.md`
3. `GameLens_Packet_4_Pause_And_Resume_Handoff_20260801.md`
4. this controlled replay plan
5. the original August Readiness Plan for historical context only

Never edit the original Plan.

Re-run the existing focused gate:

~~~bash
python -m unittest tests.test_app -v
python -m unittest tests.services.test_gamelens_metric_pipeline_conductor -v
~~~

Expected baseline: 14 focused tests pass.

### Record before stopping

- current local and `origin/dev` commit;
- working-tree changes that belong to Christian;
- current dev Cloud Run revision and URL;
- current dev service account;
- current environment variables;
- current Cloud Run timeout;
- current production and dev trigger states.

### Done when

No code or cloud resource changed, and the starting state is written down.

---

## Chunk R1 — Add fail-closed runtime targeting

### Goal

Create one configuration seam that all replay-aware code uses for season, datasets, tables, and the raw-response bucket.

### Required behavior

The runtime configuration should expose, at minimum:

- active NFL season;
- execution environment/mode;
- League dataset;
- Scores dataset;
- Analytics dataset;
- raw-response bucket;
- optional replay date or game scope;
- a flag or mode that distinguishes daily production work from a controlled replay.

Development mode must fail closed:

- no silent fallback from a missing dev variable to a production destination;
- no writable target named exactly `League`, `Scores`, or `Analytics`;
- no production raw bucket;
- no invalid or blank season.

Production defaults may remain compatible with current behavior, but dev isolation must be explicit.

### Tests first

Add focused tests that prove:

1. dev mode resolves only approved dev datasets;
2. a missing dev target raises an error before any client write;
3. a production dataset in dev mode raises an error;
4. the raw bucket must be development-only;
5. active season `2025` reaches the conductor;
6. existing production defaults are not silently changed by a unit test.

### Likely code touch points

Inspect the complete live versions before editing:

- `config.py`
- `app.py`
- `api_calls/api_call_nfl_games.py`
- `api_calls/api_call_nfl_stats.py`
- `api_calls/api_call_nfl_scores.py`
- `utils/gcs.py`
- `agg/build_metric_facts.py`
- `agg/build_windowed_metrics.py`
- `agg/build_metric_rankings.py`
- query helpers used by those modules

Do not perform broad renames or cleanup.

### Suggested commit boundary

`Add fail-closed dev replay target configuration`

### Done when

All new configuration tests pass locally and no BigQuery/GCS call was made.

---

## Chunk R2 — Scope ingestion to one replay date

### Goal

Make `load_date` an actual selection boundary, not a label passed through the call stack.

### Required behavior

For controlled replay mode:

- Stats selects only games scheduled on `load_date`;
- Scores selects only games scheduled on `load_date`;
- the selected development schedule contains exactly one eligible game;
- an unexpected second game causes a pre-write failure;
- unrelated backlog rows remain untouched;
- Schedule may report an existing pre-seeded game rather than fetching an old season;
- accepted counts describe games, not metric rows.

A date with exactly one completed game is preferred. This avoids adding a public `game_id` contract solely for the rehearsal.

### Tests first

Add focused tests that prove:

1. one replay date selects one expected game;
2. a different backlog date is excluded;
3. two eligible games in controlled-one-game mode fail before writes;
4. zero eligible games is an explicit no-op;
5. Stats failure is not converted into a healthy no-op;
6. Scores failure remains visible;
7. retry/status behavior remains game-scoped.

### Suggested commit boundary

`Scope controlled replay ingestion to one date`

### Done when

Tests demonstrate that a 2025 request cannot drain the full historical backlog.

---

## Chunk R3 — Make the scheduler seam truthful and season-aware

### Goal

The real `POST /` response must be sufficient to judge the rehearsal without guessing from Cloud Logging.

### Required response evidence

The response should identify:

- execution mode;
- active season;
- load date;
- selected game count and ideally selected game ID;
- Schedule status;
- Stats accepted-game count and failures;
- Scores accepted/successful-game count and failures;
- conductor status;
- Facts, Windowed Metrics, and Rankings row counts;
- overall status;
- explicit no-op reason when applicable.

Use `POST /` for the rehearsal. Do not use `GET /test` unless that route is separately changed to return and evaluate the same truthful summary.

HTTP behavior:

- `200`: success or legitimate no-op;
- non-2xx: failure or partial failure that requires inspection.

### Tests first

Extend `tests.test_app` for:

- active season from configuration;
- 2025 conductor call;
- truthful success;
- truthful no-op;
- Stats exception;
- Stats internal rejection/failure;
- Scores failure;
- conductor exception;
- conductor-reported stage failure;
- HTTP status mapping.

### Suggested commit boundary

`Return truthful controlled replay summaries`

### Done when

The focused app and conductor suites pass and the response contains enough evidence to diagnose the run.

---

# Part B — Isolated cloud sandbox

## Chunk R4 — Build the sandbox without calling the app

### Goal

Create the physical safety wall before deploying replay-capable code.

### Steps

1. Confirm BigQuery location compatibility.
2. Create `League_dev`, `Scores_dev`, and `Analytics_dev`.
3. Create frozen baseline clones.
4. Create working clones with the normal table names expected by the configured dev service.
5. Recreate `games_to_process` and `scores_to_process` so every reference points only to dev objects.
6. Create a development raw-response bucket.
7. Create or select a dev-only runtime service account.
8. Grant the minimum read/write permissions on dev resources.
9. Confirm the service account cannot write to production datasets or the production bucket.
10. Apply labels such as `app=gamelens`, `environment=dev`, and `purpose=packet4-replay`.

### Required audit

Search all current code paths used by the run for literal production destinations. Produce a table:

| Code path | Read target | Write target | Dev-configurable? | Test covering it |
| --- | --- | --- | --- | --- |

Do not deploy until every write target is accounted for.

### Done when

The dev resources exist, baseline and working row counts match before rewind, views reference only dev objects, and no app request has been sent.

---

## Chunk R5 — Choose and rewind exactly one completed 2025 game

### Candidate rules

Choose a 2025 date that has:

- exactly one completed scheduled game;
- two teams with valid Stats;
- meaningful Scores rows;
- a successful box-score-loaded/status record;
- existing Facts, Windowed, and Rankings coverage;
- preferably a normal regulation game rather than an unusual edge case for the first proof.

### Capture the baseline before mutation

Record:

- replay date;
- game ID;
- home/away teams;
- schedule row count;
- Stats metric row count for both teams;
- Scores row count and values;
- Stats/Scores status rows;
- derived table counts;
- representative `lens_tags`;
- baseline schemas.

### Rewind only the working clones

In the working development tables:

- remove the selected game's `game_metrics_flat` rows;
- remove the selected game's Scores rows;
- reset/remove only its Stats/box-score status marker;
- reset/remove only its Scores status marker;
- leave the development schedule row in place;
- do not delete unrelated dates;
- do not touch frozen `*_baseline` tables.

Then query the dev views and prove:

- exactly one Stats game is eligible;
- exactly one Scores game is eligible;
- the game ID and date are the intended ones;
- no other date is eligible.

### Recovery

If the rewind is wrong, restore the working clone from its frozen baseline. Never repair this by querying or mutating production from the service.

### Done when

Exactly one intended game is pending in the dev sandbox.

---

# Part C — Real Cloud Run rehearsal

## Chunk R6 — Deploy and preflight dev without ingestion

### Goal

Prove the deployed revision's identity and configuration before allowing writes.

### Deployment requirements

- deploy only `nfl-games-app-dev`;
- use the dev-only service account;
- set active season to `2025`;
- set all three dev datasets;
- set the dev raw-response bucket;
- set controlled replay mode;
- persist a 900-second Cloud Run request timeout;
- keep production Scheduler unchanged;
- do not route production traffic.

### Preflight checks

1. `GET /health` returns 200.
2. Inspect the deployed revision and environment configuration.
3. Confirm the service account identity.
4. Confirm the service URL contains the dev service.
5. Run a read-only configuration/target check that lists resolved destinations without secrets.
6. Confirm the one-game dev backlog remains unchanged.

### Done when

The exact deployed revision is recorded and every resolved writable target is development-only.

---

## Chunk R7 — Run one positive replay

Set shell variables only after copying the verified dev URL and chosen date:

~~~bash
DEV_URL="https://verified-dev-service-url"
REPLAY_DATE="YYYY-MM-DD"
~~~

Call the truthful scheduler seam exactly once:

~~~bash
curl -sS -X POST "$DEV_URL/" -H "Content-Type: application/json" -d "{\"load_date\":\"$REPLAY_DATE\"}"
~~~

Expected high-level scorecard:

~~~text
Service: nfl-games-app-dev
Mode: controlled_replay
Season: 2025
Schedule: success/existing
Stats: exactly 1 accepted game
Scores: exactly 1 successful game
Facts: success
Windowed Metrics: success
Rankings: success
Overall: success
HTTP: 200
~~~

Do not configure an automatic retry. Save the complete JSON response and record start/end time.

If the request fails or times out:

- do not immediately run it again;
- identify the last completed stage;
- query dev tables to determine which writes persisted;
- preserve logs and response;
- use the recovery section below.

### Done when

There is one captured response and a known dev-table state.

---

## Chunk R8 — Verify the rebuilt outputs

A successful HTTP response is necessary but not sufficient.

### Ingestion checks

- exactly one game was accepted by Stats;
- both teams have meaningful Stats rows;
- Scores contains the expected two team rows;
- status records are present and correct;
- no unrelated date changed;
- no duplicate stable keys exist.

### Derived checks

For Facts, Windowed Metrics, and Rankings:

- non-empty output;
- expected schema;
- stable-grain duplicate count is zero;
- total row count matches the frozen baseline or has a documented, explainable difference;
- selected-game/team values match the baseline or have an explainable provider correction;
- no unexpected null expansion;
- `lens_tags` remains `REPEATED STRING`;
- representative populated `lens_tags` remain JSON/list-shaped;
- conductor stage counts match actual table counts.

### Downstream checks

- call `GET /game/<selected_game_id>` against dev;
- final score appears;
- metric/ranking context appears;
- ranking data is pregame-safe;
- `lens_tags` is a JSON array;
- existing route/auth/CORS behavior is unchanged.

### Runtime checks

Record:

- total request duration;
- per-stage durations if available;
- Cloud Run timeout;
- BigQuery job failures/warnings;
- peak memory/instance behavior if visible;
- enough structured log fields to reconstruct the run.

### Done when

The response, dev tables, frozen baselines, and `/game` agree.

---

## Chunk R9 — Prove the same request is safe twice

Call the same `POST /` request a second time only after R8 passes.

Expected behavior:

- no duplicate Stats rows;
- no duplicate Scores rows;
- no duplicate status rows;
- accepted Stats count is zero and the conductor is skipped, **or** a clearly documented deterministic rebuild occurs without duplication;
- overall response is success/no-op, not a hidden failure;
- derived results remain equal to the verified first-run result.

The preferred outcome is an explicit no-op because the status markers now show the game as loaded.

### Done when

The second run is boring: no duplicates, no surprise backlog, no production writes.

---

# Part D — Failure recovery and closure

## 6. Partial-write recovery

The metric conductor is not transactional. Facts may write successfully before Windowed Metrics or Rankings fails.

If a stage fails:

1. Stop.
2. Save the response and logs.
3. Record which stages completed.
4. Compare each working dev table to its frozen baseline.
5. Identify whether the failure was code, schema, permission, timeout, or provider-data related.
6. Restore affected working tables from frozen baselines if a clean retry is needed.
7. Correct the smallest root cause.
8. Re-run local focused tests.
9. Repeat the preflight before another cloud call.
10. Never “just try it again” against an unknown partial state.

Because all writes are development-only, recovery can be deliberate without endangering production.

---

## 7. Acceptance decision

### PASS — Packet 4 machinery proven

Declare the controlled replay passed only if:

- isolation checks passed;
- exactly one 2025 game was processed;
- all three metric stages succeeded;
- rebuilt data passed comparison checks;
- `/game` worked against the dev outputs;
- `lens_tags` stayed intact;
- the second request was idempotent;
- runtime fit within the configured limit;
- failure visibility was sufficient.

This supports moving to a separate production-activation decision.

### CONDITIONAL PASS

Use this only for a small, understood difference such as a provider correction where:

- the difference is documented;
- stable grain and schemas remain correct;
- no production safety rule was violated;
- rerun behavior remains safe.

### FAIL

Any production write, wrong season, unbounded backlog, hidden failure, duplicate, schema break, malformed `lens_tags`, unexplained count difference, or non-reconstructable partial state is a failure.

A failed rehearsal is valuable evidence. It is not permission to weaken the guardrails.

---

## 8. Documentation closure

After the rehearsal:

1. Update the Packet 4 pause/resume handoff with:
   - replay date and game ID;
   - code commits;
   - deployed dev revision;
   - response summary;
   - before/after row counts;
   - duration;
   - idempotency result;
   - known differences or failures.
2. Update the canonical roadmap Packet 4 status.
3. Update the How-To handoff snapshot.
4. Preserve the original August Readiness Plan unchanged.
5. Record whether the next action is:
   - fix and repeat dev replay;
   - rehearse a real 2026 completed game in dev;
   - prepare controlled `dev → main` activation;
   - or pause.

---

## 9. Recommended commit-sized implementation sequence

Keep code work in these small commits:

1. `Add fail-closed dev replay target configuration`
2. `Scope controlled replay ingestion to one date`
3. `Return truthful controlled replay summaries`
4. `Persist dev replay deployment settings`
5. `Document Packet 4 controlled replay result [skip ci]`

Each commit gets its focused tests before the next begins.

Do not combine infrastructure creation, endpoint invocation, production activation, or roadmap completion into a single “Packet 4” action.

---

## 10. The very next step

The next step is **Chunk R0 only**.

After R0, begin R1 by writing the fail-closed runtime-target tests. Do not create BigQuery clones, change Cloud Run, or call Tank01 yet.

That gives us the easiest possible first win:

> Prove locally that dev mode cannot point at production, then stop.
