# GameLens Packet 1 — Pregame Capture Contract

**Status:** Complete and ready for Packet 2 review  
**Completed:** 2026-08-10  
**Branch:** `dev`  
**Production behavior changed:** No  
**Production data written:** No

## The plain-English decision

GameLens gets one trustworthy memory of each regular-season game: the first
valid `/game`-style read saved before kickoff. That read becomes Level 1's
source. After kickoff it is preserved, never rebuilt from newer evidence.

Packet 1 defines and tests that boundary only. It does not capture a live game,
write BigQuery or GCS, run Levels 1–4, or connect anything to `app.py`.

## Gate H and branch baseline

- `dev` and `main` were identical at `77f4a06833402cc5c89ab672b17b0a675419590b`
  before Packet 1 began.
- Gate H's automatic production run fired at the normal 8:00 a.m. Eastern
  schedule, reached Cloud Run at approximately `12:00:08 UTC`, returned HTTP
  `200`, processed the expected 2026-08-06 schedule data, preserved raw GCS
  backups, completed its delete/insert path, and reported `success`.
- Packet 1 did not inspect or mutate live Google Cloud resources. The Gate H
  entry above records the already-reviewed production evidence that allowed
  this sprint to start.

## Before-kickoff admission rule

A production capture is eligible only when all of these are true:

1. The game comes from the existing Schedule lookahead: today plus the next two
   dates.
2. `gameStatus` is `Scheduled`.
3. The capture timestamp is strictly earlier than the scheduled kickoff. A
   capture exactly at kickoff is too late.
4. `seasonType` is regular season. Preseason may rehearse the same contract in
   dev/shadow, but it is recorded as `preseason_shadow_only` and cannot enter a
   production Levels 1–4 cohort.
5. `final_score` and `model_outcome` are empty, and no outcome-bearing field such
   as `actual_winner`, `model_result`, `result_code`, or a final margin is
   populated anywhere in the payload.

Missing rankings do not invalidate a capture. The manifest records
`ranking_context.available = false` and its reason so an honest early-season
read is preserved instead of disguised as a failure.

## Capture timing

The existing daily 8:00 a.m. Eastern Scheduler remains the only daily trigger.
After the existing completed-game ETL and metric conductor finish truthfully,
Packet 2 may inspect the already-loaded today-plus-two-day Schedule window and
capture eligible future games.

The first valid capture is canonical. A later retry with the same deterministic
identity and the same payload hash is a clean no-op. A different payload for an
already-canonical identity is quarantined as `canonical_payload_conflict`; it
does not replace the first read.

If the current metric build fails, capture waits. If there were simply no new
Stats and the prior metric tables remain healthy, capture may still proceed.

## Identities

| Identifier | Contract |
|---|---|
| `pipeline_run_id` | One Scheduler or manual ETL execution; supplied by the existing top-level run |
| `learning_run_id` | Stable season + phase + ruleset cohort, such as `gamelens_2026_regular_season_v1`; never a new value every day |
| `capture_id` | Deterministic hash of `learning_run_id + game_id + scheduled_kickoff`; identical across retries |
| `claim_key` | Deterministic hash of `capture_id + source_field_path + claim_type + claim_rank`; one claim inside one capture |

The future Level 1 storage key is `learning_run_id + capture_id + claim_key`.
Packet 3 must MERGE or replace only the selected game; it must never delete and
replace the cumulative learning cohort.

## Required snapshot and manifest

The immutable snapshot contains the complete pregame payload built by
`services.game_service.get_game_details(...)`. Its companion manifest contains:

- all four identities above;
- game, season, phase, week, status, and scheduled kickoff;
- capture timestamp and `captured`/skipped/quarantined state;
- payload SHA-256;
- metric and ranking source dates;
- ranking availability and plain reason;
- model, ruleset, feature, and formula versions available at capture time;
- environment and storage object identity;
- postgame-field validation result;
- explicit `outcome_write_allowed = false`; and
- skip, quarantine, or error reason when capture does not succeed.

Packet 2 should use the configured environment bucket with a dedicated prefix:

```text
gamelens_learning/<environment>/<learning_run_id>/<game_id>/<capture_id>/
```

The two objects are `payload.json` and `manifest.json`. Creation must be
conditional so an existing canonical object cannot be overwritten. Dev/shadow
and production therefore use the same object shape without sharing evidence.

## Read-only `/game` behavior

The payload must be built in-process through `services.game_service`; the
learning conductor must not call the public HTTP route or rebuild GameLens
logic.

Current `get_game_details(...)` saves Model Outcome rows only for final games,
while this contract admits only scheduled games. Packet 2 should still make the
read-only intent explicit—prefer a default-preserving `persist_outcome=False`
or equivalent service option—rather than rely on status as the only protection.
That narrow change belongs to Packet 2 and must keep the existing `/game`
behavior unchanged.

`model_trust` is not on the denylist. It contains the legitimate Pregame Model
Read and reasoning. Only its later outcome-bearing evidence is prohibited.

## The two postgame gates

| Gate | Required evidence | Ready result | Missing result |
|---|---|---|---|
| Model Outcome and Trust grading | Valid canonical capture + final score | Grade the frozen matchup read | `capture_missing` or `final_score_missing`; show final score if available but do not invent a grade |
| Levels 2–3 | Valid canonical capture + final score + accepted completed-game Facts | Level 2 validates claims, then Level 3 adds pregame-safe context | `accepted_facts_missing`; defer both Levels without rebuilding Level 1 |

Facts without a final score do not make either gate ready. A final score without
accepted Facts may grade the game, but claim validation waits. Level 4 remains a
separate weekly/evidence-threshold batch.

## DRY responsibility map

| Need | Reuse | Packet 1 decision |
|---|---|---|
| Schedule refresh and today-plus-two lookahead | `api_calls/api_call_nfl_games.py::fetch_nfl_games(...)` and the canonical Schedule table | Reuse its date boundary; add only a thin read selector for eligible stored rows |
| Pregame-safe metrics/rankings | `queries/game_queries.py` and the existing Facts → Windowed Metrics → Rankings tables | Keep the existing `data_date < game_date` rule; no new feature builder |
| Complete pregame product read | `services/game_service.py::get_game_details(...)` | One canonical builder, called in-process and explicitly read-only |
| Level 1 claim rows | `agg/gamelens_training/build_claim_training_examples.py::extract_claim_rows(...)` | Adapt the saved capture context in Packet 3; no second extractor |
| Final score | `queries/game_queries.py::get_final_score(...)` | Reuse for the game-grade readiness gate |
| Completed-game Facts and Level 2 comparison | `agg/gamelens_training/update_claim_training_validation.py` | Reuse its facts loading and validation workers after they become callable |
| Metric meaning and `lens_tags` | `analytics/metric_registry.py` | Never copy definitions into the contract or conductor |
| Metric orchestration | `services/gamelens_metric_pipeline_conductor.py` | Read its truthful success/failure summary; do not duplicate its builders |
| Outcome/Trust calculation | Current `build_model_outcome(...)`, `build_model_trust(...)`, and persistence path | Packet 4 grades the frozen read and reconciles older rows; Packet 1 writes nothing |
| Top-level coordination | Future `services/gamelens_learning_pipeline_conductor.py` | Select, gate, call, and summarize only; no football formulas |

## Packet 1 code boundary

`services/gamelens_learning_contract.py` is deliberately pure Python. It owns:

- timing/status/phase eligibility;
- postgame-field rejection;
- stable identity generation;
- manifest shape and payload hash;
- first-canonical/no-op/conflict decisions;
- clean no-game summaries; and
- the two postgame readiness gates.

It imports neither Flask nor Google Cloud and performs no writes. Packet 2 can
call it around the existing service instead of spreading these rules across
`app.py`, storage code, and Level workers.

## Evidence

```text
python -m compileall services/gamelens_learning_contract.py \
  tests/services/test_gamelens_learning_contract.py

python -m unittest tests.services.test_gamelens_learning_contract -v
```

Result: 14 focused tests passed. Covered:

- scheduled-before-kickoff acceptance;
- in-progress/final and exactly-at-kickoff rejection;
- production preseason rejection with dev/shadow admission;
- populated final/outcome-field rejection;
- legitimate empty outcome fields;
- missing ranking context;
- deterministic capture and claim identities;
- first-canonical, duplicate no-op, and payload-conflict quarantine;
- clean no-game no-op; and
- separate outcome-grade and Levels 2–3 readiness gates.

## Stop/go decision

Packet 1 is **GO**. No production behavior or data changed. `app.py`, `/game`,
Calibrated Matchup Lean, Level workers, Scheduler, BigQuery, and GCS are
unchanged.

The next bounded action is Packet 2 only: implement and test shadow pregame
capture around this contract, use isolated storage, call the canonical game
service read-only, and stop after one deterministic replay proves there is still
exactly one canonical snapshot.
