# GameLens Packet 4 Pause and Resume Handoff

**Prepared:** 2026-08-01  
**Repository:** `csells10/meow`  
**Working branch:** `dev`  
**Packet:** Packet 4 — daily ETL / metric-pipeline activation wiring  
**Current classification:** **LOCAL VALIDATION COMPLETE — PRODUCTION ACTIVATION BLOCKED**

---

## 1. The one-paragraph handoff

Packet 4 has been implemented on `dev` and its focused local behavior is healthy. The daily lifecycle preserves `Schedule → Stats → Scores`, uses the number of accepted Stats games as the gate, produces a successful explicit no-op when Stats accepts zero games, and calls the 2026 GameLens metric conductor with writes enabled when one or more games are accepted. Fourteen focused tests passed: seven application/orchestration integration tests and seven conductor tests. Existing Flask routes remain registered. Nothing from this session deployed to production, enabled the `dev` trigger, changed Scheduler or Cloud Run timeouts, merged `dev` into `main`, or manually invoked `POST /`. The remaining risk is the first **real accepted 2026 completed-game payload** in the cloud: BigQuery permissions, schemas, full season-to-date replacement writes, runtime, timeout behavior, and `/game` after real data have not yet been proven together. Resume with a single controlled `dev` rehearsal only after a completed 2026 game exists and only after explicitly reviewing the tables that can be replaced.

---

## 2. Status at the pause point

| Area | Status | What that means |
| --- | --- | --- |
| Packet 4 code on `dev` | Implemented | Live `dev` code contains the accepted-Stats gate and 2026 conductor wiring. |
| Focused app tests | **7/7 passed** | Success, no-op, component failures, conductor failures, and HTTP truthfulness are covered locally. |
| Focused conductor tests | **7/7 passed** | Facts, Windowed Metrics, and Rankings ordering and stop-on-failure behavior are covered locally. |
| Route registration | Passed | `/`, `/admin/gamelens/claim-health`, `/game`, `/games`, `/health`, `/me`, `/static`, and `/test` remain registered. |
| Container rehearsal | Not run | Docker Desktop was not installed. This was an extra check, not a stated Packet 4 acceptance requirement. |
| Real 2026 positive cloud path | **Not proven** | No completed 2026 game was available for the controlled rehearsal at the time of validation. |
| `dev` deployment trigger | Disabled | Keep it disabled until a controlled rehearsal is explicitly approved. |
| Production deployment | Not performed | Production has not received the Packet 4 activation wiring from this work. |
| Packet 4 production readiness | **Blocked** | Do not describe Packet 4 as production-ready until the controlled live-data rehearsal passes. |
| Original August Readiness Plan | Preserved | Historical intent only; never edit it. |

### Recommended roadmap wording

Use this exact status until the cloud rehearsal is complete:

> **Packet 4 — local validation complete; production activation blocked pending one controlled 2026 completed-game cloud rehearsal.**

Do **not** mark Packet 4 fully complete or production-ready yet.

---

## 3. Source-of-truth hierarchy

When resuming, use evidence in this order:

1. **Live `dev` branch in GitHub** — authoritative for current code and active documentation.
2. **Fresh local `dev` checkout and terminal output** — authoritative for the developer workstation state.
3. **This handoff** — authoritative for the pause point, tests already run, decisions, risks, and restart procedure.
4. **Uploaded source snapshots** — architecture references only; some are stale.
5. **Original `GameLens_Backend_August_Readiness_Plan.md`** — historical intent only; read but never edit.

### Critical stale-file warning

The uploaded `app.py` snapshot still contains the old hard-coded call:

```python
from agg.aggregate_nfl_metrics_2025 import run_aggregate_for_season
run_aggregate_for_season("2025")
```

That snapshot predates the Packet 4 implementation and must **not** be used to redesign or roll back the live `dev` implementation. The terminal results from the live checkout prove that current `dev` imports and exercises `run_gamelens_metric_pipeline`.

### Files that failed to transfer into this documentation workspace

These four attachments were unavailable and were not inspected here:

- `api_call_nfl_stats.py`
- `api_call_nfl_scores.py`
- `api_call_nfl_games.py`
- `config.py`

Future code-level work involving the ingestion boundary must inspect these files from live `dev` or re-upload them. Do not infer their current implementation from the stale attachments.

---

## 4. Last verified Git state

The most recent terminal evidence in this session showed:

```text
## dev...origin/dev
 M documentation/stand_up/202607/GameLens_Stand_Up_20260730.md
?? documentation/stand_up/202607/GameLens_Stand_Up_20260731.md
7a063b8 (HEAD -> dev, origin/dev) Add dev-only Cloud Build deployment
```

Interpretation at that moment:

- Local `dev` and `origin/dev` pointed to the same commit: `7a063b8`.
- The branch was synchronized.
- Two stand-up files contained user work and must be preserved:
  - modified: `documentation/stand_up/202607/GameLens_Stand_Up_20260730.md`
  - untracked: `documentation/stand_up/202607/GameLens_Stand_Up_20260731.md`
- Do not clean, delete, restore, overwrite, or silently stash those files.

Earlier Packet 4 handoff milestones expected in `dev` history include:

- `0fb649b` — Finalize August Packet 4 handoff
- `95f7bd3` — Record Packet 4 deployment preflight
- `2d95b4e` — Align Cloud Run image with Python 3.11

Re-verify all commit IDs after returning; this document records a pause-time snapshot, not a permanent branch pointer.

---

## 5. Packet 4 intended behavior

The desired daily flow is:

```text
Cloud Scheduler
  → POST /
  → setup_schedules(load_date)
  → run_api_calls(load_date)
  → Schedule loader
  → Stats loader
  → Scores loader
  → accepted Stats count gate
      ├─ 0 accepted games
      │    → explicit successful no-op
      │    → conductor skipped
      └─ 1+ accepted games
           → run_gamelens_metric_pipeline(
                 season="2026",
                 if_exists="replace",
                 write=True
             )
           → Facts
           → Windowed Metrics
           → Rankings
           → truthful execution summary / HTTP status
```

### Why Stats is the gate

The schedule supplies the identity and matchup backbone, but new metric output should be rebuilt only when the Stats loader successfully accepts at least one completed game. Scores can still fail independently and must remain visible in the final summary. The metric pipeline gate is therefore the **accepted Stats game count**, not merely whether the scheduler ran or whether the date changed.

### No-op behavior

Before a game is final—or whenever Stats accepts zero games—the correct outcome is:

- overall status: `no_op`
- metric pipeline status: `skipped`
- reason: `no_accepted_stats_games`
- conductor: not called
- result: successful, because there was legitimately nothing new to process

### Positive behavior

When Stats accepts one or more games:

- active season is `2026`
- conductor writes are enabled
- stages execute in this order:
  1. Facts
  2. Windowed Metrics
  3. Rankings
- the execution summary must report completed and failed components truthfully
- conductor exceptions and reported stage failures must not be converted into false HTTP 200 success

---

## 6. What the local tests proved

### App/orchestration suite

Command that worked:

```bash
python -m unittest tests.test_app -v
```

Result:

```text
Ran 7 tests in 0.032s
OK
```

Passing tests:

1. `test_conductor_exception_is_not_success`
2. `test_conductor_reported_failure_is_not_success`
3. `test_positive_accepted_stats_runs_2026_conductor_with_writes`
4. `test_scheduler_route_returns_summary_and_truthful_http_status`
5. `test_scores_exception_remains_visible_when_pipeline_succeeds`
6. `test_stats_exception_is_visible_and_skips_conductor`
7. `test_zero_accepted_stats_is_successful_no_op`

This proves locally, with mocks:

- accepted Stats cause the 2026 conductor to run with writes enabled
- zero accepted games are a successful explicit no-op
- a Stats exception stays visible and skips the conductor
- a Scores exception stays visible even if the conductor succeeds
- a conductor exception is not reported as success
- a conductor-reported failure is not reported as success
- the scheduler route maps summaries to truthful HTTP status behavior

The `ERROR` log lines printed during these tests were expected negative-path simulations. The test result `ok` is the important signal.

### Conductor suite

Command that worked:

```bash
python -m unittest tests.services.test_gamelens_metric_pipeline_conductor -v
```

Result:

```text
Ran 7 tests in 0.043s
OK
```

Passing tests:

1. `test_empty_facts_result_stops_pipeline`
2. `test_facts_exception_skips_later_stages`
3. `test_non_dataframe_result_stops_pipeline`
4. `test_rankings_failure_preserves_completed_stages`
5. `test_requires_nonblank_explicit_season`
6. `test_success_calls_builders_in_order_and_returns_counts`
7. `test_windowed_failure_preserves_facts_and_skips_rankings`

This proves locally, with mocks:

- stage order is Facts → Windowed Metrics → Rankings
- a blank season is rejected
- an exception in Facts prevents later stages
- empty or invalid Facts output stops the pipeline
- a Windowed failure leaves Facts reported as completed and skips Rankings
- a Rankings failure leaves prior completed stages visible
- a successful run returns stage row counts

### Route inventory

Command used:

```bash
python -c "import app as m; [print('{} -> {}'.format(r.rule, ','.join(sorted(r.methods - {'HEAD','OPTIONS'})))) for r in sorted(m.app.url_map.iter_rules(), key=lambda x: x.rule)]"
```

Verified routes:

```text
/ -> POST
/admin/gamelens/claim-health -> GET
/game/<path:game_id> -> GET
/games -> GET
/health -> GET
/me -> GET
/static/<path:filename> -> GET
/test -> GET
```

This only verified Flask registration. It did not call the routes or prove their cloud dependencies.

---

## 7. The test-discovery trap that cost time

Do **not** use this command for `test_app.py`:

```bash
python -m unittest discover -s tests -p "test_app.py" -v
```

Why it fails:

- `unittest discover -s tests` places `tests` early in Python's import search path.
- `tests/api_calls` shadows the real root-level `api_calls` namespace package.
- `config.py` then raises a misleading error such as:

```text
ModuleNotFoundError: No module named 'api_calls.api_call_nfl_games'
```

The files were not missing. The working directory and environment were healthy.

Direct imports proved all three loaders were available:

```text
IMPORTED: ...\api_calls\api_call_nfl_games.py
IMPORTED: ...\api_calls\api_call_nfl_scores.py
IMPORTED: ...\api_calls\api_call_nfl_stats.py
```

Always use package-style commands from the repository root:

```bash
python -m unittest tests.test_app -v
python -m unittest tests.services.test_gamelens_metric_pipeline_conductor -v
```

Verified Python environment at the time:

```text
C:\Users\csell\OneDrive\Desktop\Projects\Meow\meow\nfl\Scripts\python.exe
```

---

## 8. Container check: unavailable, not failed

Attempted command:

```bash
docker build -t gamelens-packet4-local .
```

Result:

```text
bash: docker: command not found
```

A standard-location check reported:

```text
Docker Desktop is not installed in the standard location
```

Interpretation:

- no image build started
- no application code failed
- no deployment occurred
- Docker installation is not required merely to close the local test evidence
- Python 3.11 alignment had already been committed to `dev` in the Packet 4 preparation work

Do not install Docker solely because this optional check was unavailable. If a future activation plan requires a local container rehearsal, make that a separate explicit environment decision.

---

## 9. Data-pipeline architecture and write semantics

### Sources and targets

```text
League.schedule
    +
Analytics.game_metrics_flat
    ↓
Analytics.game_team_metric_facts_2026
    ↓
Analytics.team_metrics_windowed_2026
    ↓
Analytics.team_metric_rankings_2026
    ↓
query/service layer
    ↓
GET /game/<game_id>
```

### Builders are full season-to-date rebuilds

The three builders are intentionally deterministic full-season rebuilds, not yesterday-only incremental transforms. When one new final game arrives, historical-to-current windows and league rankings may change, so rebuilding the 2026 season keeps all downstream rows internally consistent.

The current activation path uses `if_exists="replace"`. Therefore a positive rehearsal can replace the contents of the three 2026 derived tables. This is expected design, but it is also why the first write-enabled rehearsal requires an explicit pre-write review.

### Stage behavior

| Stage | Input | Output | Important validation |
| --- | --- | --- | --- |
| Facts | `Analytics.game_metrics_flat` + `League.schedule` | `Analytics.game_team_metric_facts_2026` | Final games only, registered metrics, stable grain, required fields, deduplication |
| Windowed Metrics | Facts | `Analytics.team_metrics_windowed_2026` | Phase-aware windows, derived-rate recalculation, list-valued `lens_tags`, stable grain |
| Rankings | Windowed Metrics | `Analytics.team_metric_rankings_2026` | League rank/percentile/tier metadata for the query layer |

### Important: this pipeline is not transactional

If Facts writes successfully and Windowed Metrics or Rankings then fails:

- the completed Facts write remains
- later stages are skipped or reported failed
- the execution summary exposes the partial state
- there is no automatic rollback of the earlier table

The tests prove honest reporting and safe stage stopping. They do **not** prove atomic all-or-nothing writes.

Because the builders are deterministic full season-to-date rebuilds, a carefully verified rerun after fixing the cause is the likely recovery mechanism. Do not rerun blindly; first identify exactly which stage wrote and why the next stage failed.

---

## 10. Protected contracts that must survive future work

### `lens_tags`

`lens_tags` is a protected forward-looking contract:

```text
metric_registry.py
  → Windowed Metrics
  → Rankings
  → query/service layer
  → /game JSON
```

Rules:

- Python representation: `list[str]`
- BigQuery representation: `REPEATED STRING`
- `/game` representation: JSON array
- never flatten, stringify, rename, or silently discard tags
- the Windowed builder validates that tag values are lists
- the query/service layer falls back to `[]`, not a string

### Levels 1–4

- Preserve all existing Levels 1–4 code and historical behavior.
- Do not make Levels 1–4 a prerequisite for `/game` readiness.
- Do not activate Levels 1–4 as part of Packet 4.
- Do not change claim formulas, calibration logic, or learning thresholds during activation work.

### Claim-learning evidence

- Frontend game selection is not claim-learning evidence.
- Do not convert a user's selected game into a validation label or implicit claim vote.
- Preserve historical QA runs and `run_id` values.
- Never replace or reuse historical QA `run_id` values as part of a rebuild.

### Existing API/auth behavior

- Preserve `/game`, `/games`, `/me`, Claim Health, auth, and CORS behavior.
- `/game` must degrade gracefully when ranking rows are unavailable.
- Ranking queries should remain pregame-safe by using an `as_of_date` strictly before the target game's date.

---

## 11. Why August 1 is not the cliff

The calendar changing to August does not activate Packet 4 by itself.

| Event | Expected effect |
| --- | --- |
| August 1 arrives | No special code path activates merely because the month changed. |
| Daily production Scheduler continues | It calls the currently deployed production revision, not un-deployed `dev` code. |
| A game is scheduled but not final | Stats should accept zero completed games; the new path would be a tested no-op if running. |
| First completed 2026 game is accepted by Stats | This is the meaningful hinge: the positive metric pipeline path can run. |

At the pause point, the Packet 4 wiring remained on `dev`, the `dev` trigger remained disabled, and production had not been redeployed with it. Therefore the first game cannot automatically unleash the new conductor in production unless someone deliberately changes that deployment state.

The concern is still valid: the first accepted completed-game payload is where real schemas, writes, runtime, and downstream behavior finally meet. That event should be treated as a controlled rehearsal, not a casual scheduler test.

---

## 12. What remains unproven

The following must remain open risks until tested in the cloud with real 2026 completed-game data:

1. **Real Stats acceptance:** the actual 2026 box-score payload is accepted and counted correctly.
2. **BigQuery permissions:** the running `dev` service account can read all sources and replace all three 2026 target tables.
3. **Schema compatibility:** real output matches the existing explicit schemas, especially repeated `lens_tags`.
4. **Non-empty output:** Facts, Windowed Metrics, and Rankings all return non-empty DataFrames for real 2026 data.
5. **Full rebuild duration:** the season-to-date rebuild completes inside the active Cloud Run and Scheduler limits.
6. **Truthful cloud response:** a real cloud failure produces the expected non-success summary/status.
7. **Partial-state behavior:** operational handling is clear if Facts writes but a later stage fails.
8. **Downstream `/game`:** a real 2026 game returns ranking context and matchup output without breaking legacy/degraded behavior.
9. **Operational logs:** the execution can be reconstructed from structured logs without ambiguity.
10. **Production activation:** deployment and timeout changes have not been rehearsed or approved.

Known pre-activation infrastructure context from the Packet 4 handoff:

- Cloud Run timeout: `300s`
- production Scheduler attempt deadline: `180s`
- Scheduler automatic retries: none
- eventual controlled target discussed for both: `900s`

Do not change these during a casual test. Timeout changes belong to the deliberate activation step after the real `dev` runtime is measured.

---

## 13. Resume procedure — exact order

### Phase A: re-establish the local/GitHub truth

From the repository root in Git Bash:

```bash
git switch dev
git status --short --branch
git fetch origin
git log -5 --oneline --decorate
git log -1 --oneline origin/dev
```

Stop and inspect the output before pulling.

Rules:

- Preserve the two stand-up files listed in Section 4.
- Do not use `git reset --hard`, `git checkout --`, `git clean`, or an automatic stash.
- If local `dev` is behind `origin/dev` and no upstream change conflicts with the stand-up files, use only:

```bash
git pull --ff-only origin dev
```

- If the branch diverged, stop. Do not improvise a merge or rebase.

### Phase B: read the active documentation in order

1. `documentation/August/GameLens_August_Readiness_Roadmap_How_To.md`
2. `documentation/August/GameLens_Backend_August_Readiness_Roadmap.md`
3. this pause/resume handoff
4. `documentation/August/GameLens_Backend_August_Readiness_Plan.md` for historical context only

Never edit item 4.

### Phase C: rerun only the focused local gate

```bash
python -m unittest tests.test_app -v
python -m unittest tests.services.test_gamelens_metric_pipeline_conductor -v
```

Expected baseline: 14 focused tests pass.

Do not use `unittest discover -s tests` for `test_app.py`; see Section 7.

Then re-inventory routes:

```bash
python -c "import app as m; [print('{} -> {}'.format(r.rule, ','.join(sorted(r.methods - {'HEAD','OPTIONS'})))) for r in sorted(m.app.url_map.iter_rules(), key=lambda x: x.rule)]"
```

### Phase D: confirm the cloud safety pins read-only

Before any deployment or request:

- confirm the `dev` Cloud Build trigger is still disabled
- confirm only `main` can deploy production
- identify the exact `dev` Cloud Run service and revision
- record its service account and timeout
- confirm the production Scheduler target, deadline, and retry policy
- confirm production traffic/revision independently

Use live repository configuration and `gcloud ... describe` commands to derive names. Do not invent service or trigger names from memory.

### Phase E: establish whether a rehearsal is now meaningful

Proceed only when a 2026 game is actually final and its Stats payload is available.

Before a write-enabled call:

1. identify the exact `load_date`
2. confirm the final game(s) expected on that date
3. snapshot row counts and schemas for:
   - `Analytics.game_metrics_flat`
   - `Analytics.game_team_metric_facts_2026`
   - `Analytics.team_metrics_windowed_2026`
   - `Analytics.team_metric_rankings_2026`
4. confirm `lens_tags` is `REPEATED STRING` in both downstream schemas
5. explain that the three target tables can be replaced by the full 2026 season-to-date rebuild
6. obtain explicit approval before the write-enabled request

### Phase F: one controlled `dev` rehearsal

Only after the pre-write evidence and explicit approval:

- deploy/prepare only the isolated `dev` service according to the active roadmap
- keep production untouched
- send one targeted request to the exact `dev` `POST /` endpoint with the approved `load_date`
- do not enable a recurring trigger
- capture the entire response body, HTTP status, revision, and structured logs
- do not issue a second request merely because the first seems slow

Expected positive summary:

- Stats accepted count is greater than zero
- pipeline season is `2026`
- write is `true`
- Facts completed with a positive row count
- Windowed Metrics completed with a positive row count
- Rankings completed with a positive row count
- overall status is success

### Phase G: verify consequences before doing anything else

After the single call:

1. compare before/after source and target row counts
2. verify each table's latest timestamps/data dates
3. verify `lens_tags` remains repeated/list-valued
4. query for duplicate grains and unexpected nulls
5. call `/game/<game_id>` on `dev` with proper auth
6. verify ranking context, matchup breakdown, legacy fields, and degraded behavior
7. check that historical tables and Levels 1–4 artifacts were untouched
8. write the exact result into the active Roadmap and How-To

Only after this evidence passes should production deployment and timeout changes become a separate decision.

---

## 14. Stop conditions during the controlled rehearsal

Stop immediately and do not rerun if any of the following occurs:

- Stats reports an exception or an ambiguous accepted count
- a target table/schema is missing or differs from the expected 2026 schema
- `lens_tags` is no longer repeated/list-valued
- Facts returns empty or a non-DataFrame result
- Windowed Metrics or Rankings fails after an earlier table has written
- the HTTP response says success while a component says failure
- the request exceeds an active timeout or the client loses the response
- `/game` fails, drops protected fields, or stops degrading gracefully
- any command points at the production service or production Scheduler unexpectedly
- local `dev` is not synchronized with the reviewed GitHub commit

After a partial write, record:

- revision and commit
- request payload and `load_date`
- HTTP status and response summary
- completed stage(s)
- failed stage and error type/message
- target row counts and timestamps
- whether production remained untouched

Then diagnose before considering a deterministic full rebuild rerun.

---

## 15. Actions explicitly not authorized at this pause point

- Do not deploy production.
- Do not merge `dev` into `main`.
- Do not enable the `dev` trigger.
- Do not trigger production `POST /`.
- Do not change Cloud Run or Scheduler timeouts yet.
- Do not enable automatic retries.
- Do not run a write-enabled 2026 rebuild without the pre-write explanation and explicit approval.
- Do not edit `GameLens_Backend_August_Readiness_Plan.md`.
- Do not activate or redesign Levels 1–4.
- Do not alter metric formulas, windows, ranking logic, claim calibration, auth, or CORS.
- Do not overwrite or discard the two stand-up files.
- Do not trust the stale uploaded `app.py` over live `dev`.

---

## 16. Documentation closure still owed

No further Packet 4 code change is required based on the local evidence. Documentation should be updated only after reconciling it with live `dev`:

- Active Roadmap: record **local validation complete / activation blocked** and the 14/14 focused test evidence.
- Active How-To: record the working package-style test commands, the `tests/api_calls` shadowing trap, Docker unavailability, and the first-game rehearsal gate.
- Original Plan: do not edit.
- Stand-up notes: preserve the existing user edits and add the pause point only if Christian chooses to commit them.

The current handoff is deliberately a separate artifact so the historical Plan remains untouched.

---

## 17. Ready-to-paste prompt for the return chat

```text
We are resuming Packet 4 of the GameLens August Backend Readiness work in GitHub repository csells10/meow.

Use the live dev branch as the sole code source of truth. Begin read-only. Do not rely on stale uploaded code when GitHub can verify the current implementation.

Read these documents in order:

1. documentation/August/GameLens_August_Readiness_Roadmap_How_To.md
2. documentation/August/GameLens_Backend_August_Readiness_Roadmap.md
3. GameLens_Packet_4_Pause_And_Resume_Handoff_20260801.md
4. documentation/August/GameLens_Backend_August_Readiness_Plan.md — historical context only; NEVER edit it

Pause-point status:

- Packet 4 implementation is on dev.
- Local validation is complete: 7/7 tests.test_app and 7/7 tests.services.test_gamelens_metric_pipeline_conductor passed.
- Existing Flask routes remained registered.
- The correct test commands are package-style; do not use unittest discover -s tests for test_app.py because tests/api_calls shadows the real api_calls package.
- Docker Desktop was not installed, so the optional container build was not run.
- Production was not deployed or changed.
- The dev trigger remained disabled.
- POST / was not manually triggered.
- The real 2026 positive cloud path remains unproven.
- Status must remain: LOCAL VALIDATION COMPLETE; PRODUCTION ACTIVATION BLOCKED.
- The first accepted completed 2026 Stats payload—not August 1 itself—is the meaningful risk point.

Critical behavior to preserve:

Schedule → Stats → Scores → accepted Stats count gate.
Zero accepted Stats games = successful explicit no-op and conductor skipped.
One or more accepted Stats games = season 2026 conductor with write=True, running Facts → Windowed Metrics → Rankings and returning truthful status.

Safety constraints:

- Do not deploy production, merge to main, enable triggers, change timeouts, or call POST / until we review the evidence and I explicitly approve the step.
- Preserve lens_tags as REPEATED STRING in BigQuery, list[str] in Python, and a JSON array in /game.
- Preserve Levels 1–4, historical run_ids, auth/CORS, and existing routes.
- Do not treat frontend game selection as claim-learning evidence.
- Preserve my local stand-up edits:
  M documentation/stand_up/202607/GameLens_Stand_Up_20260730.md
  ?? documentation/stand_up/202607/GameLens_Stand_Up_20260731.md
- Do not use destructive Git commands or silently stash my work.

Start by checking live dev and reporting:

1. current HEAD and whether local dev can fast-forward safely,
2. whether the Packet 4 code and focused tests still match this handoff,
3. whether a completed 2026 game now exists and makes a rehearsal meaningful,
4. the current dev/production trigger, revision, timeout, and Scheduler safety state,
5. the exact read-only BigQuery preflight needed before any write,
6. the smallest controlled dev-only rehearsal plan.

Do not change code or cloud state in the first response. Give me explicit Git Bash commands one small step at a time.
```

---

## 18. Final memory anchor

The pipeline is not waiting for August 1. It is waiting for the first real completed game that Stats accepts.

At the pause point:

- the code path is locally covered
- the no-op path is covered
- the failure paths are covered
- production is protected by non-deployment and disabled `dev` automation
- the real cloud positive path is the remaining test

Return at the controlled-rehearsal step. Do not restart Packet 4 from scratch, and do not let a future chat mistake “14 tests passed” for “production has been proven.”
