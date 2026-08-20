# GameLens Packet 5 — Admin and Run Visibility

**Status:** Checkpoint 2 accepted; protected route test gate passed; visual HTTP review next
**Created:** 2026-08-19  
**Branch:** `dev`  
**Predecessor:** [Packet 4 — Postgame Outcome plus Levels 2–3](./GameLens_Packet_4_Postgame_Learning.md)  
**Sprint authority:** [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md)  
**Development schema authority:** [GameLens Development Dataset Recreation Runbook](./GameLens_Development_Dataset_Recreation_Runbook.md)  
**Production behavior changed:** No  
**Production learning data written:** No  
**Packet 5 code authorized by this document:** Accepted read-only inventory,
hierarchical backend service, and protected route registration; bounded week
grouping may proceed as the next visual slice; no frontend yet

---

## The short version

Packets 1–4 already store the necessary development truth. Packet 5 should make
that truth understandable through the protected Admin boundary without copying
it into another warehouse.

The current Admin endpoint is useful historical aggregate reporting, but its
queries are hardwired to the historical
`Analytics.gamelens_claim_training_examples` table. Current Packet 1–4
development evidence is distributed intentionally across six canonical
`GameLens_dev` tables with different grains.

Packet 5 therefore has two jobs:

1. reconcile aggregate claim health and game calibration from the canonical
   claim and grade owners; and
2. expose bounded operational run visibility from the existing receipt owners
   so an operator can identify the game, stage, input version, failure, and safe
   retry boundary.

The code inspection proves an **Admin/query-boundary gap**, not a storage gap.
Packet 5 will not create an Admin summary table, pipeline-run warehouse,
duplicate claim table, duplicate outcome table, or refresh job.

## Review result before implementation

The required documentation, current Admin code, table schemas, and Packet 4
coordinator were inspected on `dev`.

The key result is:

> Reuse all six tables. Add read-only query/service boundaries. Do not add
> storage in Packet 5 unless a later read-only proof demonstrates a question
> that the existing grains cannot answer.

Christian reviewed and approved the Game Journey product direction on
2026-08-19. This plan now authorizes only Slice 1's read-only backend inventory
and sample response. It does not authorize a route, frontend work, a new table,
pipeline changes, or production learning.

---

## 1. What already exists

### Protected Admin surface

The current application registers:

```text
GET /admin/gamelens/claim-health
```

The route:

- is protected by `require_admin_auth`;
- requires `run_id`;
- accepts `season` and `grain`;
- returns aggregate sections only;
- intentionally returns no game-level drilldown rows; and
- is registered in `app.py`.

The service already separates:

- Game Calibration; and
- Claim Health.

That separation is correct and must remain.

### Current Admin source contract

`queries/admin_claim_health_queries.py` currently binds directly to:

```text
nfl-stream-406420.Analytics.gamelens_claim_training_examples
nfl-stream-406420.League.schedule
```

Most claim matrices filter only by the historical `run_id`. Schedule-based
coverage and trend queries separately exclude preseason from `gameWeek`.
That implementation supported the historical 2025 pilot, but it is not yet a
safe source selector for the stable Packet 1–4 `learning_run_id` cohorts.

Current game-calibration sections derive one game row from claim rows with
`ANY_VALUE(model_result)`. That cannot represent a valid game grade when a
canonical capture produces zero claims. The new capture-aware
`GameLens_dev.game_model_outcomes` table now owns that answer.

The existing calibrated Admin preview also recalculates the old High-to-Medium
research rule. Calibrated Matchup Lean has since been released through one
shared `/game` helper. Normal current-cohort Admin reporting must use the
stored effective label. The old preview may remain explicitly historical and
research-only; Packet 5 must not reimplement it for 2026 evidence.

---

## 2. Six-table reconciliation

Each table answers a different question. Similar-looking receipt tables are not
duplicates merely because all of them contain statuses.

| Table | Canonical grain / identity | Question it answers | Packet 5 use |
|---|---|---|---|
| `pregame_snapshots` | One immutable `capture_id`; canonical selection follows game/cohort rules | What did GameLens know before kickoff? | Capture coverage, timing, cohort/phase, hash, and source version |
| `stage_runs` | One Packet 2 attempt-stage row per `attempt_id + stage_name` | What did the snapshot coordinator attempt overall? | Attempt-level capture status and totals |
| `stage_game_results` | One Packet 2 result per `attempt_id + stage_name + game_id` | Which game captured, skipped, waited, failed, or produced a Level 1 zero? | Per-game capture/Level 1 visibility and zero-claim proof |
| `claim_training_examples` | One claim per `learning_run_id + claim_key`, linked to `capture_id` | Which claims existed, how Level 2 labeled them, and what Level 3 context was attached? | Claim health, validation, feature health, and claim lineage |
| `game_model_outcomes` | One frozen grade per `learning_run_id + capture_id` | How did the frozen matchup read grade at game level? | Game Calibration and Model Trust, separate from claims |
| `postgame_learning_stage_receipts` | One immutable Packet 4 receipt per `attempt_id + receipt_scope + game_id + stage_name` | Which grade/Level 2/Level 3 stage ran, failed, skipped, retried, or changed rows? | Detailed postgame operational visibility and retry diagnosis |

### Receipt overlap is intentional

`stage_runs` and `stage_game_results` are the Packet 2/3 capture and Level 1
operational pattern. `postgame_learning_stage_receipts` stores the richer
Packet 4 grade/Level 2/Level 3 failure and reconciliation contract.

Packet 4 already proved that extending the exact Packet 2 schemas would break
their verified contract. Packet 5 must read both eras as separate canonical
sources. It must not merge, rewrite, consolidate, or backfill them merely to
make querying feel uniform.

---

## 3. Canonical joins and cohort rules

The Admin read path must preserve these identities:

```text
pipeline_run_id
  -> learning_run_id
    -> capture_id + source payload hash
      -> game_id
        -> claim_key for claim-level evidence
```

Rules:

- `learning_run_id` is the stable season/phase/ruleset cohort.
- `run_id` in the current development claim table is a compatibility alias
  and must equal `learning_run_id`.
- `attempt_id` is operational; it must not replace the stable learning cohort.
- `capture_id` owns the frozen prediction lineage.
- `claim_key` owns one claim inside that capture.
- `season_type` is the phase boundary. Do not infer production eligibility
  only from `game_week`.
- preseason development evidence must never appear in a regular-season Admin
  cohort or weekly learning report.
- a missing capture remains `capture_missing`; it cannot be repaired through
  an Admin join.
- a true zero claim count remains zero and visible.

Request parameters may select a trusted source profile, stable cohort, season,
phase, grain, attempt, game, or stage. They must never accept an arbitrary
project, dataset, table name, or SQL fragment.

---

## 4. Approved protected Admin product shape — 2026-08-19

The protected Admin product keeps two views with different grains:

1. the existing aggregate **Claim Health** scorecard; and
2. a separate **Game Journey** operations view with one row per scheduled game.

The existing aggregate sections — Overview, Game Calibration, Core Areas,
Pillars, Features, and Technical Debug — remain aggregate-only. Game Journey is
the fast QA surface: an operator scans the slate, sees where each game is in
the lifecycle, and opens one game for evidence and retry detail.

### A. Game Journey is anchored on Schedule

Schedule supplies the expected game row. All evidence is attached with explicit
`LEFT JOIN` relationships so missing data becomes a visible state rather than
removing the game or failing the whole response.

The primary table should show:

- game ID, matchup, kickoff, game status, and current clock;
- pregame and postgame stage status chips;
- the first warning or failure, when present; and
- a details action.

Approved status vocabulary:

```text
complete
waiting
no_work_needed
not_applicable
warning
failed
```

The status and the operator's attention level are separate. A completed stage
with a valid zero-row result is successful, not missing. For example, a Level
2 or Level 3 `no_op / zero_claims` receipt displays as **OK (0)** because the
worker ran and correctly found no Level 1 claims to process.

The primary screen separates exceptions into two quiet lists:

- **Needs Attention** contains only current failures and unexpected warnings
  that an operator can investigate or retry.
- **Known Gaps** contains truthful historical or non-repairable conditions,
  including `kickoff_reached` for the six missed preseason captures and the
  DAL–SEA Level 1 receipt gap after kickoff.

Known gaps remain visible in the affected game's details, but they do not ring
the same alarm as an actionable failure. Waiting and not-applicable stages do
not appear in either exception list.

A click opens the game's details: stage, canonical source and lineage,
`capture_id`, counts, status reason, timestamps, and safe retry guidance. One
failed game must not hide or invalidate healthy sibling games.

### B. Three clocks keep data loading separate from frozen GameLens evidence

| Clock | Displayed stages | Meaning |
|---|---|---|
| Daily Data Load | Schedule → Final Score + Stats → target-game Facts → target-game Windowed Metrics → game-date Rankings | Whether the production 8:00 a.m. pipeline loaded the completed game through each canonical data stage |
| GameLens Pregame | Snapshot → frozen context → Level 1 | What GameLens actually preserved before kickoff and whether claim extraction has a receipt |
| Postgame Learning | Frozen game grade → Level 2 → Level 3 → Level 4 relationship | The completed-game evaluation and learning progression after final |

These labels are a user-facing journey, not permission to rewrite worker grains.

The Daily Data Load clock and frozen pregame context answer different questions.
For the load clock, Facts are checked directly by target `game_id`; Windowed
Metrics are checked by `latest_included_game_id`; and Rankings are checked for
both scheduled teams on the game date and the phase-appropriate window. The
rankings table has no `game_id`, so Packet 5 must not invent one.

The frozen-context stage continues to read only the canonical snapshot's
source dates, pipeline lineage, ranking availability, and evidence context. A
successful next-morning load does not prove that GameLens captured those values
before kickoff. Conversely, a missed snapshot does not mean the daily ETL
failed. The Admin view must display both truths independently.

Above the game clocks, a bounded **Run Summary** preserves attempt-level truth.
Packet 2 capture/Level 1 attempts come from `stage_runs`; Packet 4 learning
attempts come from the `receipt_scope = 'attempt'` rows in
`postgame_learning_stage_receipts`. The summary shows attempt ID, stage,
status/reason, input/output counts, duration, and finish time. It does not
force attempt-level rows onto individual games. Per-game cells continue to use
the canonical snapshot, game-stage receipt, claim, and grade owners.

Level 4 is weekly/batch work, not a per-game worker. Until its contract is
implemented, Game Journey may display only the game's relationship to that
batch:

```text
not_eligible
waiting_for_completed_week
eligible
included_in_week_<n>
excluded
```

An excluded state must include a reason. Packet 5 does not run Level 4 or invent
a game-grain Level 4 record.

### C. Explicit joins preserve the six grains

The read path normalizes Schedule `gameID` to canonical `game_id`, then uses
only approved identities:

- snapshot, claim, and outcome lineage joins through
  `learning_run_id + capture_id + game_id`;
- claim detail adds `claim_key`;
- capture receipts join through `attempt_id + stage_name + game_id`;
- postgame receipts join through
  `attempt_id + receipt_scope + game_id + stage_name`; and
- production Scores join by normalized target `game_id`;
- target Facts join by `game_id`;
- target Windowed Metrics join by `latest_included_game_id`;
- target Rankings join by Schedule game date, away/home team, and reviewed
  phase window because their canonical grain contains no `game_id`; and
- frozen pregame context uses only stored snapshot source dates/evidence.

A join may classify absence as waiting, not applicable, capture missing, or
failed. It may not fabricate a row, coalesce across cohorts/phases, reconstruct
a capture, or turn a current upstream value into historical pregame evidence.

### D. Preserve the existing claim-health endpoint

Keep the existing route, authentication, historical default behavior, section
names, and aggregate-only contract.

A later Packet 5 slice may add one server-owned source profile such as:

```text
historical_analytics
development_learning
```

The value must map to reviewed table identifiers in code. It is not a raw table
parameter.

`development_learning` is allowed only in an explicitly development runtime.
It reads `GameLens_dev.claim_training_examples` and
`GameLens_dev.game_model_outcomes`, plus the approved read-only Schedule
source when expected-game context is requested.

For the development source:

- Claim Health comes from `claim_training_examples`.
- Game Calibration comes from `game_model_outcomes`.
- stored confidence is reported; Calibrated Matchup Lean is not recalculated.
- zero-claim cohorts return truthful empty claim matrices and independent game
  grade counts.
- existing historical response fields remain backward compatible.

### E. Add the protected API doorway for Game Journey

The existing claim-health endpoint deliberately contains no game-level rows.
This new protected endpoint is the doorway the future Admin screen will call.
It promises one stable response shape — scheduled games, clocks, stage statuses,
and detail — while the service initially assembles that response from clear
joins across Schedule and the six canonical tables.

The API promise and the storage choice are deliberately separate. If Week 1
measurements later prove that direct joins are too slow, expensive, or hard to
operate, the same API may read from an approved materialized Game Journey read
model without changing the frontend. Packet 5 must first prove the exact grain,
freshness rules, and partial-failure behavior. It must not create that seventh
derived table from convenience or assumption.

Operational diagnosis requires one scheduled-game row plus bounded stage
detail. That is a proven API-grain gap, not yet a proven storage gap.

The proposed bounded protected route remains:

```text
GET /admin/gamelens/run-visibility
```

The frontend name is **Game Journey**. The route may accept reviewed filters
such as:

- `learning_run_id`;
- `attempt_id`;
- `game_id`;
- `stage_name`;
- `season_type`; and
- a bounded result limit.

It reads Schedule, the approved production read-only Score/Facts/Windowed/
Rankings sources, and the six existing development tables. It may reconcile
canonical data-load, snapshot, claim, outcome, and receipt counts; it does not
recompute football results.

The response should contain:

```text
scope
source_profile
filters
cohort_summary
games
game_stage_results
reconciliation
warnings
generated_at
```

Every failure row must answer:

1. Which game?
2. Which stage?
3. Which capture and upstream version?
4. What failed?
5. What is safe to retry?

The route must remain Admin-authenticated, read-only, bounded, and separate from
the public frontend.

### Why two read contracts are preferable

The existing endpoint is an aggregate scorecard. Game Journey is an operational
diagnostic. Combining unbounded per-game receipts into the aggregate
claim-health response would violate its current grain and make the historical
frontend contract harder to preserve.

Two protected read contracts share source adapters and identifiers without
sharing response grains or creating duplicate storage.

---

## 5. Required reconciliation metrics

### Lifecycle coverage

For one stable `learning_run_id + season_type`, expose:

| Metric | Canonical owner |
|---|---|
| Expected games when requested | Approved read-only Schedule source |
| Captured games | Distinct canonical games in `pregame_snapshots` |
| Capture-missing games | Schedule/coverage classification plus latest per-game receipt |
| Level 1 processed games | `stage_game_results` at `level1_claim_extraction` |
| Games with claims | Distinct games in `claim_training_examples` |
| Zero-claim processed games | Successful/no-op Level 1 result with output zero |
| Graded games | `game_model_outcomes` |
| Level 2 labeled claims | Claim rows with a Level 2 result, including explicit unavailable |
| Level 3 featured claims | Claim rows with a populated feature formula version |
| Packet 4 stage outcomes | Canonical game-stage receipts by status/reason |

### Required inequalities and equalities

The read path must check and explain, not force:

- canonical captures are unique at their approved identity;
- every claim points to one existing cohort/capture/game;
- every grade points to one existing cohort/capture/game;
- Level 2-labeled claims cannot exceed Level 1 claim rows;
- Level 3-featured claims cannot exceed Level 1 claim rows;
- receipt logical keys are unique;
- claim validation counts plus explicit unavailable/unprocessed counts
  reconcile to the selected claim population;
- game-grade populations and claim populations remain separate;
- captured games may temporarily exceed processed or graded games;
- games with claims may be fewer than Level 1 processed games because zero
  claims are valid; and
- partial failure preserves successful sibling games and stages.

No query may create artificial equality by dropping zero-claim,
capture-missing, skipped, unavailable, or failed states.

---

## 6. Data-source and security boundary

Packet 5 is read-only.

Implementation must:

- preserve `require_admin_auth`;
- use parameterized values for filters;
- bind fully qualified table names from server-owned source profiles;
- reject the development source outside an explicit dev runtime;
- refuse writes, DDL, temporary Admin tables, and refresh jobs;
- keep production Schedule/Analytics access explicitly read-only during
  development proof;
- avoid returning secrets or unrestricted exception details;
- bound attempt/game-stage rows and require a narrow filter for detail queries;
- preserve existing CORS/auth behavior; and
- leave `app.py`, the 8:00 a.m. run, `/game`, and the frontend unchanged
  until a later explicitly reviewed route-registration slice.

A route registration, if approved in Packet 5, is a protected read-only API
change. It is not learning orchestration or production activation.

---

## 7. DRY boundary

| Responsibility | Single owner Packet 5 must read or call |
|---|---|
| Historical Admin aggregates | Existing Admin query/service functions |
| Frozen product evidence | `pregame_snapshots` |
| Claim selection, Level 2 labels, Level 3 features | Existing shared claim row |
| Game Outcome and Model Trust | Stored `game_model_outcomes`; never rerun the builders in Admin |
| Capture attempt/game state | `stage_runs` and `stage_game_results` |
| Postgame stage state and retry details | `postgame_learning_stage_receipts` |
| Metric meaning and `lens_tags` | Existing metric registry and stored rows |
| Calibrated Matchup Lean | Stored effective confidence from the released shared rule |
| Selection, grouping, reconciliation, response formatting | Thin Admin query/service adapters |

Admin must not call `build_model_outcome(...)`,
`build_model_trust(...)`, Level 1 extraction, Level 2 validation, Level 3
feature calculation, or the Calibrated Matchup Lean helper. It reports stored
evidence only.

---

## 8. Implementation slices after review

### Visual review checkpoints

Packet 5 is intentionally delivered in small, behavior-sized commits rather
than one atomic implementation. Christian reviews the data shape between
checkpoints:

| Checkpoint | Deliverable Christian can inspect | Work allowed afterward |
|---|---|---|
| 0 — Plan | The approved Game Journey and API-doorway explanation | Slice 1 only |
| 1 — Source inventory | Six-table grain/count/identity report with warnings | Assemble the read-only journey sample |
| 2 — Game Journey sample | Human-readable table plus JSON for every scheduled sample game | Refine joins and status meanings |
| 3 — Focused tests | Invariant results and deliberately missing/failed examples | Consider service integration |
| 4 — Protected API | Authenticated local response with bounded filters | Consider frontend work separately |
| 5 — Week 1 proof | Real claim-bearing slate, timing, cost, and freshness evidence | Decide whether live joins remain sufficient or a stored read model is justified |

At each checkpoint, pause for visual inspection of row counts, representative
games, warnings, and stage meanings. No checkpoint requires every row to be
green. A later checkpoint may adjust the presentation without rewriting
canonical Packet 1–4 evidence.

Commit boundaries should normally match one behavior: documentation, inventory
queries, journey assembly, focused tests, API service, and route registration.
Avoid unrelated cleanup.

### Slice 1 — read-only source, grain, and Game Journey inventory

Before route or service changes:

- inspect all six live development schemas and layouts;
- count rows and distinct logical keys by table;
- list stable learning cohorts and season phases;
- detect duplicate keys, orphan captures/claims/grades, and alias disagreement;
- report latest attempt/receipt timestamps and statuses;
- prove the seven current preseason captures remain an honest zero-claim
  cohort;
- emit one read-only Game Journey row for each scheduled preseason game;
- classify every daily-load, pregame, and postgame-learning stage with the
  approved status vocabulary;
- show successful zero-row Level 2/3 receipts as `OK (0)` rather than implying
  those workers did not run;
- render a bounded attempt-grain Run Summary from `stage_runs` and Packet 4
  attempt receipts without joining those attempts onto arbitrary games;
- separate actionable Needs Attention items from non-repairable Known Gaps;
- include expandable-detail evidence in the JSON shape, including lineage,
  counts, reasons, timestamps, and retry guidance; and
- perform no write.

Output one compact JSON report to stdout. Do not create an inventory table,
route, or frontend component. Tests must protect identities and lifecycle
invariants, not freeze today's exact row counts or require every game to be
green. The seven-game sample is sufficient for the first pass because it
contains success, honest zero-claim processing, a missing capture, partial
failure, and exact retry evidence.

### Slice 2 — trusted source bindings and reconciliation queries

- introduce server-owned table/source bindings;
- keep historical Admin queries backward compatible;
- add development-only canonical claim and grade aggregates;
- add lifecycle reconciliation queries over existing tables;
- use parameterized filters and explicit phase gates; and
- add no route yet if the response shape is still under review.

### Slice 3 — aggregate Admin service integration

- preserve existing claim-health fields and historical behavior;
- add current-source lifecycle coverage and canonical game calibration;
- keep claim health separate from game calibration;
- keep zero-claim output explicit; and
- prove the existing Admin response contract does not regress.

### Slice 4 — bounded run-visibility service and protected route

- add the separate protected read contract;
- return attempt plus per-game/per-stage trace;
- require bounded filters for detail;
- preserve successful siblings beside failures;
- include stable reason, retryability, and log reference;
- register only after local auth/route tests pass; and
- make no frontend change.

### Slice 5 — development-cloud read-only proof

Using the current development evidence:

- reconcile seven canonical preseason captures;
- show the zero-claim Level 1 population truthfully;
- show canonical development grades independently of claims;
- reproduce the Packet 4 healthy multi-game receipt result;
- reproduce DAL–SEA success beside CAR–ARI capture-missing failure and skipped
  downstream stages;
- prove the identical retry is distinguishable from the first write;
- prove preseason cannot appear in a production learning source profile;
- record query scope, counts, duration, and result limit; and
- rerun the response against the first Week 1 slate and first genuine
  claim-bearing games when available, treating that as the planned real-data
  expansion and operational revalidation point rather than retroactively
  manufacturing preseason evidence.

### Slice 6 — documentation closure

- record exact source bindings, response fields, tests, and cloud counts;
- update the Sprint, README, architecture handoff, runtime guide, and go plan;
- update the recreation runbook only if an approved schema changed;
- decide Packet 5 GO or NO-GO; and
- hand off to the Packet 6 plan without production wiring.

---

## 9. Required tests

### Source and identity tests

- historical source profile retains current tables and response shape;
- development source binds only to the six approved tables;
- development source is rejected outside dev;
- arbitrary dataset/table names are rejected;
- `run_id == learning_run_id` alias disagreement is visible;
- duplicate capture, claim, grade, and receipt logical keys fail reconciliation;
- orphan claims or grades are visible;
- preseason and regular-season cohorts cannot mix.

### Aggregate Admin tests

- canonical game calibration reads the grade table, not `ANY_VALUE` from claims;
- zero-claim games can appear in game calibration while claim matrices remain
  empty;
- validation, unavailable, unprocessed, and total claim counts reconcile;
- Level 3 feature counts cannot exceed claim counts;
- stored effective confidence is reported without recalculation;
- the historical calibrated preview remains clearly research-only;
- day/week/season-phase grains use a stable cohort rather than daily run IDs;
- existing response sections remain backward compatible.

### Run visibility tests

- every scheduled game remains visible through Schedule-anchored `LEFT JOIN`
  relationships even when downstream evidence is absent;
- success, no-op, skipped, waiting, not-applicable, warning, failure, and
  partial failure are distinguishable;
- a true zero displays as zero;
- a successful `no_op / zero_claims` receipt displays as `OK (0)`;
- known historical gaps remain visible without inflating Needs Attention;
- the attempt-grain Run Summary is bounded and does not alter game grain;
- tests do not require exact current row counts or an all-green slate;
- target Facts, Windowed Metrics, and Rankings use their canonical daily-load
  relationships, while frozen pregame context remains snapshot-owned;
- a completed daily load cannot turn a missed snapshot into a capture;
- Level 4 is represented only as a weekly/batch eligibility relationship;
- latest Packet 2 per-game status does not replace the canonical snapshot;
- Packet 4 attempt and game-stage receipts retain their separate grains;
- DAL–SEA remains visible when CAR–ARI fails;
- failed boundary, retryability, exception class, message, and log reference
  are returned safely;
- first-write effects may differ from identical-retry effects without changing
  immutable evidence;
- detail results are bounded;
- Admin authentication remains required.

### Safety tests

- no BigQuery mutation method is called;
- no DDL or staging table is created;
- no football calculation worker is called;
- no `app.py` Scheduler or learning wiring is added;
- `/game` and frontend contracts remain unchanged; and
- the released Calibrated Matchup Lean rule is not reimplemented.

---

## 10. Packet 5 GO evidence

Packet 5 receives Implementation GO only when:

1. this plan is reviewed before code;
2. the six table grains and logical identities are verified from code and a
   read-only development inventory;
3. no duplicate summary warehouse or refresh job is created;
4. historical Admin behavior remains backward compatible;
5. current game calibration reads canonical grade rows independently of claims;
6. current claim health reads canonical claim rows;
7. zero-claim and capture-missing states remain truthful;
8. operational status reads existing receipts and preserves their distinct
   grains;
9. every failure answers the five traceability questions;
10. phase/cohort isolation is enforced consistently;
11. all Packet 5 behavior is read-only;
12. auth, `/game`, frontend, Scheduler, 8:00 a.m. ETL, and production learning
    writes remain unchanged;
13. local tests and the development-cloud read-only proof pass; and
14. exact evidence plus any remaining gates are documented.

Implementation GO would approve only the read-only Packet 5 Admin/visibility
boundary. It would not authorize production datasets, production learning
writes, Level 4, Scheduler wiring, `main`, or frontend changes.

---

## 11. What Packet 5 does not do

Packet 5 does not:

- manufacture claims or rewrite zero-claim evidence;
- reconstruct CAR–ARI or any missed capture;
- run Levels 1–4;
- create a daily Admin summary table;
- create a generalized pipeline-run warehouse;
- replace or consolidate the three receipt tables;
- change a Packet 1–4 table schema unless read-only proof establishes a
  concrete missing field;
- use the development setup scripts as production migrations;
- wire learning after the 8:00 a.m. run;
- change `/game`, Matchup Lean, Model Trust, claim language, or the frontend;
- reimplement the released Calibrated Matchup Lean rule;
- create production GameLens tables; or
- merge learning work to `main`.

The historical August readiness plan proposed a materialized Admin daily
summary. Current code and Packet 1–4 evidence supersede that assumption. Packet
5 begins by querying canonical grains directly. A future summary table requires
measured query-cost or retention evidence and its own approved grain; convenience
alone is not a concrete gap.

---

## 12. Owner review decision — 2026-08-19

Christian approved the first-pass Admin product direction:

- preserve the existing aggregate Claim Health view;
- add a separate protected Game Journey view with one row per scheduled game;
- split each row into Daily Data Load, GameLens Pregame, and Postgame Learning
  clocks so target-game ETL readiness is not confused with frozen evidence;
- show clear stage statuses and open a game for source, lineage, counts, reason,
  timestamps, and retry detail;
- show completed zero-row learning stages as `OK (0)`, and separate actionable
  Needs Attention items from visible Known Gaps so the page is not a noisy
  gong;
- represent Level 4 honestly as a weekly/batch relationship rather than a
  per-game stage;
- use clear Schedule-anchored `LEFT JOIN` relationships and preserve the six
  canonical development grains;
- create no Admin summary warehouse or new table; and
- use Week 1 and the first genuine claim-bearing games to expand and revalidate
  the display without manufacturing preseason claims.

Product details may evolve after real Week 1 data is visible. That expected
iteration is not a reason to over-lock the first pass. Tests should enforce
architectural and lineage invariants while allowing honest missing, waiting,
zero, failed, and not-applicable states.

### Authorized next action

Turn the accepted Slice 1 assembly into a development-only, bounded backend
read service. The default response must be compact and hierarchical; full
source, reason, lineage, and retry evidence is returned only for a selected
game. Do not register a route, change the frontend, create a new table, change
`app.py`, wire the pipeline, or enable production learning in this checkpoint.

---

## 13. Checkpoint 1 accepted evidence — 2026-08-19

Christian accepted the data shape after running the read-only command against
the 2026-08-15 preseason slate.

### Observed canonical source health

| Table | Rows | Logical keys | Duplicates | Invalid keys |
|---|---:|---:|---:|---:|
| `pregame_snapshots` | 7 | 7 | 0 | 0 |
| `stage_runs` | 5 | 5 | 0 | 0 |
| `stage_game_results` | 17 | 17 | 0 | 0 |
| `claim_training_examples` | 0 | 0 | 0 | 0 |
| `game_model_outcomes` | 2 | 2 | 0 | 0 |
| `postgame_learning_stage_receipts` | 18 | 18 | 0 | 0 |

All six tables were available. No duplicate or invalid logical keys were
reported, and the command performed no write.

### Accepted seven-game interpretation

- all seven scheduled games completed Schedule, Score/Stats, target Facts,
  target Windowed Metrics, and game-date Rankings;
- six games truthfully retain the known `kickoff_reached` snapshot gap;
- DAL–SEA has its canonical snapshot and frozen context;
- DAL–SEA has one canonical frozen grade;
- DAL–SEA Level 2 and Level 3 both ran successfully with zero claims and display
  `OK (0)` while preserving the raw `no_op / zero_claims` receipts;
- DAL–SEA's missing Level 1 receipt after kickoff remains a Known Gap;
- the slate has zero Needs Attention games and seven Known Gap games; and
- Level 4 remains not applicable because Packet 5 does not implement its
  weekly contract.

The attempt-grain Run Summary also reconciled Packet 2 capture attempts and
Packet 4 learning attempts without forcing those rows onto individual games.
The historical Packet 4 partial-failure attempt remains visible as raw run
history; it is not automatically promoted into a current-slate alarm.

### Approved drill-down hierarchy for the backend contract

The future QA surface should reveal information progressively:

| Level | Default content | Opened content |
|---|---|---|
| 1 — Overview | Source health, scheduled/captured counts, Needs Attention, Known Gaps, recent attempts | Source-table counts and attempt reasons |
| 2 — Game row | Game ID, matchup, kickoff, overall state, three clock rollups, stage chips, first issue | One selected game |
| 3 — Clock | Daily Data Load, GameLens Pregame, or Postgame Learning rollup | All stages within that clock |
| 4 — Stage evidence | Status and count | Reason, canonical source, lineage/details, timestamps, and retry evidence |

The unselected response must not return every stage's diagnostic payload. A
selected `game_id` may return the full detail for exactly that game. This keeps
the frontend scannable while preserving the clear joins and evidence needed
for QA.

---

## 14. Checkpoint 2 accepted service shape — 2026-08-19

Christian accepted the hierarchical read-service preview against the saved
Checkpoint 1 report.

The default response correctly showed:

- six of six healthy canonical table grains;
- seven scheduled games, one captured game, zero Needs Attention games, and
  seven Known Gap games;
- one compact game row per scheduled game;
- separate Daily Data Load, GameLens Pregame, and Postgame Learning rollups;
- quiet Known Gaps rather than false active alarms; and
- bounded recent attempt history that does not alter per-game grain.

Selecting DAL–SEA correctly opened only that game's detailed evidence:

- the canonical `capture_id` and stable `learning_run_id`;
- complete target Score/Stats, Facts, Windowed Metrics, and Rankings;
- complete snapshot and frozen-context capture;
- the known missing Level 1 receipt after kickoff;
- one canonical frozen game grade;
- completed Level 2 and Level 3 zero-claim receipts; and
- honest Level 4 not-applicable state.

The CLI table truncates long source labels for readability; the underlying
selected-game JSON retains the complete values. The frontend should keep Recent
Runs collapsed by default and render `no_ranking_rows_found` as a secondary
availability note beneath a completed frozen-context capture, not as an active
failure.

### Authorized protected route checkpoint

Register the existing proposed route:

```text
GET /admin/gamelens/run-visibility
```

The route must:

- reuse `require_admin_auth` without creating a second auth mechanism;
- require `season_type`, `learning_run_id`, `start_date`, and `end_date`;
- accept optional `season`, canonical `game_id`, and bounded `limit`;
- reuse the accepted hierarchical service without duplicating SQL or stage
  classification;
- return safe 400 responses for malformed filters, 404 for a selected game
  outside the requested slate, and a generic 500 for unexpected query failure;
- reject the development source outside a validated dev runtime;
- preserve the compact default response and one-game drill response exactly;
- remain GET-only and read-only; and
- make no frontend, Scheduler, `/game`, table, production-learning, or 8:00 a.m.
  pipeline change.

After local route/auth tests pass, pause for a visual HTTP-response checkpoint
before any frontend component is added.

---

## 15. Protected route test checkpoint — 2026-08-20

Christian pulled `dev` through the Flask automatic-`OPTIONS` correction and
ran the focused Packet 5 route/service suite plus its Packet 4 schema dependency:

```text
python -m unittest \
  tests.test_admin_run_visibility_routes \
  tests.services.test_gamelens_admin_run_visibility_service \
  tests.test_qa_gamelens_packet5_admin_inventory \
  tests.test_qa_gamelens_packet4_schema_inventory
```

Observed result:

```text
Ran 40 tests in 12.729s

OK
```

The corrected preflight test accepts Flask's automatic HTTP `200` response and
still proves that `GET` and `OPTIONS` are advertised. The application route,
service, SQL, authentication, and read-only behavior were not weakened to make
the test pass.

This evidence confirms:

- the existing Admin guard protects the route;
- authenticated non-Admin users remain rejected;
- bounded filters reach the accepted service contract;
- malformed filters, an unknown selected game, an unavailable development
  source, and an unexpected query failure produce safe responses;
- `POST` remains unavailable while browser preflight can pass;
- the blueprint is registered in `app.py`; and
- the inventory and service dependencies still pass their focused tests.

The next checkpoint remains visual and read-only: run the protected route
rehearsal against canonical development data, inspect the overview and one
selected game, then decide whether its shape is ready for a Lovable wireframe.
No frontend work is authorized by the test result alone.
