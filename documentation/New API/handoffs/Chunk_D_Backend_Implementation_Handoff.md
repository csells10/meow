# Chunk D — Backend Implementation Handoff

**Status:** complete; post-freeze contract repair implemented; Chunk E re-verification pending
**Branch:** `feature/matchup-lens-api`
**Starting commit:** `83ebe7b`
**Assigned model / effort:** `gpt-5.6-terra` / Medium
**Scope:** frozen Matchup Lens backend endpoint only

## Outcome

Implemented the frozen, Firebase-authenticated `GET /game/<path:game_id>/lens-context` endpoint and no frontend, deployment, test, data, pipeline, or orchestration work.

The service validates only game-ID structure, derives canonical identity from the schedule header, selects the existing phase-aware window, performs the required raw ranking boundary/catalog read before calling the unchanged ranking helper, enforces raw duplicate protection, makes the exact source-date-aligned windowed lookup, and returns deterministic `matchup_lens_v1` JSON.

Expected absence returns the frozen `200` unavailable states. Unsafe/malformed evidence returns the frozen `409` states. Upstream `DeadlineExceeded` maps to `504`; unexpected exceptions map to the safe `500` envelope. Existing Firebase decorator responses remain unchanged.

## Changed paths

- Added `services/matchup_lens_service.py`
- Appended isolated read-only helpers to `queries/game_queries.py`
- Added the authenticated nested route and service import in `routes/game_routes.py`
- Added this handoff

No other path was changed.

## Implementation notes

- Runtime catalog is the lexicographically sorted exact league snapshot, with consistent `label`, `signal_strength`, and normalized tag definitions required per metric. It never uses 59, 63, 70, or 73 as a fixed denominator.
- Raw duplicate detection occurs at `season + as_of_date + window_type + metric + team_id`, before metric dictionaries from `get_team_rankings_for_game()` are trusted.
- The unchanged helper is called without a metric allowlist; its two team key sets must equal the validated raw team key sets.
- The window helper receives parallel exact team/metric/source-date tuples and zips them inside parameterized BigQuery SQL. It cannot select a merely newer pregame row.
- Responses contain only away/home team evidence; fixed league-context suppression, six readiness rows, named Collision/Turnover consumer coverage, warnings, field order, compact UTF-8 serialization, `allow_nan=False`, and a trailing newline follow the decision record.
- The existing catch-all `/game/<path:game_id>` handler remains in place unchanged other than the file newline; the more-specific nested route is declared before it.

## Verification performed

| Command / exercise | Result |
|---|---|
| `python -m py_compile services/matchup_lens_service.py queries/game_queries.py routes/game_routes.py` | Passed |
| `git diff --check` | Passed |
| Temporary dependency-stubbed, in-memory synthetic builder exercise | Passed: `200`, available success envelope, catalog count, finite compact JSON, and trailing newline |
| AST static check for both route patterns and both appended helper names | Passed |
| Complete diff inspection | Completed before commit |

No persisted test or fixture was created or modified; that remains Chunk E ownership. The scratch interpreter does not have the Google packages installed, so the pure synthetic exercise used temporary in-process import stubs rather than attempting a live BigQuery read. No BigQuery job or write was performed.

## Narrow repair after Chunk E

**Repair starting commit:** `9f6bb3c44a42859b02a2e04c058a11f9e6358857`
**Repair scope:** E-D-001 and E-D-002 only

Chunk E independently reproduced two frozen-contract defects. Christian
authorized a narrow Chunk D repair before continuing verification.

### E-D-001 — zero-game provenance invariant

The service now requires the source-aligned pair to be internally consistent:

- `games_in_window == 0` requires `latest_included_game_id == null`;
- `games_in_window > 0` requires a non-null
  `latest_included_game_id`.

Either contradictory shape returns the existing frozen
`409 SOURCE_ALIGNMENT_CONFLICT` response.

### E-D-002 — date-derived freshness

For every validated metric, `data_lag_days` is now recomputed as the calendar
difference between the selected `as_of_date` and the metric's validated
`source_data_date`. Team lag and `basis.max_data_lag_days` continue to be
derived from those normalized metric values. A stale but otherwise valid helper
lag cannot become response truth.

### Repair paths

- Updated `services/matchup_lens_service.py`.
- Updated this Chunk D handoff.

No query, route, test, fixture, dependency, runtime configuration, pipeline,
schema, table, job, learning/orchestrator component, or controlling contract
document was changed.

### Repair verification

| Command | Result |
|---|---|
| `python -m pytest -q tests/test_matchup_lens_endpoint.py -k "zero_games_requires_null_latest_included_game_id or lag_is_derived_from_dates_instead_of_trusting_helper_value"` | Passed: 2 passed, 35 deselected |
| `python -m pytest -q tests/test_matchup_lens_endpoint.py` | Passed: 37 passed |
| `python -m pytest -q tests/test_game_window_selection.py` | Passed: 2 passed |
| `python -m py_compile services/matchup_lens_service.py tests/test_matchup_lens_endpoint.py` | Passed |

No live BigQuery call was made in this repair environment. Christian will run
the later authenticated read-only smoke test from his Visual Studio terminal.
Chunk E must still record its independent repair-verification verdict before
Chunk F begins.

## Unresolved risks for Chunk E

1. Run endpoint-specific fixtures for every frozen state, especially raw duplicates, source alignment conflicts, nullable/non-finite percentiles, and route dispatch under Flask.
2. Exercise the parameterized BigQuery helpers against approved read-only evidence and measure the added raw boundary/catalog read plus source-alignment read.
3. Verify the Flask router resolves the nested path before the existing catch-all in a dependency-complete environment and confirm auth bodies are byte-for-byte unchanged.
4. Verify the exact runtime table column types match the frozen integer/date validations.

## Next dependency

Chunk E may independently add the endpoint tests and fixtures defined by the contract. No deployment, frontend work, merge, or Chunk E execution is authorized by this handoff.


## Post-freeze contract repair after live BigQuery/Lovable evidence

**Repair starting commit:** `4b675c3ad894de15ede6aec5aea899b23826bb2e`  
**Repair scope:** amended Chunk C signal/readiness/provenance contract only

The read-only live smoke request for `20260917_DET@BUF` reached BigQuery but
returned the safe `409 INVALID_METRIC_EVIDENCE` state. The source evidence
itself was aligned: snapshot `2026-09-14`, source date `2026-09-13`, DET 62
metrics, BUF 63 metrics, matching raw/helper keys, and no catalog-definition
conflicts. Lovable inspection then confirmed its working demo is normalized
offline and does not consume ranking-table `numerator` or `denominator`
provenance fields.

### Implementation

- Exact lowercase `strong`, `supporting`, and `context` are now accepted
  transport signals in both raw catalog and team-payload validation.
- Null, unknown, and differently cased signals still fail safely as
  `409 INVALID_METRIC_EVIDENCE`.
- `context` remains in the dynamic catalog, transported team metric maps, and
  top-level/team coverage counts.
- Six-lens readiness now requires both the existing tag rule and a scoring
  signal of `strong` or `supporting`. Context metrics do not enter lens
  denominators or lens missing lists.
- `numerator` and `denominator` are no longer validated, normalized, or
  serialized. No other metric response field was removed.
- No numeric weight was introduced. Existing scoring weights and modifiers
  remain frontend-owned.
- The earlier date-derived lag and bidirectional games/latest-game consistency
  repairs remain intact.

### Changed paths

- Updated `services/matchup_lens_service.py`.
- Updated this Chunk D handoff.

No query, route, test, fixture, dependency, runtime configuration, pipeline,
schema, table, job, frontend, learning/orchestrator component, or controlling
contract document was changed.

### Verification

- Complete service diff inspected: four narrow source substitutions only.
- Confirmed the service contains no serialized `numerator` or `denominator`
  keys.
- Confirmed both accepted-signal checks use exactly
  `strong | supporting | context`.
- Confirmed readiness permits only `strong | supporting`.
- Confirmed the date-derived lag and games/latest-game invariant repairs remain.
- Python syntax parsing was performed on the complete amended service before
  the repository write.
- Persisted Chunk E tests were intentionally not edited. Their exact metric
  field-order expectation and signal fixtures must be amended independently by
  Chunk E before its full suite can represent the amended contract.

### Verdict and next dependency

**PASS — narrow Chunk D implementation repair only.**

Live BigQuery validation is not reported as passed. Chunk E remains blocked
until it updates its independently owned tests for the amended contract and the
read-only live smoke request succeeds. No Chunk F, deployment, or merge work is
authorized by this repair.
