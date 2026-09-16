# Chunk D — Backend Implementation Handoff

**Status:** complete
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

## Unresolved risks for Chunk E

1. Run endpoint-specific fixtures for every frozen state, especially raw duplicates, source alignment conflicts, nullable/non-finite percentiles, and route dispatch under Flask.
2. Exercise the parameterized BigQuery helpers against approved read-only evidence and measure the added raw boundary/catalog read plus source-alignment read.
3. Verify the Flask router resolves the nested path before the existing catch-all in a dependency-complete environment and confirm auth bodies are byte-for-byte unchanged.
4. Verify the exact runtime table column types match the frozen integer/date validations.

## Next dependency

Chunk E may independently add the endpoint tests and fixtures defined by the contract. No deployment, frontend work, merge, or Chunk E execution is authorized by this handoff.
