# Chunk E — Independent Endpoint Verification Handoff

**Status:** complete — failed; Chunk D repair authorization required
**Verdict:** **FAIL**
**Branch:** feature/matchup-lens-api
**Verified branch head:** 3e2dcef7f189dce1af2959541f27fb3c074b8701
**Assigned model / effort:** gpt-5.6-sol / High
**Scope:** Chunk E endpoint-specific verification only

## Outcome

Chunk E independently verified the frozen Matchup Lens endpoint with clearly
labeled synthetic data. Thirty-five endpoint checks passed. Two endpoint checks
failed and reproduce narrow Chunk D contract defects. The relevant unchanged
window-selection regression tests also passed.

No implementation file was repaired or edited. No deployment, merge, frontend
work, Lovable communication, BigQuery write, scheduled job, learning flow, or
orchestrator flow was performed.

The branch must stop here for explicit Chunk D repair authorization. Chunk F is
not authorized while this verdict is failed.

## Starting-state confirmation

- The GitHub-connected branch head was exactly
  3e2dcef7f189dce1af2959541f27fb3c074b8701.
- The completed Chunk C contract decision record and handoff were present.
- The completed Chunk D implementation handoff was present.
- The handoff directory contained A, B, C, and D only; there was no later
  handoff to reconcile.
- The Chunk C decision record controlled all expected results.

## Changed paths

- Added tests/test_matchup_lens_endpoint.py.
- Added documentation/New API/handoffs/Chunk_E_Verification_Handoff.md.

No fixture file was needed. Every source value in the test module is explicitly
labeled synthetic. No other repository path is part of the Chunk E commit.

## Synthetic verification coverage

| Gate | Result | Evidence |
|---|---|---|
| Nested route versus existing /game/<game_id> and /games | Pass | Flask test client reaches the lens handler only for the nested path; controlled existing route payloads remain separate |
| Existing Firebase 401/403 bodies and builder short-circuit | Pass | Four bodies asserted byte-for-byte; builder asserted not called |
| Success envelope, nested field order, compact deterministic bytes, trailing newline, allow_nan=False | Pass | Repeated serialization is byte-identical; schema/key ordering and source call are asserted |
| Only canonical away/home teams | Pass | Response contains only away and home; header teams override URL display abbreviations |
| Dynamic runtime catalog | Pass | Synthetic league-only metric appears without any 59/63/70/73 assumption |
| Raw duplicate rejection before dictionary helper | Pass | Duplicate protected grain returns 409 DUPLICATE_RANKING_ROWS; helper is not called |
| Structural ID and canonical header identity | Pass | Invalid shapes stop before reads; response identity comes from the canonical header |
| Missing exact source alignment | Pass | 200 SOURCE_ALIGNMENT_UNAVAILABLE with evidence fields cleared |
| Duplicate/disagreeing source alignment | Pass | 409 SOURCE_ALIGNMENT_CONFLICT |
| Newer pregame window row | Pass | Newer synthetic row is not attached; exact source-date count and latest game remain selected |
| Both-team top-level requirement | Pass | Missing home rows return 200 MISSING_TEAM_EVIDENCE before helper use |
| Partial/null lens behavior | Pass | Matchup remains available; only Drive Control becomes partial; null percentile remains valid |
| Six readiness rows | Pass | Exact frozen order and keys asserted |
| Twelve named Collision/Turnover consumers | Pass | Exact consumer order and both-side availability asserted |
| Frozen warning order and league-rank suppression | Pass | Warning sort key is deterministic; suppression warning/context present |
| NaN, Infinity, below zero, above 100, bad signal, unsafe dates, window mismatch | Pass | Each maps to its frozen safe 409 reason |
| 404, 504 DeadlineExceeded, safe 500 | Pass | Exact reason codes; synthetic internal exception details absent |
| No write path | Pass | AST/source inspection finds no BigQuery mutation calls or SQL DML/DDL in the endpoint service or two endpoint query helpers |
| Zero-game/latest-game invariant | **Fail** | Defect E-D-001 below |
| Date-derived freshness lag | **Fail** | Defect E-D-002 below |

## Defect E-D-001 — zero games can retain a latest included game

### Frozen contract

Decision record §5.2 requires latest_included_game_id to be null when
games_in_window is zero and requires it when the count is positive. Decision
record §8 reserves 409 SOURCE_ALIGNMENT_CONFLICT for conflicting historical
window context.

### Narrow reproduction

Use an otherwise valid synthetic success state. For every exact source-aligned
away-team window row, set:

    games_in_window = 0
    latest_included_game_id = "20981228_AAA@CCC"

Call:

    build_matchup_lens_context("20990101_AAA@BBB")

### Expected

409 SOURCE_ALIGNMENT_CONFLICT, with all evidence fields cleared according to
the frozen unavailable envelope.

### Actual

200 available:true, with contradictory evidence:

    games_in_window = 0
    latest_included_game_id = "20981228_AAA@CCC"

The implementation checks positive count → non-null ID, but does not enforce
zero count → null ID.

## Defect E-D-002 — freshness lag trusts payload metadata instead of dates

### Frozen contract

Decision record §8 defines:

    team data_lag_days = max(as_of_date - source_data_date)
    basis.max_data_lag_days = max(team lag)

These are deterministic calendar-day calculations, not trusted passthrough
values.

### Narrow reproduction

Use an otherwise valid synthetic success state:

    as_of_date = 2098-12-29
    away source_data_date = 2098-12-28
    away helper data_lag_days = 7

Call:

    build_matchup_lens_context("20990101_AAA@BBB")

### Expected

200 available:true with:

    teams.away.data_lag_days = 1
    basis.max_data_lag_days = 1

### Actual

The response trusts the helper field:

    teams.away.data_lag_days = 7
    basis.max_data_lag_days = 7

The source dates remain safe, but the returned freshness is not the frozen
date-derived value.

## Commands and results

Dependency-complete local verification environment:

    Python 3.12.14
    pytest 8.4.2
    Flask 2.3.3
    google-api-core 2.25.1
    google-cloud-bigquery 3.38.0
    firebase-admin 7.1.0

Syntax command:

    python -m py_compile tests/test_matchup_lens_endpoint.py

Recorded result: passed.

Full command:

    python -m pytest -q tests/test_matchup_lens_endpoint.py tests/test_game_window_selection.py

Result:

    2 failed, 37 passed

The two failures are exactly E-D-001 and E-D-002. The 37 passes include 35 new
endpoint checks and both unchanged test_game_window_selection.py checks.

Endpoint checks excluding the two known reproductions:

    python -m pytest -q tests/test_matchup_lens_endpoint.py -k "not zero_games_requires_null_latest_included_game_id and not lag_is_derived_from_dates_instead_of_trusting_helper_value"

Recorded result: 35 passed, 2 deselected.

The unchanged relevant regression file:

    python -m pytest -q tests/test_game_window_selection.py

Recorded result: 2 passed.

## Live-validation status

**Blocked, not passed.** This execution environment has no approved live Google
Cloud/BigQuery credentials. No live DET/BUF request or query was attempted.
Therefore this handoff does not claim:

- live DET/BUF endpoint acceptance;
- live parameter/column-type compatibility;
- added-query latency or cost;
- deployed route behavior.

Chunk A's accepted live evidence remains provenance, but it is not relabeled as
a Chunk E live run.

## Residual risks

1. The two defects above require a narrowly authorized Chunk D repair and a
   rerun of the affected tests.
2. Live read-only validation remains blocked on approved credentials.
3. Query latency/cost for the league boundary read and exact alignment read is
   unmeasured here.
4. Runtime BigQuery column types were not independently exercised.

## Required next action

Authorize a narrow Chunk D repair for E-D-001 and E-D-002 only. Do not begin
Chunk F. After the repair is committed to this branch, rerun:

1. the two defect reproductions;
2. the full endpoint test file;
3. the unchanged window-selection regression file.

Chunk E can then append a repair-verification result in a separately authorized
follow-up. Until then the endpoint verification verdict remains **FAIL**.
