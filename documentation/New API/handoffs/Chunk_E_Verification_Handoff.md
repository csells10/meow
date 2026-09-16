# Chunk E — Independent Endpoint Verification Handoff

**Status:** complete — PASS after contract amendment and narrow Chunk D repairs
**Verdict:** **PASS**
**Branch:** feature/matchup-lens-api
**Original verified implementation head:** 3e2dcef7f189dce1af2959541f27fb3c074b8701
**Final verified code/test head:** 108fc4109d6bb1596e4ad6dfadf525a41ed2dfd9
**Assigned model / effort:** gpt-5.6-sol / High
**Scope:** Chunk E endpoint-specific verification only

## Outcome

Chunk E now **passes** after the two original implementation defects were
repaired, the live BigQuery/Lovable mismatch was resolved through the
post-freeze Chunk C amendment and corresponding Chunk D repair, and the
E-owned tests were updated independently to the amended contract.

Final evidence:

- local automated verification: **42 passed, 8 subtests passed in 2.76s**;
- authenticated read-only live BigQuery builder smoke:
  **200 / available:true / reason:null**;
- live catalog / DET / BUF / shared counts: **63 / 62 / 63 / 62**;
- 16 known `context` metrics transported without entering readiness;
- production provenance strings are not serialized as `numerator` or
  `denominator`;
- deterministic response bytes, date-derived lag, exact source alignment,
  warning order, and league-rank suppression all passed.

The original failed reproductions and diagnosis remain below as historical
evidence of what Chunk E found. They are resolved and no longer control the
verdict.

No deployment, merge, frontend work, BigQuery write, scheduled job, learning
flow, or orchestrator flow was performed. Chunk F was not started.
## Starting-state confirmation

- The GitHub-connected branch head was exactly
  3e2dcef7f189dce1af2959541f27fb3c074b8701.
- The completed Chunk C contract decision record and handoff were present.
- The completed Chunk D implementation handoff was present.
- The handoff directory contained A, B, C, and D only; there was no later
  handoff to reconcile.
- The Chunk C decision record controlled all expected results.

## Changed paths

- Added and later amended `tests/test_matchup_lens_endpoint.py`.
- Added and finalized `documentation/New API/handoffs/Chunk_E_Verification_Handoff.md`.

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
| Zero-game/latest-game invariant | Pass after repair | Both contradictory directions map to `409 SOURCE_ALIGNMENT_CONFLICT` |
| Date-derived freshness lag | Pass after repair | Metric, team, and basis lag derive from validated dates |

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

## Original commands and results (pre-repair)

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

## Final repair verification

### Automated suite

Christian fast-forwarded the E test amendment commit
`108fc4109d6bb1596e4ad6dfadf525a41ed2dfd9` and ran:

    python -m pytest -q tests/test_matchup_lens_endpoint.py tests/test_game_window_selection.py

Result:

    42 passed, 8 subtests passed in 2.76s

The amended synthetic coverage additionally proves:

- production-shaped string provenance inputs are ignored and the removed
  `numerator`/`denominator` fields are absent from exact metric output;
- exact lowercase `context` is transported and included in catalog/team
  coverage while excluded from readiness denominators and missing lists;
- null, unknown, and differently cased signals fail safely;
- all original E-D-001 and E-D-002 reproductions now pass.

### Live BigQuery smoke

**PASS.** Christian ran the repaired builder from the local authenticated
`nfl` virtual environment with production runtime configuration against
`20260917_DET@BUF`. The smoke was read-only.

Observed result:

    Status: 200
    Available: True
    Reason: None
    catalog / DET / BUF / shared: 63 / 62 / 63 / 62
    context metrics transported: 16
    warning codes:
      ASYMMETRIC_LENS_EVIDENCE
      LEAGUE_RANK_OUTPUT_SUPPRESSED
      PARTIAL_LENS_EVIDENCE
    BIGQUERY MATCHUP LENS SMOKE PASS

The smoke executed the checked-in builder twice and asserted deterministic,
newline-terminated JSON; the accepted snapshot/source dates
`2026-09-14`/`2026-09-13`; exact DET/BUF source-aligned latest games;
date-derived lag; omission of `numerator` and `denominator`; full dynamic
coverage; and exclusion of known `context` metrics from readiness missing
lists. No BigQuery write was performed.

## Residual risks

1. Query latency and billed bytes for the additional boundary and exact
   alignment reads were not separately benchmarked by this smoke.
2. The authenticated deployed Cloud Run route has not been exercised because
   deployment is outside Chunk E.
3. The Lovable adapter and its no-static-fallback behavior remain Chunk F work.

## Required next action

Chunk E is closed with a **PASS** verdict. Stop here and wait for explicit
authorization before Chunk F. Do not deploy, merge, contact Lovable for
implementation, or begin frontend adapter work under this handoff.
