# LL-FIND-001 — Box-score snap totals: evidence and bounded diagnostic handoff

**Date:** 2026-10-14  
**Status:** Evidence documented; finding remains Open; no new waiver  
**Reviewed source:** `learning-lite@fa6e762eb1c5da5517e61167140de03a5ab4acdd`  
**Scope:** Initial local parser review, followed by Christian's read-only BigQuery/GCS checks and one fresh provider comparison. Documentation only in the repository; no application fix, cloud data write, replay, rebuild, deployment, or LL-4 execution.

## Conclusion

The supplied `getNFLBoxScore` response directly contains the three team snap counts needed by the existing metrics. The current parser already reads these exact fields and produces the expected positive values for this sample. Implementing another parser or estimating snaps from plays is not justified by this evidence.

The initial positive example narrowed the investigation. The follow-up below now reproduces historical missing-field-to-zero conversion and records an existing parser correction. The later comparison below also establishes that a 2026 regular-season response acquired team snap fields after its original saved observation. Investigation is paused at this documented stopping point; no historical data has been repaired and LL-FIND-001 remains Open.

LL-1, LL-2, and LL-3 retain their existing acceptance. In particular, the authentic PIT–CLE LL-3 snapshot remains immutable.

## Supplied evidence

- Original attachment: `Pasted text(20261014-191135).txt`, 163,560 bytes.
- Original-file SHA-256: `9ce1671643532146d071c7e160b04347325ac882d59b36b8d242a5d0e9bc4db9`.
- Envelope: `statusCode: 200`, with a `body` object.
- Game: `20241020_CAR@WSH`; `gameDate: 20241020`; Regular Season, Week 7; `gameStatus: Completed`.
- This is a historical 2024 example supplied on October 14, 2026. Its original retrieval time and request parameters were not supplied.
- The full attachment remains outside the repository; this note retains only the relevant observed values. It contains no API credentials.

### Direct team fields and existing metric mapping

Use `body.teamStats.away` for CAR and `body.teamStats.home` for WSH in this sample.

| Source below `body.teamStats.<side>.snapCounts` | Existing metric | CAR | WSH |
|---|---|---:|---:|
| `totalOffensive` | `total_offensive_snaps` | 43 | 69 |
| `totalDefensive` | `total_defensive_snaps` | 69 | 43 |
| `totalSpecialTeams` | `total_special_teams_snaps` | 26 | 26 |
| Sum of all three, calculated locally | `total_snaps` | 138 | 138 |

The provider supplies these values as numeric strings. `total_snaps` is the repository's derived sum, not an additional field supplied in this response. The two teams' offensive/defensive counts cross-match in this example.

### Distinctions to preserve

- The response also contains 96 player records with `snapCounts`. Individual participation counts must not be summed as though they were distinct team plays; use the explicit team totals.
- `totalPlays` is not interchangeable with `totalOffensive`: WSH has `totalPlays: 63` and `totalOffensive: 69` in this very response. The reason for that difference was not established here; do not invent a reconciliation formula.
- CAR's supplied player example has `offSnap: 40`, `offSnapPct: 0.93`; the percentage representation is fractional in this sample. Player percentage conversion is outside this task.
- The pasted response includes play-by-play and fantasy data. The production-oriented ingestion source requests `playByPlay=false` and `fantasyPoints=false`. Do not assume parameter parity or change those flags without evidence about the same endpoint's behavior.
- This positive historical regular-season sample does not prove snap availability for preseason, every game, immediately after final, or the current season.

## Existing implementation and observed verification

Reviewed files:

- [`parse_nfl_stats.py`](../../api_calls/api_utils/parse_nfl_stats.py): `parse_game_stats`, `_optional_float`, and the `snapCounts` block.
- [`test_parse_nfl_stats.py`](../../tests/api_calls/test_parse_nfl_stats.py): existing optional-field, missing-group, partial-group, and historical snap-count cases.
- [`api_call_nfl_stats.py`](../../api_calls/api_call_nfl_stats.py): endpoint flags, raw-response saving, and per-game reconciliation.
- [`helper.py`](../../utils/helper.py): `fetch_and_validate_api_data` returns the complete envelope, not only its inner body.
- [`response_helpers.py`](../../utils/response_helpers.py): raw JSON persistence shape.

Verification used unmodified source downloaded at the reviewed commit. Only the pure parser and its existing test module ran; ingestion modules were read as text, not imported or executed.

1. Parsed the full supplied attachment in memory.
2. Confirmed all eight team/count outputs in the table above.
3. Observed derived shares: CAR offense `0.312`, defense `0.500`, special teams `0.188`; WSH offense `0.500`, defense `0.312`, special teams `0.188`.
4. Confirmed the input object was unchanged.
5. Ran the existing parser suite: **7 tests passed**.

The parser omits absent/non-numeric/non-finite snap fields, emits independently available counts, and only derives `total_snaps` when all three component values are available. It deliberately preserves explicit numeric zeroes. Finite negative/fractional values are not rejected by `_optional_float`; this is an unvalidated domain edge, not an observed cause in the uploaded sample. Do not expand the fix scope on that basis alone.

## Concrete leads, not a root-cause conclusion

| Lead | What source review establishes | Evidence still needed |
|---|---|---|
| Availability or timing | This supplied 2024 regular-season response has positive team totals | Original stored response for an affected source game; retrieval timing, season phase, and effective request parameters |
| Legacy stored observations | Current ingestion treats a game as complete when both expected team IDs have any stored metric rows | Whether an affected game was previously loaded without valid snap values and subsequently skipped |
| Explicit provider zeroes | Current parser preserves supplied zeroes | Whether the original response actually supplied zeroes, omitted fields, or was processed by a different parser revision |
| Downstream propagation | The historical finding observed zero-valued Facts/Windows/Rankings | Matching flat input rows and transformations for the same team, game, metric, and source dates |

`reconcile_game_rows` is not a missing-metric repair command. When both expected teams are present, it returns without inserting the freshly parsed rows. That can explain why a later richer response would not automatically enrich an already-loaded game, but it is not proof that this caused LL-FIND-001. Do not replay ingestion or toggle completeness flags to test this.

Raw-artifact inspection must account for the current call `save_raw_response({"body": box}, ...)`, where `box` already contains the provider envelope. An artifact saved through that path can place the actual game object under `body.body`; the uploaded response places it under `body`. Inspect the shape explicitly before comparing. This wrapper alone does not establish a snap-count bug, and normalization is not authorized here.

The August finding's scheduled fixture `20260827_PIT@BUF` was the consumer of earlier team evidence. Determine the actual completed source game(s) behind those Facts; do not assume the upcoming fixture itself supplied the defective snap counts.

## Follow-up evidence — October 14, affected August record

**Current assessment:** Historical missing-field-to-zero conversion is reproduced in source. The affected stored data remains unrepaired; LL-FIND-001 remains Open. Exact historical deployment and total affected population remain unverified.

Christian ran bounded read-only BigQuery and GCS checks and supplied these results:

- Facts for PIT/BUF, August 1–23: PIT `20260813_GB@PIT` has all four snap metrics at zero and `total_plays=64`; BUF `20260815_CAR@BUF`, PIT `20260821_NYJ@PIT`, and BUF `20260822_BUF@CLE` have total-play rows but no selected snap-metric rows. Each returned metric/value group has one row.
- Flat metrics for `20260813_GB@PIT`: both GB (team ID `12`) and PIT (`26`) have all four snap metrics at zero, with one row per returned metric/value group. Total plays are GB `59`, PIT `64`. Therefore the zeroes are already present before Facts processing.
- Raw artifact: `gs://xtra_point/nfl_boxscore_20260813_GB@PIT.json`.
- Generation: `1786708887408844`; SHA-256: `4218cb02b2fff0465b01b65b09c64a6a01c6376e00dbc03f4baa9d79c956fe2e`.
- Created and updated: `2026-08-14T12:01:27.468000Z`.
- Actual game object: `$.body.body`; game identity matches; status `Completed`; total plays GB `59`, PIT `64`.
- Both teams have `snapCounts_present=false`. The printed null is the inspection script's absent-key result, not evidence of an explicit provider JSON null.

GitHub history identifies the preceding parser revision at `7bec0d294c31a9cf0674ef4b00a0ee20793f532b`. It used `t_stats.get("snapCounts", {})` and `.get(field, 0)`, then unconditionally emitted all four snap metrics. Commit [`ad5fec518642c85bd2e56c0ad2614ad70c6e78e6`](https://github.com/csells10/meow/commit/ad5fec518642c85bd2e56c0ad2614ad70c6e78e6), timestamp `2026-08-14T20:00:59Z`, removed that absent-to-zero behavior. That correction is already in the reviewed current code.

A local controlled comparison used a copy of the supplied 2024 CAR–WSH response with both team `snapCounts` groups removed. The historical parser emitted eight zero-valued snap rows; the current parser emitted zero snap rows. This reproduces the mechanism; it is not a replay of the full August raw artifact, which was inspected by Christian rather than downloaded into this review environment.

The raw timestamp, stored rows, and historical behavior strongly support legacy zeroes introduced by the old parser. Git history alone does not prove which revision Cloud Run executed. Current raw-object metadata is provenance evidence, not a complete ingestion/deployment audit.

**Changed next step:** Preserve the existing parser correction. Bound the remaining affected population by season phase and source date, check at least one completed regular-season comparison, and determine downstream/claim impact. Then prepare a narrowly scoped historical-data repair proposal if warranted. Do not default to new parser code, broad source redesign, or replay. Explicit provider zero handling remains a distinct unresolved domain policy; this case demonstrates missing fields, not provider-supplied zeroes.

These new observations supersede the initial uncertainty about whether a missing-field-to-zero implementation existed. They do not resolve all original finding criteria, repair data, waive later-checkpoint use, or alter valid frozen captures.

## Follow-up evidence — regular-season coverage and later availability

Christian supplied the following read-only query results on October 14, 2026. These are observed results, not independently executed cloud queries by the reviewer.

| Facts coverage among team-games with total_plays | Preseason | Regular Season |
|---|---:|---:|
| Distinct games | 49 | 79 |
| Team-game groups | 98 | 158 |
| No snap metrics | 84 | 158 |
| One to three snap metrics | 0 | 0 |
| All four snap metrics | 14 | 0 |
| First game date | 2026-08-06 | 2026-09-09 |
| Latest game date | 2026-08-29 | 2026-10-12 |

A separate aggregation over the four snap metrics returned only Preseason: each metric had 14 rows, all zero, spanning seven games dated August 6–13; no positive, negative, or null rows were returned. Thus 56 stored snap metric rows were zero. Regular-season absence is missing coverage, not proof of healthy snap values. The denominator is existing Facts team-games with total_plays, not an independent schedule-completeness audit.

### Same-game comparison: 20261001_PIT@CLE

Original saved response:

- Object: `gs://xtra_point/nfl_boxscore_20261001_PIT@CLE.json`.
- Created/updated: `2026-10-02T12:00:42.781000Z`; generation `1790942442728566`.
- SHA-256: `8137a0ea46a3b73346e133a59792970b07a05ad426303fa24a827aba2c60f464`.
- Game object at `$.body.body`; matching game ID, status Completed.
- Both teams lack the snapCounts key; PIT/CLE totalPlays are 64/62. Printed nulls are absent-key inspection results.

Fresh response requested by Christian on October 14 with gameID=20261001_PIT@CLE, playByPlay=false, fantasyPoints=false:

| Team | totalOffensive | totalDefensive | totalSpecialTeams |
|---|---:|---:|---:|
| PIT | 72 | 64 | 28 |
| CLE | 64 | 72 | 28 |

Both snapCounts keys are present, with numeric-string values; game identity matches and status is Completed. Christian saved the response outside the repository as `../ll3-evidence/current-boxscore-20261001-PIT-CLE.json`. The fresh file's hash and exact retrieval timestamp were not supplied. No credential or full response is stored in this note.

This establishes later availability for this game. The fresh request uses the flags in reviewed ingestion source; exact historical wire parameters were not independently captured. We do not know when the provider added the fields, whether publication delay is consistent across games, or whether the change was delayed enrichment versus another provider update. Do not infer a twelve-day publication delay from the interval between our observations.

The existing parser supports these fields. The reviewed ingestion completeness check skips insertion when both teams already have rows, so later available fields are not automatically added to an already-loaded game through that path. This explains a concrete persistence mechanism; it does not establish every historical execution or prove all 158 missing team-games now have provider values.

### Usefulness and stopping decision

Snap totals remain potentially useful for later matchup context and postgame analysis once actually available. They are not dependable inputs to the first postgame run on this evidence. These checks establish availability behavior, not predictive value or a reason to make snap metrics a prerequisite for Learning Lite.

Preserve as-of availability: never enrich the accepted pregame snapshot retrospectively or treat a later observation as evidence known at capture time. Missing values must remain distinguishable from true zero. Existing totalPlays values are not a snap-count substitute.

Christian approved stopping this investigation and documenting the evidence. Keep LL-FIND-001 Open, with no new waiver. Preserve the completed parser correction and LL-1/LL-2/LL-3 acceptance. Resume the previously recommended read-only LL-4 readiness inventory; any accepted real-snapshot claim use still requires its explicit scoped-use decision or resolution. No new recovery or data repair is authorized.

## Deferred proposal prompt — only when snap work is resumed

This supersedes the earlier diagnostic prompt to repeat August tracing and find a regular-season comparison. Those checks now have evidence above. A smaller capable coding model can prepare this bounded proposal; use one agent and verify the current revision.

```text
Prepare a proposal only for LL-FIND-001 in csells10/meow, learning-lite.
Read this snap-total evidence note and the latest Findings Log updates.
Preserve accepted LL-1/LL-2/LL-3, immutable snapshots, and the existing
missing-field parser correction. Do not repeat completed diagnosis.

Known: original October 2 PIT-CLE raw response lacks team snapCounts;
an October 14 request with playByPlay=false/fantasyPoints=false contains
PIT 72/64/28 and CLE 64/72/28. Facts has no snap metrics for 158 measured
regular-season team-games. Historical preseason zeros are a separate
legacy-data issue. The existing both-team completeness check can prevent
later metrics being inserted.

Review current code and propose the smallest development-only approach
to adding missing snap metrics from a later response for this one game.
Specify candidate files, provenance/as-of timestamps, eligible metric
keys, duplicate protection, identical-retry behavior, conflict handling,
and preservation of existing values and unrelated metrics. Do not assume
a source correction may overwrite a stored value. Keep legacy-zero repair
separate: filling absent rows does not correct existing zeros.
Explain how invalid/missing metrics stay out of rank/tier/edge evidence,
and identify any remaining downstream scope decision.

Give focused tests and a code-only stopping point for a later authorized
implementation. Do not assume predictive benefit, a fixed publication
delay, all-game coverage, or that automatic refresh is worth its cost.
No provider requests, ingestion execution, code edits, DML/DDL, replay,
backfill, rebuild, deployment, snapshot mutation, waiver, or closure.
Return the bounded proposal and unresolved decisions, then stop.
```

## Later implementation handoff — prepare only after diagnosis

Do not issue a generic 'fix snap totals' command. Once a concrete correction is accepted, prepare a separate prompt containing:

1. The evidenced failure point and exact source revision.
2. The approved missing/zero/positive semantics and explicitly allowed files.
3. A minimal fixture reproducing that failure plus positive, missing, partial, and relevant invalid-value cases.
4. Expected parsed and downstream results, with unrelated metrics preserved.
5. Focused test commands and a code-only stopping point.
6. A separately reviewed historical-data repair plan if needed; no replay or rebuild implicit in a code fix.

Acceptance requires the Findings Log's resolution criteria, including valid positive values flowing and invalid/missing values not receiving misleading rank/tier/edge treatment. Passing this one positive fixture is useful evidence, but insufficient for full closure.

## Effect on Learning Lite

LL-3 remains complete at its documented development scope. LL-4 readiness work can continue, but accepted real-data claim use still needs the finding's recorded scope decision or resolution. The snap investigation is paused with a deferred proposal prompt; this note does not activate any LL gate.
