# LL-FIND-001 — Box-score snap totals: evidence and bounded diagnostic handoff

**Date:** 2026-10-14  
**Status:** Evidence documented; finding remains Open; no new waiver  
**Reviewed source:** `learning-lite@fa6e762eb1c5da5517e61167140de03a5ab4acdd`  
**Scope:** Inspect the user-supplied response, execute the existing pure parser locally, and document the next investigation. No application fix, provider request, cloud read/write, replay, rebuild, deployment, or LL-4 execution.

## Conclusion

The supplied `getNFLBoxScore` response directly contains the three team snap counts needed by the existing metrics. The current parser already reads these exact fields and produces the expected positive values for this sample. Implementing another parser or estimating snaps from plays is not justified by this evidence.

The initial positive example narrowed the investigation. The follow-up below now reproduces historical missing-field-to-zero conversion and records an existing parser correction. The remaining questions concern historical deployment, affected data scope, and downstream impact; no historical data has been repaired and LL-FIND-001 remains Open.

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

## Smallest next task

Trace one affected source game and one completed regular-season comparison game through: original saved response → existing parser → flat metrics → Facts → Windows → Rankings. Prefer existing saved responses and read-only metadata/SELECT access. A fresh provider response, if separately obtained, must remain labeled as a later observation rather than a replacement for what was originally ingested.

The affected August record now has a missing raw group and zero-valued flat rows, with a matching historical parser mechanism. Use that result rather than repeating the diagnosis. Bound current impact and any data-repair scope; distinguish unverified historical deployment from reproduced source behavior.

Provider semantics still needed: coverage by season phase, publication delay for snap counts, meaning of explicit zeroes, and whether request options affect inclusion. The public Tank01 site directs documentation to [its RapidAPI listing](https://rapidapi.com/tank01/api/tank01-nfl-live-in-game-real-time-statistics-nfl/playground). Field-level documentation was not retrievable in this review. No acquisition or UI change is evidence of a changed data contract. A later reviewer should use an actual provider excerpt when available.

## Copy-ready diagnostic prompt — no fixes

Task sizing: a smaller capable coding model is suitable for this constrained reproduction and comparison. Low/medium reasoning is a reasonable starting point if selectable; escalate only an unresolved semantics or lineage question. This is not a claim about a particular model's price or correctness. Use one agent.

```text
Investigate LL-FIND-001 snap totals in csells10/meow, branch learning-lite. This is read-only diagnosis and documentation, not implementation.

Read GameLens_Learning_Lite_Snap_Totals_Evidence_2026-10-14.md and the current status updates in GameLens_Learning_Lite_Findings_Log.md under documentation/learning_lite. Preserve accepted LL-1/LL-2/LL-3 and all valid snapshots. Check current branch/file revisions before relying on the recorded baseline.

Known evidence: the supplied 20241020_CAR@WSH response has body.teamStats.<side>.snapCounts.totalOffensive/totalDefensive/totalSpecialTeams. CAR=43/69/26, WSH=69/43/26; the existing parser already emits these values and total_snaps=138 for each team. Its seven existing parser tests passed at fa6e762. Do not implement a new parser, infer snaps from totalPlays, or aggregate player participation as team snaps.

The follow-up already identifies a saved August response without snapCounts, zero flat/Facts rows, and the historical parser's missing-to-zero behavior, corrected in ad5fec5 on August 14 after the saved artifact timestamp. Preserve that correction. Inspect only the relevant current parser/tests, Stats reconciliation, raw-response envelope, and downstream seams needed to bound remaining bad-data scope plus one regular-season comparison. The August scheduled PIT@BUF fixture was a consumer of earlier evidence; first identify its actual completed source game(s). Existing tables are Analytics.game_metrics_flat, Analytics.game_team_metric_facts_2026, Analytics.team_metrics_windowed_2026, and Analytics.team_metric_rankings_2026; verify live schemas and appropriate source year before constructing bounded SELECTs.

Use existing raw artifacts and read-only access. Inspect whether an artifact has body or body.body. Match game/team/metric/source dates. Separate what the original ingestion saw from any response retrieved later. Treat both-team completeness as a possible reason old observations were retained, not the cause of missing-to-zero conversion. Do not infer a deployed revision from commit time alone.

If cloud/artifact access is unavailable, return the exact minimal SELECT/metadata checks and specific raw file needed; do not guess results or keep expanding the audit. Do not import or run ingestion modules: their imports can create clients/read secrets. The pure parser and existing parser tests can run locally.

Return: (1) observed values at each inspected boundary, (2) first evidenced failure point or precise unknown, (3) provider semantics still unverified, (4) smallest proposed correction with exact candidate files/tests, and (5) whether historical data repair is a separate need. Explain separately whether affected metrics reach the accepted PIT-CLE snapshot's claim surfaces; say unknown if uninspected.

No provider requests, DML/DDL, table recreation, replay/backfill/rebuild, metric policy change, runtime/deployment change, or snapshot mutation. Do not mark LL-FIND-001 resolved or extend its expired waiver. Stop at the evidence and proposal.
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

LL-3 remains complete at its documented development scope. LL-4 readiness work can continue, but accepted real-data claim use still needs the finding's recorded scope decision or resolution. This note supplies a smaller diagnostic task; it does not activate any LL gate.
