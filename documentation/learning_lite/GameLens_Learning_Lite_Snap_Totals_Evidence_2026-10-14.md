# LL-FIND-001 — Box-score snap totals: evidence and bounded diagnostic handoff

**Date:** 2026-10-14  
**Status:** Evidence documented; finding remains Open; no new waiver  
**Reviewed source:** `learning-lite@fa6e762eb1c5da5517e61167140de03a5ab4acdd`  
**Scope:** Inspect the user-supplied response, execute the existing pure parser locally, and document the next investigation. No application fix, provider request, cloud read/write, replay, rebuild, deployment, or LL-4 execution.

## Conclusion

The supplied `getNFLBoxScore` response directly contains the three team snap counts needed by the existing metrics. The current parser already reads these exact fields and produces the expected positive values for this sample. Implementing another parser or estimating snaps from plays is not justified by this evidence.

The remaining question is why the affected stored observations contain zeroes or missing values. This example narrows that investigation; it does not establish the cause, repair historical data, prove current-season coverage, or resolve LL-FIND-001.

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

## Smallest next task

Trace one affected source game and one completed regular-season comparison game through: original saved response → existing parser → flat metrics → Facts → Windows → Rankings. Prefer existing saved responses and read-only metadata/SELECT access. A fresh provider response, if separately obtained, must remain labeled as a later observation rather than a replacement for what was originally ingested.

First establish where the value becomes absent or zero. Stop there with a correction proposal, or state that the original evidence is unavailable. A missing original response is a real limit; do not fill that gap by guessing.

Provider semantics still needed: coverage by season phase, publication delay for snap counts, meaning of explicit zeroes, and whether request options affect inclusion. The public Tank01 site directs documentation to [its RapidAPI listing](https://rapidapi.com/tank01/api/tank01-nfl-live-in-game-real-time-statistics-nfl/playground). Field-level documentation was not retrievable in this review. No acquisition or UI change is evidence of a changed data contract. A later reviewer should use an actual provider excerpt when available.

## Copy-ready diagnostic prompt — no fixes

Task sizing: a smaller capable coding model is suitable for this constrained reproduction and comparison. Low/medium reasoning is a reasonable starting point if selectable; escalate only an unresolved semantics or lineage question. This is not a claim about a particular model's price or correctness. Use one agent.

```text
Investigate LL-FIND-001 snap totals in csells10/meow, branch learning-lite. This is read-only diagnosis and documentation, not implementation.

Read GameLens_Learning_Lite_Snap_Totals_Evidence_2026-10-14.md and the current status updates in GameLens_Learning_Lite_Findings_Log.md under documentation/learning_lite. Preserve accepted LL-1/LL-2/LL-3 and all valid snapshots. Check current branch/file revisions before relying on the recorded baseline.

Known evidence: the supplied 20241020_CAR@WSH response has body.teamStats.<side>.snapCounts.totalOffensive/totalDefensive/totalSpecialTeams. CAR=43/69/26, WSH=69/43/26; the existing parser already emits these values and total_snaps=138 for each team. Its seven existing parser tests passed at fa6e762. Do not implement a new parser, infer snaps from totalPlays, or aggregate player participation as team snaps.

Inspect only the current parser/tests, Stats ingestion and raw-response envelope, and the transformation seams needed to trace one affected completed source game plus one regular-season comparison. The August scheduled PIT@BUF fixture was a consumer of earlier evidence; first identify its actual completed source game(s). Existing tables are Analytics.game_metrics_flat, Analytics.game_team_metric_facts_2026, Analytics.team_metrics_windowed_2026, and Analytics.team_metric_rankings_2026; verify live schemas and appropriate source year before constructing bounded SELECTs.

Use existing raw artifacts and read-only access. Inspect whether an artifact has body or body.body. Match game/team/metric/source dates. Separate what the original ingestion saw from any response retrieved later. Check whether both-team completeness caused ingestion to skip missing metric enrichment. Do not assume that mechanism caused the finding.

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
