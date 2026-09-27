# Chunk C — Matchup Lens Contract Freeze Handoff

**Status:** complete; post-freeze documentation amendment PASS; implementation/live validation blocked  
**Branch:** `feature/matchup-lens-api`  
**Starting commit:** `08192f10a2cfd07d2b3b711f6265607e60ef70b9`  
**Assigned model / effort:** `gpt-5.6-sol` / High  
**Amendment starting commit:** `6fc17e0c378c64db0d2ffe823b307e86e35f2de8`  
**Scope:** Chunk C post-freeze documentation amendment only; no Chunk D implementation

## Outcome

Chunk C is **complete**, including this post-freeze documentation amendment. The controlling record now aligns `matchup_lens_v1` with verified live BigQuery and Lovable evidence: `context` is valid transport evidence but not scoring/readiness evidence, and production provenance strings `numerator`/`denominator` are not response fields and must not be coerced.

No application code, existing test, service, route, query, pipeline, schema, table, scheduled job, learning system, orchestrator state, Endpoint Plan, or Product Roadmap was changed. No deployment, merge, Lovable contact, or BigQuery write occurred.

## Controlling record

The frozen implementation contract is:

`documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`

If this handoff and the detailed record appear to differ, the detailed record controls.

## Accepted Chunk A, Chunk B, and post-freeze live evidence

| Evidence | Accepted result |
|---|---|
| Plan sample / registry / code eligible / live ranking | 59 / 73 / 70 / 63 |
| BUF / DET / shared metric counts | 63 / 62 / 62 |
| DET missing metric | `fourth_down_pct` |
| Ranking date / source-data date | `2026-09-14` / `2026-09-13` |
| Source-aligned games | 1 per team |
| Latest included games | BUF `20260913_BUF@HOU`; DET `20260913_NO@DET` |
| Selected duplicate groups | 0 at the accepted snapshot; not a runtime guarantee |
| Live tag vocabulary | 90; all current frontend-required tags present |
| Frontend types | Exact `LensSnapshot`, `TeamMetricRow`, and `MetricDefinition` known |
| Six lenses | Exact order, ANY-tag rules, exclusions, 2/1 weights, and 0.5/0.75 modifiers known |
| Missing/null scoring | Skip; renormalize; one numeric eligible metric is enough |
| Percentiles | Upstream polarity-corrected 0–100 |
| API/frontend seam | Existing Firebase client, game-ID key, static source hazards, and adapter location known |
| Rank blocker | Two response teams would produce false ranks out of two |
| Live smoke outcome | `409 INVALID_METRIC_EVIDENCE`; failed, not passed |
| Live aligned dates | Snapshot `2026-09-14`; source `2026-09-13` |
| Live aligned inventory | BUF 63 / DET 62 / shared 62; dynamic, not fixed denominators |
| Live consistency | Raw/helper metric keys match; no catalog definition conflicts |
| Valid transport signals | `strong`, `supporting`, `context` |
| Live provenance fields | `numerator`/`denominator` are strings unused by Lovable |

The original A/B discovery was not rerun. The post-freeze rows above are separately verified live evidence supplied after the `409` smoke failure; the failure is not relabeled as a passing endpoint run.

## Frozen decisions and rationale

### State policy

- `200 available:true` for complete or partial-lens evidence.
- `200 available:false` for expected absence, one missing team, or missing exact historical alignment.
- `400` invalid ID; `404` unknown game.
- `409` unsafe dates, window mismatch, source-alignment conflict, raw duplicate rows, or invalid metric evidence.
- Existing Firebase `401`/`403` bodies remain unchanged.
- `504` maps an upstream deadline; `500` is a safe unexpected failure.

Expected absence is not an HTTP conflict. Unsafe or malformed evidence is never returned as usable merely because the browser can parse it. Every non-auth unavailable body clears display, basis, catalog, team evidence, and coverage; only the state matrix's known-header states retain canonical `game` context.

### Exact v1 envelope

The response always contains the frozen top-level fields in order:

1. `schema_version`
2. `available`
3. `reason`
4. `game`
5. `display`
6. `basis`
7. `metric_catalog`
8. `teams`
9. `coverage`
10. `league_context`
11. `method`

All keys are required; nullable fields are explicit. Available responses require the canonical game, both teams, basis, display, and coverage. Unavailable responses expose no metric evidence. Dates are ISO `YYYY-MM-DD`; JSON numbers must be finite; ordering and compact UTF-8 serialization are deterministic.

### Dynamic catalog and coverage

The catalog is the sorted distinct league ranking metric set at the exact selected `as_of_date` and `window_type`. It is not a fixed 59, 63, 70, or 73 list.

Away/home counts are distinct validated keys for each canonical team. Shared is the set intersection. Missing lists are runtime catalog minus each team's key set. All are computed after raw duplicate validation. These top-level catalog/team counts include valid `context` evidence and preserve the live 63/62/62 inventory semantics without creating a fixed denominator.

### Source alignment

`games_in_window` and `latest_included_game_id` must match canonical team, metric, season, selected window, and each ranking row's exact `source_data_date`. Missing exact history returns `SOURCE_ALIGNMENT_UNAVAILABLE`; duplicate or disagreeing rows return `SOURCE_ALIGNMENT_CONFLICT`. A newer pregame row may never be attached to older ranking evidence.

### Duplicate guarantee

One additional raw-boundary ranking read is required. The helper's metric dictionaries do not prove source uniqueness. D may not weaken this to a dictionary count or rely only on Chunk A's one-time clean observation.

### Both teams versus partial lenses

Top-level availability requires both scheduled teams. Once both teams are represented and safety checks pass, a missing/null lens input affects only readiness. It does not make the whole matchup unavailable.

### Six-lens ownership

The frontend retains all numeric scoring and language. The backend returns only readiness metadata derived from the exact current include/exclude tags plus signal eligibility. A metric is readiness-eligible only when its tags qualify and its signal is `strong` or `supporting`; `context` is excluded from lens denominators and lens missing lists. No score or weighting formula is reimplemented by the backend.

DET's absent `fourth_down_pct` makes its Drive Control readiness partial, BUF complete, and the comparison partial. The existing frontend score remains calculable from remaining metrics and retains its renormalization; the asymmetry must be visibly warned instead of remaining silent.

### Numeric validation

- Null percentile is valid and skipped by the adapter.
- NaN, Infinity, below 0, or above 100 fails with `INVALID_METRIC_EVIDENCE`.
- Raw `value` may be any finite number or null.
- `signal_strength` must be exactly `strong`, `supporting`, or `context`; null, unknown, and differently cased values remain `409 INVALID_METRIC_EVIDENCE`.
- `numerator` and `denominator` are removed from `MatchupLensMetric`, its exact field order, and numeric validation. Their production values are provenance strings unused by Lovable; do not coerce them.
- Counts/ranks obey the frozen integer bounds; `allow_nan=False` is mandatory.

### League-standing decision

The selected v1 solution is to return only the two canonical teams and suppress League Standing and trace ordinal/rank output. The alternatives were evaluated:

- League-wide percentile context preserves ranks but materially broadens data access, freshness semantics, payload, and scope.
- Precomputed standing metadata would duplicate frontend lens formulas or create a new ranking contract.
- Targeted suppression is the smallest honest solution and never shows “out of 2.”

Non-rank trace evidence, scores, cards, tabs, constellation, Biggest Edge, observations, comparison language, and `view` behavior remain unchanged.

### Collision and Turnover Watch

Their 12 hardcoded metric-name consumers remain a separate `named_metric_coverage` block. They do not alter six-lens readiness, and the backend does not invent a feature score or new grouping. The endpoint still returns every dynamic ranking metric, not only those 12.

### Later frontend expectations

The record freezes loading, invalid/unknown, `available:false`, partial, 401, 403, 504, retry, query-key, persistence, no-static-fallback, no-preseason-flash, and no-previous-game-placeholder behavior. The later adapter creates definitions and percentiles only for `strong`/`supporting`, omits `context`, never coerces signals, and sends no numeric weights; existing frontend weights/formulas remain frontend-owned. Client AbortSignal support is deferred; backend upstream timeout mapping is included.

## Synthetic fixture coverage

The decision record labels every fixture as synthetic and defines:

- complete success;
- partial Drive Control with one asymmetric missing metric;
- expected no evidence;
- missing home team;
- missing exact source alignment;
- invalid game ID;
- unknown game;
- unsafe dates;
- window mismatch;
- source-alignment conflict;
- duplicate ranking rows;
- null/NaN/Infinity/below-range/above-range percentile behavior;
- valid `context` transport with readiness/adapter exclusion;
- null, unknown, and differently cased invalid signal strength;
- 401/403 authentication/authorization states;
- upstream timeout;
- unexpected server failure.

Chunk E owns the complete schema-valid persisted fixtures. Documentation snippets are clearly marked as synthetic and, where abbreviated, are not represented as production-valid payloads.

## Exact D/E file ownership

### D may add/edit

- Add `services/matchup_lens_service.py`.
- Append isolated endpoint reads to `queries/game_queries.py`; do not alter existing functions.
- Add the authenticated nested handler to `routes/game_routes.py`; do not change existing `/game` behavior.
- Add `documentation/New API/handoffs/Chunk_D_Backend_Implementation_Handoff.md`.

D must not touch `app.py`, existing services beyond the listed new file, tests, fixtures, runtime configuration, dependencies, pipelines, schemas, tables, jobs, learning/orchestrator code, or controlling documentation. D owns no persisted tests.

### E may add only

- `tests/test_matchup_lens_endpoint.py`.
- Optional complete synthetic files under `tests/fixtures/matchup_lens/`.
- `documentation/New API/handoffs/Chunk_E_Verification_Handoff.md`.

E must not edit implementation or existing tests. It returns defects to D and verifies repairs.

## Downstream status and obligations

This amendment resolves the contract mismatch but does not repair the endpoint.

1. Chunk D must implement the amended signal validation, readiness filtering, response field order, and omission of `numerator`/`denominator`.
2. Chunk D must preserve the full dynamic catalog and top-level/team coverage for `context` evidence and must not change league-context suppression or any unrelated frozen choice.
3. Chunk E remains blocked until the D repair is committed and a live read-only BigQuery smoke request succeeds.
4. Chunk E must verify valid `context`, all three invalid-signal classes, unchanged 63/62/62-style dynamic coverage behavior, adapter obligations, and the absence of the two removed response fields.
5. No endpoint, BigQuery, deployment, or frontend PASS is claimed by this documentation-only amendment.

## Files changed

- Updated `documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`.
- Updated `documentation/New API/handoffs/Chunk_C_Contract_Freeze_Handoff.md`.

No other file is part of this amendment.

## Validation performed

- Confirmed the branch head was exactly `6fc17e0c378c64db0d2ffe823b307e86e35f2de8` before writing.
- Read both authorized files completely.
- Consulted only the directly relevant Chunk B signal/adapter sections and Chunk E live-validation status.
- Reconciled the amended schema, exact metric field order, validation rules, catalog coverage, lens readiness, adapter behavior, and downstream ownership.
- Preserved the live snapshot/source dates, BUF/DET/shared 63/62/62 inventory, raw/helper key agreement, and absence of definition conflicts.
- Preserved league-context suppression and all unrelated frozen decisions.
- Confirmed this is a documentation-contract PASS only. The live smoke remains failed with `409`; the endpoint and BigQuery validation are not marked passed.

## Amendment verdict

**PASS — documentation amendment only.** Chunk D repair is required. Chunk E remains blocked until the repair and a successful live read-only BigQuery smoke validation.

## Ready-to-copy prompt for the narrow Chunk D repair

```text
Continue GameLens on branch `feature/matchup-lens-api` in `csells10/meow`.

Execute only the post-freeze Chunk D repair defined in the amended controlling contract and Chunk C handoff. Do not begin Chunk E, deployment, frontend work, or unrelated cleanup.

Implement these exact corrections:

- Accept only `strong | supporting | context` as transported signal strengths; null, unknown, or differently cased values remain `409 INVALID_METRIC_EVIDENCE`.
- Keep `context` in the dynamic catalog, team metric maps, and top-level/team coverage, but exclude it from lens readiness denominators and missing lists.
- Remove `numerator` and `denominator` from `MatchupLensMetric` serialization, field order, and numeric validation. They are production provenance strings unused by Lovable; do not coerce them.
- Preserve all other fields, existing frontend-owned weights/formulas, and league-context suppression.

Add or update only the D-owned implementation/handoff paths required for this repair. Run the relevant safe tests, inspect the complete diff, commit and push, then stop. Chunk E must independently verify the repair and a live read-only BigQuery smoke request must succeed before E can pass.
```
