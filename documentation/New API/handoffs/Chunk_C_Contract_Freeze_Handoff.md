# Chunk C — Matchup Lens Contract Freeze Handoff

**Status:** complete  
**Branch:** `feature/matchup-lens-api`  
**Starting commit:** `08192f10a2cfd07d2b3b711f6265607e60ef70b9`  
**Assigned model / effort:** `gpt-5.6-sol` / High  
**Scope:** Chunk C only — documentation contract freeze; no implementation

## Outcome

Chunk C is **complete**. The separate contract decision record freezes `matchup_lens_v1`, every required HTTP state, runtime catalog and coverage semantics, source-date alignment, raw duplicate protection, lens readiness, numeric validation, display-field derivation, the two-team rank safeguard, named metric coverage, synthetic fixture requirements, and exact D/E file ownership.

No application code, existing test, service, route, query, pipeline, schema, table, scheduled job, learning system, orchestrator state, Endpoint Plan, or Product Roadmap was changed. No deployment, merge, Lovable contact, or BigQuery write occurred.

## Controlling record

The frozen implementation contract is:

`documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`

If this handoff and the detailed record appear to differ, the detailed record controls.

## Accepted Chunk A and Chunk B evidence

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

No discovery result was rerun. No unresolved A/B fact was filled by pretending it had been observed.

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

Away/home counts are distinct validated keys for each canonical team. Shared is the set intersection. Missing lists are runtime catalog minus each team's key set. All are computed after raw duplicate validation.

### Source alignment

`games_in_window` and `latest_included_game_id` must match canonical team, metric, season, selected window, and each ranking row's exact `source_data_date`. Missing exact history returns `SOURCE_ALIGNMENT_UNAVAILABLE`; duplicate or disagreeing rows return `SOURCE_ALIGNMENT_CONFLICT`. A newer pregame row may never be attached to older ranking evidence.

### Duplicate guarantee

One additional raw-boundary ranking read is required. The helper's metric dictionaries do not prove source uniqueness. D may not weaken this to a dictionary count or rely only on Chunk A's one-time clean observation.

### Both teams versus partial lenses

Top-level availability requires both scheduled teams. Once both teams are represented and safety checks pass, a missing/null lens input affects only readiness. It does not make the whole matchup unavailable.

### Six-lens ownership

The frontend retains all numeric scoring and language. The backend returns only readiness metadata derived from the exact current include/exclude tags. No score or weighting formula is reimplemented by the backend.

DET's absent `fourth_down_pct` makes its Drive Control readiness partial, BUF complete, and the comparison partial. The existing frontend score remains calculable from remaining metrics and retains its renormalization; the asymmetry must be visibly warned instead of remaining silent.

### Numeric validation

- Null percentile is valid and skipped by the adapter.
- NaN, Infinity, below 0, or above 100 fails with `INVALID_METRIC_EVIDENCE`.
- Raw values may be any finite number or null.
- `signal_strength` must be exactly `strong` or `supporting`.
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

The record freezes loading, invalid/unknown, `available:false`, partial, 401, 403, 504, retry, query-key, persistence, no-static-fallback, no-preseason-flash, and no-previous-game-placeholder behavior. Client AbortSignal support is deferred; backend upstream timeout mapping is included.

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
- invalid signal strength;
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

## Unresolved blockers and risks

There is no product-decision blocker to beginning D after Christian explicitly authorizes implementation.

Known implementation/review risks are:

1. The runtime catalog/raw-boundary guarantee adds one ranking query job; D/E must measure it rather than silently remove it.
2. The exact source-aligned games lookup adds one windowed query job.
3. Flask route specificity must be proven so `/game/<id>/lens-context` reaches the new handler and `/game/<id>` remains unchanged.
4. League Standing and trace ordinal output are intentionally suppressed for v1.
5. Current frontend request primitives do not cancel in-flight requests or enforce a client timeout.

## Files changed

- Added `documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`.
- Added `documentation/New API/handoffs/Chunk_C_Contract_Freeze_Handoff.md`.

No other file was changed.

## Validation performed

- Confirmed the branch started exactly at completed Chunk B commit `08192f10a2cfd07d2b3b711f6265607e60ef70b9`.
- Confirmed only A and B handoffs existed before Chunk C.
- Read the Endpoint Plan, Product Roadmap, Chunk A, and Chunk B completely in the required order.
- Checked every required state against an exact HTTP status, safe code, and message.
- Checked every schema field for required/nullable/type/number/date rules.
- Reconciled catalog and coverage definitions with the 59/73/70/63 evidence.
- Reconciled source-aligned counts with the accepted September 13 source dates.
- Prevented dictionary reshaping from being claimed as a duplicate guarantee.
- Reconciled DET's missing `fourth_down_pct` with current Drive Control scoring.
- Kept named metric consumers separate from tag-based lens readiness.
- Evaluated all three requested rank-blocker options and selected the smallest honest slice.
- Allocated one writer per D/E file and assigned all persisted tests to E.
- Confirmed no application code or protected system was changed.

## Ready-to-copy prompt for Chunk D

```text
[@GitHub](plugin://github@openai-curated-remote) Continue GameLens on branch `feature/matchup-lens-api` in `csells10/meow`.

This is a controlled execution of Chunk D only: implement the frozen Matchup Lens backend endpoint.

Use the model and effort assigned to Chunk D in the Product Roadmap:

- Model: `gpt-5.6-terra`
- Effort: Medium

Keep the task narrow. Do not create additional agents. Do not begin Chunk E, deployment, or frontend work.

Before writing, confirm the branch head contains the completed Chunk C contract record and handoff. Then read these files completely, in order:

1. `documentation/New API/GameLens_Matchup_Lens_API_Endpoint_Plan.md`
2. `documentation/New API/GameLens_Matchup_Lens_API_Product_Roadmap.md`
3. `documentation/New API/handoffs/Chunk_A_Evidence_Inventory_Handoff.md`
4. `documentation/New API/handoffs/Chunk_B_Frontend_Contract_Handoff.md`
5. `documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`
6. `documentation/New API/handoffs/Chunk_C_Contract_Freeze_Handoff.md`
7. Any later existing handoffs, ordered by chunk letter and date

Treat Chunk C's decision record as the controlling implementation contract. Do not reopen its product choices or edit it.

Implement only these owned paths:

- Add `services/matchup_lens_service.py`.
- Edit `queries/game_queries.py` only to append isolated, parameterized, read-only endpoint helpers for:
  1. the single raw ranking boundary/runtime-catalog read;
  2. the exact source-date-aligned windowed lookup.
- Edit `routes/game_routes.py` only to add the Firebase-authenticated `GET /game/<path:game_id>/lens-context` handler and its service import.
- Add `documentation/New API/handoffs/Chunk_D_Backend_Implementation_Handoff.md`.

Do not touch `app.py`; the game blueprint is already registered. Do not alter existing functions, `/game/<game_id>`, `/games`, existing services, tests, fixtures, runtime configuration, dependencies, pipelines, tables, schemas, scheduled jobs, learning systems, or orchestrator state. Do not edit the Endpoint Plan, Product Roadmap, contract decision record, or A/B/C handoffs.

Implementation requirements:

1. Preserve the exact `matchup_lens_v1` schema, ordering, field nullability, finite-number rules, ISO dates, reason codes, messages, and state matrix.
2. Use the existing `@require_firebase_auth` policy and its unchanged 401/403 bodies.
3. Validate only the game-ID structure; derive all canonical game/team/season/window identity from `get_game_header()` and `select_window_type()`.
4. Reuse `get_team_rankings_for_game(game_id)` without a metric allowlist and without changing its behavior.
5. Before trusting its dictionaries, run the one additional raw-boundary ranking read and detect duplicate canonical-team grain at `season + as_of_date + window_type + metric + team_id`.
6. Build the runtime catalog dynamically from the exact league snapshot. Never hardcode 59, 63, 70, or 73.
7. Use the exact source-date-aligned windowed lookup. Never attach a newer pregame count to older ranking evidence.
8. Require both canonical teams for top-level availability. Keep individual lens partial/unavailable states local to that lens.
9. Return readiness metadata only; do not calculate scores or duplicate frontend weights.
10. Preserve nullable percentiles. Reject NaN, Infinity, values below 0, values above 100, and invalid signal strength exactly as frozen.
11. Return only the two matchup teams and the frozen suppression metadata. Do not add league-wide team evidence or precomputed standings.
12. Return the separate named metric coverage for all 12 Collision/Turnover Watch consumers without using it as an allowlist.
13. Serialize deterministically with `allow_nan=False` and no internal exception details.
14. Map supported upstream deadline exceptions to the frozen 504 response; map unexpected exceptions to the frozen 500 response.

Chunk D owns no persisted tests or fixtures. You may run existing tests unchanged and use non-persisted local exercises, but do not add or edit any test file. Chunk E will independently add endpoint tests after D.

Do not deploy, merge, contact Lovable, or perform BigQuery writes. Do not invoke scheduled jobs, learning flows, or orchestrator flows.

Before committing:

- inspect the complete diff;
- prove only the four authorized D paths changed;
- verify the nested route exists without changing the existing route;
- run safe relevant existing checks;
- record commands, results, unresolved risks, and exact changed paths in the Chunk D handoff.

Commit and push only the authorized Chunk D files to `feature/matchup-lens-api`. Report the resulting commit SHA and my local fast-forward commands. Stop after Chunk D and wait for explicit authorization before Chunk E.
```
