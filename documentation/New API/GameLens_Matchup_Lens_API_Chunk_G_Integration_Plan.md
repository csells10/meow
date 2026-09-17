# GameLens Matchup Lens API — Chunk G Frontend Integration Plan

**Status:** ready to begin  
**Chunk:** G — frontend integration through Christian and Lovable  
**Assigned model / effort:** `gpt-5.6-sol` / Medium  
**Backend repository:** `csells10/meow`  
**Backend branch:** `main`  
**Backend handoff head at plan creation:** `12cfc134cc67d9cecca924534e74ab4a81284558`  
**Scope:** replace the live Matchup Lens evidence source without changing its established product behavior

## Outcome required

Connect the existing Matchup Lens frontend to the authenticated production
`matchup_lens_v1` endpoint through a small, pure adapter. Preserve the current
six-lens scoring, weights, modifiers, exclusions, language, cards, tabs,
constellation, observations, Biggest Edge, Collision and Turnover Watch
behavior, and URL `view` navigation.

Chunk G is complete only when live evidence is used for a live game, game
changes cannot display evidence from another game, missing evidence degrades
honestly, and static preseason data cannot silently appear as a live fallback.

## Authoritative inputs

Read these files in order before implementation:

1. `documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`
2. `documentation/New API/handoffs/Chunk_B_Frontend_Contract_Handoff.md`
3. `documentation/New API/handoffs/Chunk_E_Verification_Handoff.md`
4. `documentation/New API/handoffs/Chunk_F_Release_Handoff.md`
5. This Chunk G plan

The contract decision record controls response fields, state handling,
validation, ordering, league-context suppression, retries, and static-fallback
rules. Chunk B controls the existing frontend formulas and integration seam.
Chunk F controls the released endpoint and production evidence.

## Live backend

- Base URL:
  `https://nfl-games-app-main-ids7lwjjta-uc.a.run.app`
- Endpoint:
  `GET /game/<encoded-game-id>/lens-context`
- Verification game:
  `20260917_DET@BUF`
- Authentication:
  existing Firebase bearer token
- Production revision:
  `nfl-games-app-main-00159-hog`
- Released source commit:
  `aedd1885fb03aa7760a9edefd0879bf8778fca01`
- Image digest:
  `sha256:36eb07e133e2cc161d0e3ba1862724a0b70ef09ebc674a0c342add4e3d461d19`

The frontend must reuse its existing API base configuration and Firebase
authentication helper. It must not introduce a second base URL, embed a token,
log a token, persist a token, or place a credential in source control.

## Accepted production response evidence

The authenticated production response for `20260917_DET@BUF` has already
passed the following checks:

- HTTP 200;
- `schema_version: matchup_lens_v1`;
- `available: true`;
- `reason: null`;
- canonical teams DET and BUF only;
- ranking/as-of date `2026-09-14`;
- source-data date `2026-09-13`;
- catalog / DET / BUF / shared counts `63 / 62 / 63 / 62`;
- 16 `context` metrics transported but excluded from lens readiness;
- warnings include asymmetric and partial evidence plus league-rank
  suppression;
- league context is suppressed;
- `numerator` and `denominator` are absent;
- the production body matched the passing candidate body byte for byte.

These values are acceptance evidence for this game, not constants or runtime
allowlists. New games must be loaded by their game ID and use their own
canonical header and evidence.

## Existing frontend contract that must remain unchanged

The current adapter target is:

```ts
export type SignalStrength = "strong" | "supporting";

export interface MetricDefinition {
  metric: string;
  label: string;
  signalStrength: SignalStrength;
  lensTags: string[];
}

export interface TeamMetricRow {
  teamId: number;
  teamAbv: string;
  gamesInWindow: number;
  latestSourceDate: string;
  dataLagDays: number;
  percentiles: Record<string, number>;
}

export interface LensSnapshot {
  asOfDate: string;
  windowLabel: string;
  gamesLabel: string;
  contextLabel: string;
  metrics: MetricDefinition[];
  teams: TeamMetricRow[];
}
```

All six lenses continue to use the existing weighted mean:

```text
score = sum(percentile × weight) / sum(weight)
```

Existing frontend ownership remains unchanged:

- base weight 2 for `strong`;
- base weight 1 for `supporting`;
- multiply by 0.5 for `volume-sensitive`;
- multiply by 0.75 for `volatility`;
- compound both modifiers when both tags exist;
- globally exclude `rare-event`;
- preserve every per-lens include and exclude tag;
- skip missing metrics rather than filling with zero;
- renormalize surviving weights;
- produce a null score when no eligible numeric metric remains.

The backend provides evidence and readiness metadata. It does not replace these
frontend calculations.

## Required implementation seam

Reuse the existing architecture identified in Chunk B:

- `src/lib/nfl-api.ts` for authenticated HTTP behavior;
- `getAuthToken()` from `src/lib/firebase.ts`;
- `src/lib/matchup-lens-source.ts` and its `LensSnapshotSource` seam;
- a pure `matchup_lens_v1` to `LensSnapshot` adapter;
- a bounded per-game wiring change in `MatchupLens.tsx`;
- a separate TanStack Query cache entry for live lens evidence.

Expected pure seam:

```ts
export function adaptMatchupLensV1(
  payload: MatchupLensV1Response,
): LensSnapshot;

export function liveLensSnapshotSource(
  gameId: string,
): LensSnapshotSource;
```

Lovable may adjust names to match current source conventions, but the adapter
must remain pure and the scoring/rendering modules below `LensSnapshot` must
not be rewritten.

## Frozen mapping rules

| Backend evidence | Frontend target | Required rule |
|---|---|---|
| `game.away_team`, `game.home_team` | live team identity | Canonical response teams are authoritative; URL `a` and `b` cannot choose live evidence. |
| `teams.<side>.team_id` | `teamId` | Convert only to a valid finite number. |
| `teams.<side>.team_abv` | `teamAbv` | Preserve the canonical abbreviation. |
| `teams.<side>.games_in_window` | `gamesInWindow` | Preserve the backend count. |
| `teams.<side>.latest_source_date` | `latestSourceDate` | Preserve the team-level source date. |
| `teams.<side>.data_lag_days` | `dataLagDays` | Preserve the backend integer. |
| metric identity and `.label` | `MetricDefinition` | Preserve dynamic metric names; do not use a fixed allowlist. |
| `.signal_strength` | `signalStrength` | Create definitions only for `strong` and `supporting`. Do not coerce `context`. |
| `.lens_tags` | `lensTags` | Preserve tags; do not invent or remove scoring tags. |
| finite `.league_percentile` | `percentiles[metric]` | Pass through on the 0–100 scale. |
| null `.league_percentile` | omitted percentile key | Never map null to zero. |
| `display.window_label` | `windowLabel` | Use the backend display value. |
| `display.games_label` | `gamesLabel` | Use the backend display value. |
| `display.context_label` | `contextLabel` | Use the backend display value. |
| `basis.as_of_date` | `asOfDate` | Preserve the ISO date. |
| `coverage` and warnings | UI readiness/state | Surface partial/asymmetric evidence honestly without changing scores. |
| `league_context.mode: suppressed` | rank-dependent UI | Hide League Standing and ordinal/out-of-N trace rank output. |

The adapter must not send numeric weights, transport `context` metrics into
frontend scoring definitions, manufacture league ranks, or use metric-level
`league_rank` as a lens-level standing.

## Query and navigation rules

Use the independent query identity:

```ts
["matchup-lens-context", gameId]
```

Required behavior:

- enable the live query only for a valid non-empty game ID;
- encode the game ID in the URL path;
- use `meta: { persist: false }`;
- do not use `keepPreviousData`;
- do not use placeholder evidence from another game;
- do not persist live lens payloads to local storage;
- preserve `refetchOnWindowFocus: false` unless current source proves a
  deliberate different convention;
- keep the existing game-details query independent;
- preserve the current `view` parameter and navigation;
- treat URL `a` and `b` as non-authoritative for a live game;
- when `gameId` changes, immediately return to a loading state until the new
  game-specific query resolves.

Static/preseason evidence may remain only in an explicitly labeled
manual/development mode when no live game ID is requested. It must never appear
while a live request loads or after a live request fails.

## Required state behavior

| Condition | Required frontend behavior |
|---|---|
| Initial live load or game change | Show the live loading shell/skeleton; show no static or prior-game evidence. |
| HTTP 200, `available: true` | Adapt and render canonical evidence. |
| HTTP 200, `available: false` | Show the backend safe reason and canonical context when present; do not score. |
| Partial/asymmetric readiness | Keep existing renormalized score, visibly identify the affected evidence, and never zero-fill. |
| HTTP 400 or 404 | Controlled invalid/unknown-game state; no static fallback and no automatic retry. |
| HTTP 401 | Preserve existing sign-out/sign-in behavior; no retry loop. |
| HTTP 403 | Preserve the safe forbidden state; no automatic retry. |
| HTTP 409 | Safe invalid/unsafe-evidence state; do not score and do not automatically retry. |
| Network, HTTP 500, or HTTP 504 | At most one automatic retry, then safe inline failure with manual retry. |
| Manual retry | Retry only the current game ID and return to an honest loading/retry state. |

No internal exception detail may be displayed to the user.

## Scope boundaries

Chunk G may change only the frontend source and tests required to:

- declare the response types;
- add the authenticated lens-context request;
- add the pure adapter;
- wire the per-game live source;
- implement the frozen loading, unavailable, partial, retry, and suppression
  states;
- add focused adapter/query/component tests;
- remove live-game dependence on the static source.

Chunk G must not:

- change backend code, queries, routes, schemas, tables, pipelines, schedules,
  learning flows, or orchestrator flows;
- change the six formulas, lens order, weights, modifiers, exclusions, language,
  or product layout;
- invent a league-standing calculation;
- reinterpret `context` as `supporting`;
- add metric allowlists;
- store Firebase tokens or response fixtures containing credentials;
- publish or switch the production frontend before Christian reviews the
  implementation evidence.

If the current frontend source has materially changed from Chunk B, stop and
report the exact conflict rather than improvising a redesign.

## Execution sequence

1. Inspect the current Lovable files named in Chunk B and confirm the types,
   source seam, API helper, query conventions, and UI components still match.
2. Return a concise proposed file list and any contract conflict before coding.
3. Implement the response types, authenticated request, and pure adapter.
4. Add per-game query wiring and prevent static/prior-game fallback.
5. Add honest state handling and league-rank suppression.
6. Run focused tests and the existing relevant frontend suite.
7. Exercise the real authenticated DET/BUF endpoint without exposing or
   persisting the Firebase token.
8. Return the complete evidence packet to Christian.
9. Wait for Christian's explicit approval before publishing or switching the
   production frontend.

## Acceptance checklist

Chunk G passes only when evidence demonstrates all of the following:

- the adapter produces the existing `LensSnapshot` shape;
- equivalent static fixture input produces equivalent existing six-lens output;
- no established score, weight, modifier, exclusion, language, card, tab,
  constellation, observation, Collision/Turnover Watch, or `view` behavior
  changed unintentionally;
- `context` evidence is transported by the API but excluded from definitions,
  percentiles, and readiness scoring inputs;
- missing/null percentiles are omitted, never zero-filled;
- DET's missing `fourth_down_pct` leaves Drive Control calculable and visibly
  partial/asymmetric;
- League Standing and ordinal/out-of-N trace rank output are hidden;
- a game change cannot flash or retain the previous game's evidence;
- live loading/failure never shows preseason or static evidence;
- unavailable, invalid, unauthorized, forbidden, conflict, timeout, server,
  retry, and exhausted-retry states follow the frozen behavior;
- the authenticated DET/BUF live response displays `2026-09-14`,
  `2026-09-13`, and `63 / 62 / 63 / 62` correctly;
- a second available game, when present, loads from its own game ID and canonical
  response teams rather than DET/BUF constants;
- no token, secret, or internal exception detail appears in source, logs,
  screenshots, fixtures, or the UI.

A successful HTTP request alone is not a Chunk G pass.

## Required return packet

Lovable must return:

1. exact files inspected;
2. exact files changed;
3. concise mapping from each change to this plan;
4. test commands and complete results;
5. screenshots or equivalent evidence for loading, live success,
   partial/asymmetric, unavailable/error, and game-switch behavior;
6. the authenticated DET/BUF acceptance summary without the token;
7. confirmation that scoring/rendering formulas were not changed;
8. confirmation that static evidence cannot act as a live fallback;
9. confirmation that league-rank-dependent output is suppressed;
10. unresolved blockers and whether anything was published.

After Christian returns this packet, the Chunk G coordinator reviews it against
the checklist and records the final result in:

`documentation/New API/handoffs/Chunk_G_Frontend_Integration_Handoff.md`

That later handoff must distinguish implementation from production publication
and must not claim the frontend is live without direct evidence.

## Ready-to-copy Lovable implementation prompt

```text
Implement Chunk G only: connect the existing GameLens Matchup Lens frontend to
the authenticated production matchup_lens_v1 endpoint.

Before changing anything, read the complete Chunk G plan and the controlling
contract supplied by Christian. Inspect the current source paths identified in
Chunk B and confirm they still match. Return your proposed changed-file list and
any contract conflict before coding. Do not redesign Matchup Lens.

Production API:
https://nfl-games-app-main-ids7lwjjta-uc.a.run.app

Endpoint:
GET /game/<encoded-game-id>/lens-context

Use the existing Firebase-authenticated API helper and existing API base
configuration. Never expose, log, persist, or commit a token.

Implement a pure matchup_lens_v1-to-LensSnapshot adapter and a separate
per-game live query keyed by ["matchup-lens-context", gameId]. Use
meta: { persist: false }; do not use keepPreviousData or placeholder evidence.
Canonical teams come from the response, not URL a/b. Preserve view navigation.

Keep all six existing lens formulas, ordering, weights, modifiers, exclusions,
language, cards, tabs, constellation, observations, Biggest Edge, Collision and
Turnover Watch behavior unchanged. Include only strong/supporting metrics in
frontend definitions and percentiles. Context metrics remain transported
evidence but must not enter scoring. Omit null percentiles; never zero-fill.

Honor backend display fields and readiness/warnings. When league_context is
suppressed, hide League Standing and ordinal/out-of-N trace ranking. Partial or
asymmetric evidence may retain the existing renormalized score but must be
identified honestly.

For live game IDs, never show preseason/static or prior-game evidence during
loading or failure. Implement the frozen 200-unavailable, 400, 401, 403, 404,
409, 500, 504, retry, and manual-retry behavior. Allow at most one automatic
retry for network/500/504 and none for the other listed states.

Add focused adapter, query, state, and game-switch tests. Verify the real
authenticated 20260917_DET@BUF response without retaining the token. Expected
acceptance evidence is schema matchup_lens_v1, available true, reason null,
canonical DET/BUF, dates 2026-09-14 and 2026-09-13, coverage 63/62/63/62,
16 context metrics excluded from readiness, league suppression, and no
numerator/denominator.

Return the exact inspected and changed files, test output, state evidence,
live-response summary, formula-preservation confirmation, static-fallback
confirmation, league-suppression confirmation, blockers, and publication
status. Do not publish or switch the production frontend until Christian
explicitly approves the reviewed evidence.
```
