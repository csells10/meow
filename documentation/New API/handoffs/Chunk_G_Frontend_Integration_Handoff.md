# Chunk G — Frontend Integration Close Handoff

**Status:** complete — authenticated preview verification passed  
**Verdict:** **PASS**  
**Backend repository / branch:** `csells10/meow` / `main`  
**Backend starting head:** `f85d7fa0d41560e41f98850144b1f66b12418641`  
**Frontend environment:** Lovable versioned preview  
**Verified preview source:** `__lovable_sha=4290b715`  
**Assigned model / effort:** `gpt-5.6-sol` / Medium  
**Scope:** Chunk G frontend integration, verification, and documentation closure only

## Outcome

The authenticated GameLens Matchup Lens frontend preview successfully consumes the
production `matchup_lens_v1` endpoint for `20260917_DET@BUF` and maps the live
response into the existing six-lens frontend model.

Chunk G passes because the evidence was not accepted from Lovable's implementation
claim alone. The implementation and tests were reviewed, the warning contract was
corrected to the exact frozen vocabulary, and Christian completed an authenticated
visual verification in the versioned Lovable preview.

The frontend remains **unpublished**. No production frontend publication or
deployment was performed by Chunk G.

## Revalidated current frontend logic

The accepted live path is:

1. `MatchupLens.tsx` classifies the URL `game` identifier.
2. `useMatchupLensContext` issues one authenticated request using the existing
   shared API base and the game-scoped key
   `["matchup-lens-context", gameId]`.
3. Firebase supplies a bearer token through the existing authentication helper.
4. The response must use `schema_version: "matchup_lens_v1"`.
5. A pure adapter validates and translates the response into the existing
   `LensSnapshot` boundary.
6. The established calculation and presentation modules consume that snapshot.

The following behaviors were preserved:

- the six lenses and their order;
- existing weights, modifiers, exclusions, tags, normalization, polarity,
  rounding, and missing-value renormalization;
- existing cards, tabs, constellation, observations, Biggest Edge, Collision,
  Turnover Watch, trace behavior, and `view` navigation;
- canonical team identity from the response rather than URL `a` / `b`;
- null percentiles omitted rather than coerced to zero;
- transported `context` metrics excluded from definitions, percentile records,
  readiness denominators, and scoring;
- partial and asymmetric evidence disclosed without inventing inputs;
- league-rank-dependent output suppressed while league context is suppressed.

## Drift from the historical Chunk B inspection

Chunk B described the historical frontend contract before the live adapter was
accepted. By the read-only G1 inspection, the current Lovable source already
contained most of the live integration architecture:

- authenticated per-game request;
- `matchup_lens_v1` response types;
- pure response adapter;
- canonical response teams;
- game-scoped non-persisted query;
- no static or previous-game live fallback;
- partial/asymmetric readiness;
- league-rank-dependent output suppression.

G3 therefore remained a narrow correction-and-verification pass rather than a
rebuild. The material corrections were:

- add manual retry for HTTP 200 with `available:false`, while retaining the
  backend safe reason and Slate navigation;
- consume the exact frozen warning-code vocabulary through an exact keyed mapping;
- suppress notices already represented by readiness or league-suppression UI;
- surface only the two contract-approved display warnings once;
- ignore unknown warning codes rather than render arbitrary unvalidated text.

The G1 inspection could not reconstruct a reliable file-by-file historical diff
from Chunk B because the Lovable repository history available to it used generic
commit messages and retained no prior Chunk B inspection artifact. That historical
diff remains unresolved; it does not block the verified current contract.

## Approved API-to-frontend mapping

| API evidence | Frontend target / behavior | Accepted rule |
|---|---|---|
| `schema_version` | contract gate | Must equal `matchup_lens_v1`; otherwise no scoring |
| `available`, `reason` | dashboard availability state | Unavailable shows safe reason, Slate action, and manual retry; no automatic retry |
| canonical game header | away/home identity | Response identity overrides URL display abbreviations |
| basis and source dates | context header / freshness | Display-only and provenance; never score inputs |
| dynamic metric catalog | metric definitions | Preserve dynamic identities and order; no fixed runtime allowlist |
| `signal_strength` | scoring eligibility | Only established scoring strengths become definitions; `context` is excluded |
| `lens_tags` | existing lens engine | Preserve tags without inventing or removing tags |
| percentile | team evidence | Keep numeric zero; omit null |
| coverage/readiness | readiness UI | Preserve all six rows and partial/asymmetric state |
| warnings | existing notice system | Exact-code mapping with deterministic deduplication |
| suppressed league context | rank-dependent UI | Hide League Standing and ordinal / “out of N” output |
| method metadata | audit/provenance | Does not change established formulas |
| numerator / denominator | not accepted transport fields | Absent from the verified response |

## Exact warning contract

| Warning code | UI disposition |
|---|---|
| `PARTIAL_LENS_EVIDENCE` | Represented by readiness UI; do not duplicate |
| `ASYMMETRIC_LENS_EVIDENCE` | Represented by readiness UI; do not duplicate |
| `UNAVAILABLE_LENS_EVIDENCE` | Represented by unavailable/readiness UI; do not duplicate |
| `LEAGUE_RANK_OUTPUT_SUPPRESSED` | Represented by league-suppression UI; do not duplicate |
| `MULTIPLE_SOURCE_DATES` | Display the backend safe message once |
| `NAMED_METRIC_GAPS` | Display the backend safe message once |
| unknown code | Ignore; no visible unvalidated text and no scoring effect |

No substring matching or case folding is used.

## Frontend files

### Current live-integration files inspected

- `src/lib/matchup-lens-api-types.ts`
- `src/lib/matchup-lens-adapter.ts`
- `src/lib/matchup-lens-live.ts`
- `src/lib/nfl-api.ts`
- `src/pages/MatchupLens.tsx`
- `src/lib/matchup-lens-presentation.tsx`
- `src/components/matchup-lens/DashboardStates.tsx`
- `src/components/matchup-lens/MatchupContextBar.tsx`
- `src/components/matchup-lens/LensReadinessNote.tsx`
- `src/test/matchup-lens-v1-fixture.ts`
- `src/test/matchup-lens-live-harness.ts`
- `src/test/matchup-lens-adapter.test.ts`
- `src/test/matchup-lens-live-page.test.tsx`
- `src/test/matchup-lens-continuity.test.tsx`

### Exact files changed in the approved G3/G4 corrective pass

- `src/pages/MatchupLens.tsx`
- `src/components/matchup-lens/DashboardStates.tsx`
- `src/test/matchup-lens-live-page.test.tsx`

`DashboardStates.tsx` changed because the existing empty-state interface exposed
only one action and could not express unavailable retry plus the retained Slate
navigation cleanly.

The adapter, scoring engine, rank, trace, Collision, Turnover Watch, and static
snapshot files were not changed by the corrective pass.

## Tests and build evidence

Final Lovable evidence reported:

- focused live-page suite: **30 / 30 passed**;
- complete Vitest suite: **177 / 177 passed** across 11 files;
- adapter suite: **42 / 42 passed**;
- continuity suite: **20 / 20 passed**;
- TypeScript: `tsgo --noEmit` clean;
- production build: succeeded in 9.77 seconds;
- only the pre-existing chunk-size advisory remained.

Focused coverage includes:

- malformed client-side game ID;
- HTTP 400, 401, 403, 404, 409, 500, and 504;
- network failure and bounded automatic retry;
- HTTP 200 with `available:false`;
- unavailable manual retry scoped to the active game;
- contract-invalid successful response;
- retry exhaustion;
- all six frozen warning codes and one unknown code;
- warning deduplication without scoring changes;
- game switching without prior-game evidence;
- loading/failure without preseason evidence;
- canonical response teams over URL abbreviations;
- league-rank-dependent output suppression.

## Live endpoint evidence

The accepted backend release record and authenticated preview jointly establish
the live path for `20260917_DET@BUF`.

Backend production evidence:

- HTTP 200;
- `schema_version: "matchup_lens_v1"`;
- `available:true`;
- `reason:null`;
- canonical away DET and home BUF;
- basis/as-of date `2026-09-14`;
- source-data date `2026-09-13`;
- catalog / DET / BUF / shared counts `63 / 62 / 63 / 62`;
- 16 transported `context` metrics;
- those context metrics excluded from readiness and scoring;
- DET Drive Control partial because `fourth_down_pct` is absent;
- BUF Drive Control complete;
- no `numerator` or `denominator`;
- league context suppressed.

The frontend kept the existing shared API base:

`https://nfl-games-app-main-362530996210.us-central1.run.app`

An unauthenticated request returned HTTP 401, proving the route exists and
enforces authentication. The authenticated versioned preview then rendered the
expected DET/BUF live evidence through that unchanged configuration.

No Firebase token or production payload was logged, displayed, persisted, or
committed.

## Authenticated UI-state evidence

Christian verified the versioned Lovable preview identified by
`__lovable_sha=4290b715`.

The captured authenticated dashboard showed:

- the DET versus BUF dashboard loaded with scored cards;
- the context header used regular-season-to-date evidence as of 2026-09-14;
- “Evidence is uneven for Drive Control” exactly once;
- “League rankings are hidden” exactly once;
- no visible “1st of”, “2nd of”, or “out of N” league-ranking language;
- existing Biggest Edge, observation, readiness, and navigation surfaces intact.

This satisfies G4's requirement to distinguish source implementation from an
authenticated rendered preview.

## Query, fallback, and retry safety

- Query key: `["matchup-lens-context", gameId]`.
- Lens-query persistence remains disabled with `meta: { persist: false }`.
- The game ID scopes both the cache and request URL.
- No placeholder or previous-game data is used.
- A game change clears transient selection/trace state and cannot flash the prior
  game's evidence.
- The live page has no production import of the static preseason source.
- Network, HTTP 500, and HTTP 504 receive at most one automatic retry.
- HTTP 400, 401, 403, 404, 409, and `available:false` do not automatically retry.
- Manual retry targets only the active game.
- HTTP 401 preserves Firebase sign-out / protected-route sign-in behavior.
- No client-side timeout or cancellation was added; a timeout state currently maps
  the backend HTTP 504 response.

## Publication state

| State | Result |
|---|---|
| Implemented in Lovable source | Yes |
| Verified by automated tests/typecheck/build | Yes |
| Verified in authenticated versioned preview | Yes |
| Published to production frontend | **No** |
| Backend deployed and live | Yes, completed in Chunk F |
| Chunk G documentation committed to backend `main` | This handoff |

Chunk G does not claim the frontend is live at `gamelens.io`.

## Rollback and source-switch behavior

Because the corrected frontend has not been published, the immediate rollback is
to withhold publication; no production frontend change must be undone.

After any later approved publication, rollback should restore the preceding
known-good frontend version or commit. It must not reactivate the static preseason
snapshot as a live-game fallback.

There is no approved runtime source switch from live evidence to static evidence.
The legacy static source module remains orphaned from the production page. Any
future removal of that dead module is cleanup work and is not part of Chunk G.

## Unresolved limitations

- The complete historical Lovable diff from Chunk B could not be reconstructed
  from the available generic commit history.
- The G4 test browser could not inject Firebase Google authentication; Christian's
  signed-in versioned preview supplied the required authenticated UI verification.
- Client-side request cancellation and a client-owned timeout are not implemented;
  timeout handling currently represents backend HTTP 504.
- The intent to retain or later delete the orphaned static source module remains a
  separate cleanup decision.
- Production frontend publication and post-publication smoke verification have not
  occurred.

None of these limitations invalidates the authenticated preview acceptance.

## Verdict

**PASS — Chunk G is closed.**

The live backend response maps into the existing six-lens frontend without
formula drift, static fallback, previous-game leakage, warning duplication, or
league-rank leakage. Tests, typecheck, build, exact warning vocabulary, bounded
retry behavior, and the authenticated versioned preview all passed.

## Next dependency

The current roadmap defines Chunks A through G and does not define an executable
Chunk H scope. It only says not to begin Chunk H automatically.

The next work should therefore begin with an explicit planning/approval step for a
frontend publication and post-publication smoke-verification leg. That new leg
should remain separate from Chunk G, require Christian's publication approval,
record the exact Lovable version being published, verify `gamelens.io` after
publication, and retain a known-good frontend rollback target.
