[@GitHub](plugin://github@openai-curated-remote) Continue GameLens from `main` in `csells10/meow`.

This is a controlled execution of Chunk G only: coordinate the frontend integration of the live Matchup Lens API through Christian and Lovable.

Use the model and effort assigned to Chunk G in the Product Roadmap:

- Model: `gpt-5.6-sol`
- Effort: Medium

Work directly. Do not create or use additional agents.

The expected backend repository head is:

`57467e4fe1bd22bdfaf768072f19b6f446990496`

Use the connected authenticated GitHub integration for repository access and any eventual documentation commit. Christian has explicitly granted GitHub write permission. Do not use shell `git push`, patches, or manual file transfer.

## Read first

Read these files completely, in order:

1. `documentation/New API/GameLens_Matchup_Lens_API_Chunk_G_Integration_Plan.md`
2. `documentation/New API/GameLens_Matchup_Lens_API_Contract_Decision_Record.md`
3. `documentation/New API/handoffs/Chunk_B_Frontend_Contract_Handoff.md`
4. `documentation/New API/handoffs/Chunk_E_Verification_Handoff.md`
5. `documentation/New API/handoffs/Chunk_F_Release_Handoff.md`
6. Any existing later handoff, ordered by chunk letter and date

Treat the Contract Decision Record as the controlling backend contract. Treat Chunk B as a historical snapshot of Lovable’s frontend logic—not proof that its current source is unchanged.

The production backend is already live and verified:

- Base URL:
  `https://nfl-games-app-main-ids7lwjjta-uc.a.run.app`
- Endpoint:
  `GET /game/<encoded-game-id>/lens-context`
- Verification game:
  `20260917_DET@BUF`
- Production revision:
  `nfl-games-app-main-00159-hog`
- Released source commit:
  `aedd1885fb03aa7760a9edefd0879bf8778fca01`

## Required Chunk G sequence

Chunk G must proceed in separate checkpoints. Do not collapse discovery, comparison, implementation, and publication into one step.

### Phase G1 — Fresh Lovable logic inspection

First, prepare one bounded read-only prompt for Christian to send to Lovable.

The prompt must require Lovable to independently inspect its current source and reiterate its actual current Matchup Lens logic before being shown our historical conclusions or proposed implementation.

Do not preload the Lovable prompt with the formulas, weights, mapping conclusions, or adapter design previously reported in Chunk B. Naming the relevant product areas and asking for exact source evidence is allowed, but the answer must originate from Lovable’s present source.

Lovable’s first response must report:

1. Exact files inspected.
2. Exact current TypeScript interfaces for:
   - `LensSnapshot`;
   - team evidence rows;
   - metric definitions;
   - lens definitions/results;
   - readiness, warning, error, or UI-state types.
3. The exact current source of Matchup Lens evidence.
4. Every transformation between that source and rendered output.
5. The six lenses in their actual current order.
6. Exact calculation logic for each lens.
7. Current metric inclusion and exclusion rules.
8. Current tag matching rules.
9. Current weights, modifiers, caps, thresholds, normalization, polarity, rounding, and missing-data behavior.
10. Exact direct metric-name consumers, including Collision and Turnover Watch.
11. Current league-standing and trace-ranking behavior.
12. Current handling of:
    - absent metrics;
    - null percentiles;
    - partial evidence;
    - one missing team;
    - warnings;
    - loading;
    - authentication failure;
    - unavailable data;
    - timeout;
    - retry;
    - game changes.
13. Current authenticated API helper, Firebase-token handling, API-base configuration, error mapping, timeout support, and retry behavior.
14. Current TanStack Query keys and settings.
15. How `game`, `a`, `b`, and `view` currently affect data selection, calculations, and navigation.
16. Every static/preseason fallback path.
17. Existing tests covering this behavior.
18. Any changes since the logic described in Chunk B, if Lovable can identify them from history.
19. No implementation recommendations until the current behavior has been fully reported.

The Lovable prompt must explicitly say:

- read-only inspection only;
- do not edit files;
- do not implement the endpoint;
- do not publish;
- do not redesign Matchup Lens;
- do not infer missing behavior;
- cite exact file paths and symbols;
- quote or precisely transcribe operational logic;
- label unresolved facts as unresolved.

Give Christian that prompt and stop. Wait for Christian to return Lovable’s complete response.

Do not begin Phase G2 from assumptions.

### Phase G2 — Drift analysis and complete mapping

After Christian returns Lovable’s current-source inspection, compare it against:

1. the historical Chunk B frontend contract;
2. the controlling backend Contract Decision Record;
3. the verified production response described by Chunks E and F.

Produce three separate outputs.

#### A. Frontend drift report

For every material frontend behavior, show:

| Area | Chunk B historical behavior | Lovable current behavior | Status | Impact |
|---|---|---|---|---|

Allowed status values:

- `unchanged`
- `compatible change`
- `contract drift`
- `unresolved`

Do not dismiss differences as harmless without evidence.

#### B. End-to-end field mapping

Create a complete mapping across all four layers:

1. `matchup_lens_v1` response;
2. frontend response TypeScript type;
3. adapter output (`LensSnapshot` or its current replacement);
4. actual calculation/display consumer.

Use one row per field or derived value with these columns:

| API response path | Response type field | Adapter transformation | Existing frontend target | Current consumer | Required/optional/nullable | Scoring/display/state purpose | Missing behavior | Mapping status |
|---|---|---|---|---|---|---|---|---|

Allowed mapping status values:

- `exact`
- `compatible transformation`
- `frontend change required`
- `unused transport metadata`
- `blocked`
- `unresolved`

The mapping must cover at least:

- top-level schema, availability, and reason;
- canonical game header;
- away/home identities;
- display fields;
- basis and freshness fields;
- dynamic metric catalog;
- metric labels;
- signal strength;
- lens tags;
- percentile;
- null percentile behavior;
- team games/count fields;
- latest included game;
- coverage totals;
- six readiness rows;
- named metric coverage;
- warnings;
- league-context suppression;
- method metadata;
- every current hardcoded metric-name consumer;
- loading, unavailable, error, and retry state inputs.

Explicitly identify:

- fields that enter scoring;
- fields used only for display;
- fields used only for availability or warnings;
- fields retained only for audit/provenance;
- response fields the frontend should ignore;
- current frontend fields that have no API source;
- API fields that have no current frontend consumer;
- any transformation that could accidentally change established results.

#### C. Implementation decision packet

Based on the verified current logic and mapping, provide:

- exact proposed frontend files to change;
- exact files that must remain untouched;
- smallest safe adapter seam;
- query/cache design;
- removal or containment of static live-game fallback;
- league-rank suppression approach;
- partial/asymmetric evidence presentation;
- test plan;
- unresolved decisions requiring Christian’s input.

Do not silently adopt a solution for contract drift. Present the smallest decision clearly.

Stop and wait for Christian’s approval of the mapping and proposed implementation scope.

### Phase G3 — Lovable implementation prompt

Only after Christian approves Phase G2, prepare the final bounded Lovable implementation prompt.

That prompt must be derived from Lovable’s newly verified logic and the approved field mapping—not copied blindly from Chunk B or the existing Chunk G plan.

The implementation must:

- preserve the currently verified six-lens calculations;
- preserve current weights, modifiers, exclusions, language, cards, tabs, constellation, observations, Biggest Edge, Collision and Turnover Watch behavior;
- reuse existing Firebase authentication and API-base configuration;
- add a pure API-to-frontend adapter;
- use canonical teams from the response;
- use `['matchup-lens-context', gameId]` unless current conventions establish a justified alternative;
- prevent persisted, previous-game, static, or preseason evidence from appearing during a live request;
- exclude transported `context` evidence from scoring without coercing it;
- omit null percentiles rather than converting them to zero;
- surface partial/asymmetric evidence honestly;
- suppress League Standing and ordinal/out-of-N trace output while league context is suppressed;
- preserve `view` navigation;
- implement the frozen unavailable/error/retry behavior;
- add focused tests;
- avoid exposing or persisting Firebase tokens;
- remain unpublished until Christian reviews the returned evidence and explicitly approves publication.

Give Christian the reviewed implementation prompt and wait for the returned Lovable implementation evidence.

### Phase G4 — Verification and handoff

After Christian returns Lovable’s implementation evidence:

1. Verify every changed file against the approved mapping.
2. Verify tests and state evidence.
3. Confirm formulas and established product behavior did not drift.
4. Confirm game switching cannot show the prior game.
5. Confirm no static/preseason fallback can masquerade as live evidence.
6. Confirm league-rank-dependent output is suppressed.
7. Confirm the real authenticated DET/BUF response maps correctly:
   - schema `matchup_lens_v1`;
   - `available:true`;
   - `reason:null`;
   - canonical DET and BUF;
   - dates `2026-09-14` / `2026-09-13`;
   - counts `63 / 62 / 63 / 62`;
   - 16 transported `context` metrics excluded from readiness/scoring;
   - no `numerator` or `denominator`.
8. Distinguish implemented, previewed, and production-published states.
9. Do not call Chunk G passed merely because Lovable says implementation succeeded.

Only after the evidence passes may you add:

`documentation/New API/handoffs/Chunk_G_Frontend_Integration_Handoff.md`

That handoff must record:

- Lovable’s revalidated current logic;
- drift from Chunk B;
- the approved API-to-frontend mapping;
- exact frontend files changed;
- tests and results;
- live endpoint evidence;
- UI-state evidence;
- publication status;
- rollback or source-switch mechanism;
- unresolved limitations;
- PASS/FAIL/BLOCKED verdict;
- next dependency.

Commit and push only that documentation handoff to `main` through the connected authenticated GitHub integration with `[skip ci]`. Do not modify backend implementation or configuration.

Stop after Chunk G. Do not begin Chunk H automatically.