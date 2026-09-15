# GameLens Matchup Lens API Product Roadmap

Date: 2026-09-15  
Branch: `feature/matchup-lens-api`  
Status: planning only; implementation has not started  
Reviewed commit: `f0ab1ca7ae7416c09c43d4d180b3b8f4642f3469`

## Product recommendation

Deliver one read-only evidence endpoint and a small frontend adapter in gated chunks. Spend the strongest reasoning on the evidence contract, then give implementation agents a frozen specification and tightly bounded file ownership. Keep one lead responsible for integration and acceptance.

The controlling design remains [GameLens Matchup Lens API Endpoint Plan](GameLens_Matchup_Lens_API_Endpoint_Plan.md). This roadmap does not edit, replace, or approve that plan. Recommendations below resolve questions for a later implementation decision; they are not silently adopted changes to its contract.

Only this roadmap is added in this step. No code, existing tests, services, routes, pipeline, orchestrator data, or endpoint-plan text changes. No implementation agents are launched. All agent assignments below describe future work.

## Review basis and limits

Reviewed the complete endpoint plan and relevant branch code in `queries/game_queries.py`, `routes/game_routes.py`, `agg/build_windowed_metrics.py`, and `cloudbuild.yaml`. The August readiness plan was consulted as historical background; it does not govern this new endpoint or reopen pipeline work.

The endpoint plan is the source for Paul's safeguards and Mary's frontend advice. Their original conversations and current Lovable source were not independently retrieved. The 63 live metrics, September 14 snapshot, and DET/BUF one-game counts are reported evidence from the plan, not newly verified BigQuery results in this review. Treat those as acceptance targets requiring read-only verification.

## Decisions and logic concerns

| Concern | Evidence and implication | Resolution owner / gate |
|---|---|---|
| 59 registry metrics versus 63 live metrics | Plan §9 already identifies this. A fixed 59-item runtime catalog could disagree with dynamic team payloads. | A: reconcile names with provenance. C: define runtime catalog and missing-metric denominator explicitly; never use the sample catalog as a filter. |
| Duplicate detection happens too late | The existing rankings helper assigns `away_rankings[metric]` / `home_rankings[metric]`, overwriting duplicate raw rows. A serializer cannot prove source uniqueness from these dictionaries. | A: inspect duplicates at the selected source grain. C: distinguish a one-time inventory check from a runtime guarantee. If runtime detection needs an additional read, explicitly settle that scope addition before D; do not modify the shared helper or claim dictionary uniqueness proves raw uniqueness. |
| Game counts can describe a different snapshot | Plan §7 permits the latest windowed date before the game. That may be later than the ranking evidence being returned. Windowed code uses `data_date` and metric-level rows. | A/C: match counts to the ranking source date(s), canonical team, season, and window. Specify behavior if historical rows are absent or counts disagree across metrics. Return unavailable rather than attach an unrelated newer count. |
| Helper availability is weaker than endpoint availability | `ranking_meta.available` is true when either team has rankings. | C/D: require both canonical teams for matchup availability; distinguish missing team evidence from an individual incomplete lens. |
| HTTP and partial-data rules remain open | Plan §6 offers both 409 and 200/unavailable; §7 requires validation while §10 allows incomplete lenses. | C: freeze a status/reason matrix. Recommendation: 200 with explicit availability for expected absence, 400/404 for bad or unknown IDs, and the established auth/server responses. Define unsafe evidence separately and decide whether 409 applies. Never let malformed evidence become usable just because HTTP is 200. |
| Lens readiness can accidentally duplicate scoring | Completeness depends on Mary's current tag, exclusion, null, and weight rules. Their exact interfaces are still a prerequisite. | B/C: distinguish transport coverage from scoring eligibility; specify six-lens readiness using confirmed rules, without introducing new scores or backend weighting logic. |
| JSON example is illustrative | IDs, source dates, metric objects, and readiness strings contain placeholders; lag of 5 and counts of 1 must not become constants. The helper also returns metadata absent from the proposed JSON, including aggregation details and rank tie method. | C: settle field retention and nullability, including metadata needed by the adapter. D/E: reject literal placeholders; compute freshness from source metadata. |
| Existing route captures path segments | `/game/<path:game_id>` already exists. A new nested route needs dispatch verification, not just a successful serializer test. | D/E: prove the new URL reaches the new authenticated handler and ordinary `/game/<game_id>` still reaches the existing handler. |
| Backend-first deployment is not equivalent to merging | Plan §14 deploys before frontend switching and merges last. Checked-in `cloudbuild.yaml` deploys with `--no-traffic` and tag `packet4-candidate`; trigger settings and promotion procedure were not inspected. | F: identify an approved candidate deployment and traffic-promotion path. Do not assume a push exposes the endpoint or repurpose existing release settings. If production requires an earlier merge, obtain an explicit sequencing decision. |
| Historical replay has a retention dependency | Pregame selection is safe when the required older snapshot exists, but this review has not proven windowed history is retained for every game. | A/C: document supported availability and fail honestly when matching evidence is absent; do not add backfills or schema changes. |

None of these findings calls for a pipeline redesign. The main risk is agreeing on stronger guarantees than the existing read path can supply.

## Agent allocation

Effort labels map to launch settings: **Light = `low`**, **Medium = `medium`**, **High = `high`**. Model choices use models available in this session; these are task-fit recommendations, not price or token-savings guarantees. No Max/Ultra effort is planned.

| Chunk | Agent role | Model | Effort | Dependency | Narrow output |
|---|---|---|---|---|---|
| A | Evidence inventory analyst | `gpt-5.6-sol` | High | Implementation/discovery authorization | Read-only metric, date, count, duplicate, and tag inventory |
| B | Frontend contract coordinator | `gpt-5.6-sol` | Medium | Can run alongside A | One question packet for Christian and a normalized interface handoff |
| C | Contract owner / lead | `gpt-5.6-sol` | High | A and Christian's B answers | Frozen response contract and resolved decision log |
| D | Backend implementer | `gpt-5.6-terra` | Medium | C approved; implementation authorized | Isolated builder, minimal lookup, additive route |
| E | Independent verifier | `gpt-5.6-sol` | High | D; fixture design can start after C | Endpoint-specific evidence and regression verdict |
| F | Release coordinator / lead | `gpt-5.6-sol` | Medium | E passes; deployment authorization | Verified candidate and approved live endpoint |
| G | Frontend integration coordinator | `gpt-5.6-sol` | Medium | F live check and Christian's handoff | Bounded Lovable prompt and integration acceptance evidence |
| H | Documentation closer | `gpt-5.6-luna` | Light | G and final review | Concise implementation record and merge handoff |

Use the same lead for C and F, and reuse B's coordinator for G if its context remains compact. These are work chunks, not eight agents to keep running simultaneously. Christian controls Lovable; its internal model is outside this roadmap's control.

## Chunk briefs and acceptance gates

### A — Prove the evidence supply

**Agent instruction:** Your task is narrow: inventory evidence only. Do not create additional agents.

Read plan §§5, 7, 9, 12–13 and the relevant ranking/windowed query code. Run only approved read-only queries. Reconcile all live metric names against the checked-in registry; list the actual difference rather than assuming exactly four additions. Check DET/BUF coverage, source dates, tag vocabulary, null percentiles, exclusions, raw duplicate team/metric rows, and matching windowed counts/latest included game IDs. Record project/dataset, selection date, query grain, and retrieval time.

Deliver one compact evidence matrix and a blocker list. Do not change data, registry entries, shared queries, or the plan. If BigQuery access is missing, report that dependency without fabricating results. **Pass:** every mismatch has evidence and every claimed count belongs to the selected evidence basis.

### B — Obtain the exact frontend contract

**Agent instruction:** Your task is narrow: prepare and interpret the interface handoff. Do not create additional agents.

Use plan §§10–11 to draft one consolidated request for Christian: current `LensSnapshot` and team-row types, six-lens tags and required inputs, modifiers/exclusions, null handling, 0–100 percentile expectations, API helper, cache behavior, and adapter insertion point. Only Christian sends it to Lovable and returns the answer.

Deliver a field mapping and any unresolved questions. Do not contact Lovable, alter UI files, or infer missing formulas. **Pass:** the adapter's required fields and eligibility rules are explicit enough for C to freeze them.

### C — Freeze the smallest implementable contract

**Agent instruction:** Your task is narrow: settle the endpoint contract from A and B. Do not create additional agents.

Resolve the concerns table. Specify success, no evidence, one missing team, partial lens, unsafe dates, invalid ID, unknown game, auth failure, and server error. Define metric catalog semantics, coverage denominators, required versus optional fields, readiness ownership, source-date alignment, and safe reasons. Include clearly labeled synthetic fixtures for all critical states; live acceptance values must retain provenance.

Allocate exact file ownership to D/E. Prefer the plan's minimal addition to the game blueprint and a new builder/helper; any necessary shared-file edit must be specifically bounded. Preserve existing handler behavior, ranking selection, runtime configuration, and all pipeline/orchestrator boundaries.

Deliver a separate contract decision record; leave the original endpoint plan intact unless Christian separately authorizes its revision. **Pass:** no implementer needs to invent metric membership, error policy, readiness rules, or a workaround for lost duplicate information. Stop for the plan's review gate before coding.

### D — Implement the backend slice

**Agent instruction:** Your task is narrow: implement only the frozen backend contract in your assigned files. Do not create additional agents.

Reuse canonical header lookup, phase selection, and `get_team_rankings_for_game()` without a metric allowlist. Add the agreed count lookup only; use existing runtime table resolution and parameterized values. Add structural validation and existing Firebase auth to the new handler. Preserve exclusions, tags, nulls, and selected provenance. Serialize deterministically with valid JSON numbers and safe errors.

Do not invoke `/game` service execution merely to reuse data, introduce caching infrastructure, modify shared analytics selection, or perform writes. Report incidental repeated header reads; do not refactor the shared helper for optimization in this scope.

**Pass:** the assigned diff matches C, can be imported and exercised locally with isolated data access, and contains no unrelated changes. Return changed paths, key decisions, verification commands, and remaining risks.

### E — Verify independently

**Agent instruction:** Your task is narrow: verify the new endpoint and its boundaries. Do not create additional agents.

Add only authorized endpoint-specific tests/fixtures. Cover route dispatch/auth, canonical teams, dynamic metrics beyond the sample catalog, source-date cutoff, count alignment, missing teams, partial lenses, malformed/null/non-finite values, exclusion preservation, and documented errors. Verify any agreed runtime duplicate protection at the raw-data boundary; dictionary-only checks are insufficient.

Run relevant existing tests unchanged. Compare existing `/games` and `/game` behavior using controlled fixtures or approved read-only requests; do not run scheduled jobs or learning flows to establish regressions. Confirm no new data-write call path. Perform the DET/BUF read-only acceptance check, distinguishing live observations from fixtures.

**Pass:** report pass/fail/blocked per gate, with commands and concise evidence. A missing live credential is blocked live validation, not a pass. Return defects to D; rerun affected checks after repairs, not the entire suite repeatedly without reason.

### F — Make the backend reachable safely

**Agent instruction:** Your task is narrow: coordinate the approved backend release. Do not create additional agents.

Inspect the actual build trigger and release procedure. Resolve no-traffic deployment versus production reachability and the plan's merge-last ordering before changing anything. Identify the precise commit, candidate URL, intended production URL, verification, promotion, and rollback actions for Christian's required approval.

**Pass:** an authenticated request reaches the intended endpoint on the approved revision; status, dates, team counts, and dynamic coverage match verified evidence. Neither a green build nor a candidate revision alone proves live availability. No frontend switch until this gate passes.

### G — Switch the frontend source through Christian

**Agent instruction:** Your task is narrow: draft the integration handoff and assess its returned evidence. Do not create additional agents.

Give Christian one bounded Lovable prompt referencing the frozen contract and existing API helper. Request the pure adapter, game-ID cache identity, canonical header use, and honest loading/unavailable/retry states. Preserve scoring, weights, exclusions, language, constellation, cards, tabs, and URL view behavior.

**Pass:** equivalent static-fixture evidence yields equivalent existing Lens output; switching games changes evidence; affected lenses degrade honestly; no preseason flash or silent fallback occurs. Confirm the DET/BUF date/count display with a real endpoint response. Scope is source/adaptation and required state handling, not redesign. Live rollback must show an honest unavailable state; static development data must remain explicitly labeled and outside live-game use.

### H — Close documentation and prepare merge

**Agent instruction:** Your task is narrow: document verified results and prepare the handoff. Do not create additional agents.

Record the implemented contract, actual metric reconciliation, validation results, limitations, release evidence, rollback, and deferred issues under `documentation/New API`. Avoid copying large payloads or re-narrating the entire project. Do not edit the original endpoint plan without separate authorization.

**Pass:** lead reviews the complete branch diff, protected areas remain unchanged, and Christian receives a concrete merge recommendation. Merge to `main` only after explicit approval. Do not promise a fast-forward until current branch ancestry is checked; do not rewrite shared history to force one.

## Sequencing and token discipline

1. Approve discovery scope, then run A and B independently. While awaiting Christian's frontend response, finish useful backend inventory; do not repeatedly poll or guess interfaces.
2. C integrates their compact findings and stops at contract review.
3. D implements. E may design fixtures after C, then reviews the finished slice independently. Keep one writer per file.
4. F resolves and executes the approved release path. G follows only after live backend verification.
5. H closes the record; the lead requests the explicitly required merge approval.

Send each agent its brief, assigned files, the relevant plan sections, and the latest accepted handoff. Avoid full conversation forks, repeated repository-wide searches, duplicate full-plan reviews, and parallel implementations of the same slice. Each agent returns: result, changed paths (or none), evidence, unresolved blocker, and next dependency. Target a short handoff rather than a new report; retain detailed output only when needed to reproduce a finding.

Start at the listed effort. Escalate a specific unresolved reasoning problem once with its evidence; do not upgrade all agents or retry blindly. Light is for clerical closure, Medium for settled implementation, and High for evidence semantics and independent correctness review. No agent may recruit sub-agents, widen its assignment, message Lovable, deploy, merge, or mutate data on its own initiative.

## Completion standard

The endpoint returns both teams' available pregame evidence with correct provenance and explicit gaps; the frontend consumes it without changing established Lens calculations; no frozen evidence masquerades as live data; protected backend and data behavior remain unchanged; release and regression evidence are recorded; and Christian explicitly approves the final merge. A documented unavailable lens can be an honest outcome under the original plan, but must be named in acceptance rather than presented as full six-lens coverage.

**Next work chunk:** A, with B's consolidated question packet prepared alongside it, after Christian authorizes moving beyond this roadmap-only step.
