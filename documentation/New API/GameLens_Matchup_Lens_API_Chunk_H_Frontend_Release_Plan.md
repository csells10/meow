# GameLens Matchup Lens API — Proposed Chunk H Frontend Release Plan

**Status:** proposed; not approved for execution or publication  
**Chunk:** proposed H — frontend production publication and post-publication verification  
**Assigned model / effort:** `gpt-5.6-sol` / Medium  
**Backend repository / branch:** `csells10/meow` / `main`  
**Starting backend commit:** `21c068e54841f672ddfc4603a2abf27544ec6c79`  
**Verified Lovable preview source:** `__lovable_sha=4290b715`  
**Production site:** `https://gamelens.io`  
**Verification game:** `20260917_DET@BUF`

## Authorization status

This document proposes a new release leg. The existing Product Roadmap does not
define an executable Chunk H; it only says not to begin Chunk H automatically.
Creating or approving this plan does not authorize publication.

Chunk H may begin only after Christian approves this proposed scope. Even after
pre-publication work begins, the publication action remains separately gated by
Christian's explicit approval of the exact source version and exact rollback
target recorded by the pre-publication packet.

## Purpose

Publish only the already verified Matchup Lens frontend represented by
`__lovable_sha=4290b715`, verify the resulting authenticated production
experience at `gamelens.io`, and restore the immediately preceding known-good
frontend version if production acceptance fails.

The release must preserve the completed backend and frontend contracts. Chunk H
is a publication and verification leg, not an implementation, redesign, or
backend release.

## Scope

Chunk H is limited to:

1. resolving the exact immutable Lovable source/version represented by
   `__lovable_sha=4290b715`;
2. recording the exact frontend version currently serving `gamelens.io` as the
   rollback target;
3. presenting those two identifiers and the publication action to Christian;
4. obtaining Christian's explicit approval immediately before publication;
5. performing the smallest Lovable action that publishes that exact verified
   frontend version to the existing production site;
6. verifying the published desktop and mobile Matchup Lens experience;
7. rolling back to the recorded preceding version when an exact rollback
   criterion is met and rollback has been authorized; and
8. recording the final result in one handoff.

## Explicit non-goals

Chunk H must not:

- change Lovable source, dependencies, tests, build settings, environment
  variables, API-base configuration, domain configuration, or authentication;
- change backend code, configuration, tests, routes, services, queries,
  pipelines, schemas, tables, schedules, Firebase handling, or Cloud Run;
- reopen Chunks A through G;
- redesign Matchup Lens or change its formulas, scores, weights, modifiers,
  tags, exclusions, navigation, cards, observations, Biggest Edge, Collision,
  Turnover Watch, trace behavior, warning rules, or retry rules;
- reactivate static or preseason evidence;
- create any automatic or manual live-to-static evidence fallback;
- expose, request, copy, log, persist, screenshot, or commit Firebase tokens;
- use publication as permission to repair an unexpected issue;
- update the Product Roadmap; or
- begin publication merely because this plan exists or has been committed.

Any needed source correction is outside Chunk H. Stop, leave or restore
production on the preceding known-good version, and propose a separate change.

## Accepted starting evidence

The release is based on the completed Chunk G state at backend commit
`21c068e54841f672ddfc4603a2abf27544ec6c79`:

- Lovable source implementation complete;
- complete Vitest suite: 177 / 177 passed across 11 files;
- focused live-page suite: 30 / 30 passed;
- adapter suite: 42 / 42 passed;
- continuity suite: 20 / 20 passed;
- TypeScript `tsgo --noEmit` clean;
- production build successful;
- authenticated versioned preview verified at
  `__lovable_sha=4290b715`;
- frontend not production-published.

The backend was released and verified in Chunk F. It is not part of this
publication action.

## State taxonomy

These states must remain separate in every status report:

| State | Meaning | Starting state |
|---|---|---|
| Source implemented | Required frontend code exists in Lovable source | Complete |
| Preview verified | Exact version rendered correctly in authenticated versioned preview | Complete at `__lovable_sha=4290b715` |
| Production published | Lovable reports the exact verified version is serving the existing production target | Not complete |
| Production smoke verified | Independent checks pass against `gamelens.io` after publication | Not complete |

A successful publication control action establishes only “production
published.” It does not establish “production smoke verified.”

## Pre-publication checklist

Complete all items read-only before requesting publication approval:

1. Confirm the current Lovable project and existing custom production domain are
   the expected GameLens project and `gamelens.io`. Do not add, remove, or
   reconfigure a domain.
2. Resolve `__lovable_sha=4290b715` to the exact immutable Lovable
   source/version/commit available for publication. Confirm there are no later
   unverified edits included in the proposed artifact.
3. Open the authenticated versioned preview again and confirm it still
   identifies `__lovable_sha=4290b715`.
4. Reconfirm the accepted build and test evidence. Do not change source or
   rerun commands that alter tracked files.
5. Record the exact frontend version currently serving `gamelens.io`,
   including Lovable's version/commit identifier and publication timestamp when
   available. This exact version is the rollback target.
6. Confirm Lovable can republish that recorded rollback version without editing
   source.
7. Record the exact production URL and Matchup Lens route/query used for
   `20260917_DET@BUF`. Use the same route shape that passed preview
   verification.
8. Confirm the publication control targets only the existing production
   frontend and does not modify backend, environment, domain, or authentication
   settings.
9. Confirm the operator can observe publication completion and identify the
   version that actually became active.
10. Prepare a compact approval packet containing:
    - source to publish: exact immutable version corresponding to
      `__lovable_sha=4290b715`;
    - current production version and exact rollback target;
    - production target: existing `gamelens.io`;
    - expected publication action;
    - acceptance route;
    - rollback action and criteria;
    - any discrepancy or unresolved identifier.

If the source version, existing production target, or rollback target cannot be
identified exactly, stop. Do not publish “latest,” an unversioned workspace, or
an inferred build.

## Approval checkpoints

### Checkpoint H0 — Plan approval

Christian reviews and approves this proposed Chunk H scope. This authorizes only
the read-only pre-publication checklist. It does not authorize publication.

### Checkpoint H1 — Immediate publication approval

After the pre-publication packet is complete, stop and ask Christian to approve
publication of the named immutable version to the named existing production
target. The request must show the exact rollback version beside it.

Approval must be explicit and occur immediately before the publication action.
A prior approval of Chunk G, this plan, testing, preview verification, or the
general goal of going live is not H1 approval.

The H1 request should also ask whether rollback to the named preceding version
is pre-authorized if any listed rollback criterion is met. If rollback is not
pre-authorized, stop and request separate approval before performing it.

### Checkpoint H2 — Closure

After production verification or an authorized rollback, present the final
evidence packet. Christian decides whether Chunk H closes as PASS, closes as
ROLLED BACK, or remains BLOCKED.

## Smallest safe publication action

Use Lovable's native version-aware publication control to publish the exact
immutable source/version corresponding to `__lovable_sha=4290b715` to the
already configured `gamelens.io` production target.

Do not:

- publish “latest” unless Lovable proves it is the same immutable verified
  version;
- rebuild from changed source;
- edit or synchronize files;
- change the custom domain, hosting, API base, environment, or auth settings;
- publish any backend component;
- batch unrelated changes into the release.

Immediately after the action completes, record Lovable's publication result,
active version identifier, and timestamp. If the active version cannot be
matched to the approved version, treat that as a failed acceptance and follow
the rollback decision.

## Production acceptance checks

Run these checks only against the published `https://gamelens.io` experience.

### Host and version

- HTTPS loads on the expected `gamelens.io` host without an unexpected
  redirect, certificate warning, domain reassignment, or alternate project.
- The production publication record identifies the exact H1-approved version.
- No host, API-base, environment, or authentication configuration changed.

### Authenticated DET/BUF smoke

Using the normal signed-in GameLens flow, without inspecting or copying the
Firebase token, open the production Matchup Lens route for
`20260917_DET@BUF` and verify:

- the dashboard scores and cards load;
- the canonical matchup is DET at BUF;
- the evidence/as-of date remains `2026-09-14`;
- “Evidence is uneven for Drive Control” appears exactly once;
- “League rankings are hidden” appears exactly once;
- no “1st of,” “2nd of,” or “out of N” language appears;
- no static, preseason, placeholder, or prior-game evidence appears;
- Biggest Edge, observations, readiness, Collision, Turnover Watch, trace, tabs,
  cards, and existing navigation remain functional;
- changing views preserves the existing `view` behavior;
- navigating away and back does not display another game's evidence.

Do not expose or retain the response token or full authenticated payload.

### Desktop and mobile

Perform one ordinary desktop check and one narrow mobile viewport/device check.
On each, verify that the page loads, primary cards and notices are readable,
navigation works, and no release-blocking overflow or overlap hides a required
control. This is verification only; do not redesign or make responsive changes
inside Chunk H.

### Safe state smoke checks

Use only client-side or ordinary user-flow checks that do not mutate backend
data or configuration:

- **Authentication:** in a signed-out/private session, confirm the protected
  route follows the existing sign-in behavior and does not loop, leak internal
  detail, or show live evidence. Return to the normal signed-in session through
  the existing authentication flow.
- **Unavailable:** use a known valid game that naturally returns
  `available:false` only if one is already documented and can be requested
  read-only. Confirm the safe reason, Slate navigation, and manual retry. If no
  such game is known, record this live subcheck as not safely reproducible and
  rely on the accepted automated coverage; do not invent an ID or alter data.
- **Failure and retry:** use browser-side offline/request blocking or a test
  harness that affects only the client session. Confirm no static or prior-game
  evidence appears, the safe failure state appears, retry is bounded, and
  manual retry targets only the active game after connectivity is restored.
  Do not change the API, auth, DNS, or production service to create a failure.
- **Recovery:** after restoring normal client networking, confirm the DET/BUF
  route recovers and still passes the core acceptance checks.

If the verification environment cannot perform a safe subcheck, report it
honestly. Do not widen production access or mutate a service to force the state.

## Rollback target and procedure

The rollback target is the exact frontend version serving `gamelens.io`
immediately before H1 publication. Its immutable Lovable identifier must be
captured in the pre-publication packet; until it is captured, publication is
blocked.

Rollback means republishing that exact preceding version through Lovable's
version history or native rollback control. It must not:

- edit source;
- publish an arbitrary newer or older version;
- change backend or domain configuration; or
- reconnect the static/preseason evidence source.

After rollback, confirm that `gamelens.io` reports the rollback version and
perform a minimal host, authentication, navigation, and prior-known-good page
smoke. Record both the failed release version and restored version.

## Exact rollback decision criteria

Treat production acceptance as failed and use the approved rollback path if any
of the following occurs:

1. a version other than the H1-approved immutable source becomes active;
2. `gamelens.io` resolves to an unexpected host/project, loses HTTPS, or
   requires a domain/configuration change;
3. normal authentication is broken, loops, exposes protected evidence while
   signed out, or exposes credential/internal error detail;
4. authenticated DET/BUF cannot load after normal cache refresh and one manual
   retry, unless a confirmed external outage makes rollback clearly irrelevant;
5. canonical teams or the `2026-09-14` evidence date are wrong;
6. required dashboard scores/cards or navigation are unavailable;
7. either required notice is missing or duplicated;
8. forbidden ordinal/out-of-N language appears;
9. static, preseason, placeholder, or prior-game evidence appears for the live
   route;
10. a material desktop or mobile defect blocks core use;
11. publication changes API-base, auth, domain, or another protected setting; or
12. any behavior suggests frontend contract drift or an unexpected payload/UI
    interpretation.

For an ambiguous or external failure, stop further actions, preserve evidence,
and ask Christian rather than improvising. Do not repair production source
within Chunk H.

## Stop conditions

Stop before or after publication, as applicable, for:

- source/version mismatch or an unresolvable `__lovable_sha`;
- inability to name or select the exact rollback version;
- unexpected project, host, domain, API base, environment, authentication
  method, payload schema, canonical team identity, or evidence date;
- any request for a Firebase token or any token appearing in output;
- a publication control that would also change source, configuration, or
  another site;
- backend `schema_version` other than `matchup_lens_v1`;
- unexpected `available`, reason, warning, league-context, or retry behavior;
- any static/prior-game evidence;
- a UI discrepancy that would require implementation;
- inability to determine which frontend version is live; or
- any action outside the explicit H1 approval.

A stop condition is BLOCKED or a rollback decision, not permission to widen the
scope.

## Final evidence packet

Create the final handoff only after production verification or an authorized
rollback:

`documentation/New API/handoffs/Chunk_H_Frontend_Release_Handoff.md`

The handoff must record:

1. plan and publication approval evidence;
2. exact source requested, including `__lovable_sha=4290b715` and its
   resolved immutable version;
3. exact preceding production/rollback version;
4. publication action, timestamp, and Lovable result;
5. exact version observed in production;
6. production host and tested route;
7. authenticated DET/BUF results;
8. desktop and mobile results;
9. authentication, unavailable, failure, retry, and recovery results, including
   any safely unreproducible state;
10. confirmation that no token was exposed or retained;
11. confirmation that no source, backend, configuration, domain, or contract
    changed;
12. whether rollback criteria were triggered;
13. rollback action and restored version, if used;
14. unresolved limitations;
15. final state taxonomy:
    - source implemented;
    - preview verified;
    - production published;
    - production smoke verified; and
16. verdict: PASS, ROLLED BACK, FAIL, or BLOCKED.

Do not update the Product Roadmap during this leg unless Christian separately
authorizes that documentation change.

## Ready-to-use Lovable publication prompt

```text
Execute only the proposed GameLens Chunk H frontend release workflow. Do not
edit source, redesign Matchup Lens, change configuration, or publish until the
explicit approval checkpoint below is satisfied.

Verified preview identifier:
__lovable_sha=4290b715

Production target:
the existing https://gamelens.io frontend

Verification game:
20260917_DET@BUF

First perform a read-only preflight and return:

1. the exact immutable Lovable source/version represented by
   __lovable_sha=4290b715;
2. confirmation that this exact version, with no later unverified edits, can be
   selected for publication;
3. the exact immutable Lovable version currently serving gamelens.io, including
   its publication timestamp if available;
4. confirmation that the current production version can be selected and
   republished as the rollback target without editing source;
5. the exact smallest version-aware publication action that would publish only
   the verified version to the existing gamelens.io target;
6. confirmation that the action will not change source, custom-domain settings,
   API-base configuration, environment, authentication, backend, or any other
   site; and
7. the exact production Matchup Lens URL/route you will use for
   20260917_DET@BUF acceptance.

Then stop. Do not publish. Wait for Christian to explicitly approve the named
source version, named production target, and named rollback version immediately
before publication.

Only after that explicit approval, publish the exact approved immutable version
using Lovable's native version-aware publication control. Do not publish
“latest” unless you prove it is the identical approved version. Record the
publication result, active version identifier, and timestamp.

After publication, verify https://gamelens.io in a normal authenticated session
without inspecting, copying, logging, displaying, persisting, or requesting a
Firebase token.

For 20260917_DET@BUF confirm:

- dashboard scores and cards load;
- canonical DET at BUF;
- evidence date 2026-09-14;
- “Evidence is uneven for Drive Control” exactly once;
- “League rankings are hidden” exactly once;
- no “1st of,” “2nd of,” or “out of N” language;
- no static, preseason, placeholder, or prior-game evidence;
- existing cards, observations, Biggest Edge, readiness, Collision, Turnover
  Watch, trace, tabs, view navigation, and ordinary navigation remain
  functional.

Perform one desktop and one narrow mobile verification without redesigning.
Safely smoke authentication in a signed-out/private session. For failure and
retry, use only client-side offline/request blocking or the existing test
harness; restore connectivity and verify recovery. Test a live
available:false state only if a documented valid read-only game already
provides it. Do not invent a game or alter backend data/configuration to force a
state.

If the active version differs, authentication breaks, core DET/BUF acceptance
fails, notices are missing/duplicated, ordinal rank language appears,
static/prior-game evidence appears, core navigation is blocked, or any protected
configuration changes, stop and report the exact criterion. If Christian
pre-authorized rollback, republish the exact recorded preceding production
version and verify it is restored. Otherwise stop and request rollback approval.
Never repair source within Chunk H and never enable a static live fallback.

Return the complete evidence packet, explicitly separating:

- source implemented;
- preview verified;
- production published;
- production smoke verified.

State whether rollback occurred and confirm that nothing except the approved
frontend version publication was changed.
```

## Completion rule

Chunk H passes only when the exact approved frontend version is published to
the existing `gamelens.io` target and the production smoke checks pass without
contract drift. Publication success without smoke verification is not PASS.

If acceptance fails and the preceding version is restored, close as ROLLED BACK
with evidence. If exact identifiers, approval, safe verification, or rollback
cannot be established, remain BLOCKED.

Stop after committing this proposed plan. Wait for Christian's review; do not
begin preflight or publication automatically.
