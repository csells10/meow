# Chunk F — Release Handoff

**Status:** complete — production release verified  
**Verdict:** **PASS**  
**Release source branch:** `feature/matchup-lens-api`  
**Repository closure branch:** `main`  
**Released source commit:** `aedd1885fb03aa7760a9edefd0879bf8778fca01`  
**Starting branch head:** `f89f684ac6265f138f3903c3329bf94cc05f3294`  
**Assigned model / effort:** `gpt-5.6-sol` / Medium  
**Scope:** Chunk F release coordination only

## Outcome

The verified Matchup Lens backend is reachable through the normal production
Cloud Run URL. The exact approved feature-branch commit was built, deployed as
a tagged no-traffic candidate, authenticated against live production evidence,
promoted only after a second approval, and authenticated again through the
production URL.

No implementation, test, fixture, route, query, service, dependency, runtime
configuration, Cloud Build configuration, pipeline, schema, table, scheduled
job, learning flow, or orchestrator flow was changed by Chunk F. The only
Chunk F repository path is this handoff.

## Accepted Chunk E basis

- 42 passed, 8 subtests passed in 2.76s.
- Authenticated read-only live builder smoke returned HTTP 200 with
  `available:true` and `reason:null`.
- Catalog / DET / BUF / shared counts were 63 / 62 / 63 / 62.
- Ranking/source dates were 2026-09-14 / 2026-09-13.
- Sixteen known `context` metrics were transported but excluded from
  readiness.
- `numerator` and `denominator` were absent.
- No BigQuery write occurred.

## Release mechanisms inspected

### Development rehearsal

- trigger: `matchup-lens-api-dev`
- trigger ID: `e659b04e-153d-474d-bfcc-8f88ab08077f`
- source: `csells10/meow`, branch regex
  `^feature/matchup-lens-api$`
- build file: `cloudbuild-dev.yaml`
- approval required: true
- service: `nfl-games-app-dev`
- region: `us-central1`
- URL: `https://nfl-games-app-dev-ids7lwjjta-uc.a.run.app`

The dev build directly moved dev traffic and was therefore treated only as an
isolated rehearsal. Its controlled-replay configuration used season 2025 and
the `League_dev`, `Scores_dev`, and `Analytics_dev` datasets.

### Production candidate

- trigger: `git-commit-nfl-games-app-us-central1-csells10-meow`
- trigger ID: `0a4277ca-ca97-4184-9fe4-d3cfed83500d`
- automatic branch regex: `^main$`
- manually approved source selector:
  `--sha=aedd1885fb03aa7760a9edefd0879bf8778fca01`
- build file: `cloudbuild.yaml`
- service: `nfl-games-app-main`
- region: `us-central1`

The production build file pushed the commit-addressed image and deployed with
`--no-traffic --tag packet4-candidate`. Manual exact-SHA invocation made the
backend-first, merge-last sequence operationally possible without changing the
trigger, build file, or source branch.

## Approval checkpoints

Christian explicitly approved:

1. the dev rehearsal and approval-gated dev build;
2. the no-traffic production candidate deployment of commit
   `aedd1885fb03aa7760a9edefd0879bf8778fca01`;
3. promotion of candidate revision `nfl-games-app-main-00159-hog` to
   100 percent production traffic; and
4. a clean fast-forward of `feature/matchup-lens-api` into `main` after
   production verification and this final handoff.

No build, candidate deployment, traffic move, or merge preceded its applicable
approval checkpoint.

## Development rehearsal evidence

- build ID: `02166289-4da3-47b7-ac70-95860e2265f4`
- result: `SUCCESS`
- start: `2026-09-16T18:54:45.349319705Z`
- finish: `2026-09-16T18:59:56.889891Z`
- revision: `nfl-games-app-dev-00160-gkt`
- image digest:
  `sha256:d637f29cfba02872a37fba60329a56b64c24b8d765d36a77314e615887b36e30`
- authenticated `/games?date=2025-09-18`: HTTP 200
- authenticated `/game/20250918_MIA@BUF/lens-context`: HTTP 200,
  `matchup_lens_v1`, `available:true`, `reason:null`
- dev catalog / MIA / BUF / shared: 56 / 53 / 53 / 53
- only canonical MIA and BUF evidence was returned
- all six readiness rows were complete
- context evidence was transported
- league context was suppressed
- `numerator` and `denominator` were absent

The dev runtime service account initially lacked Firestore authorization-list
read access and read access to the shared `Teams.team_logos` table. Christian
added `roles/datastore.viewer` and table-level
`roles/bigquery.dataViewer` for `Teams.team_logos`. These were external
read-only IAM changes, not repository or runtime-configuration changes.

An authenticated legacy `/game/20250918_MIA@BUF` request reached
`save_model_results` and stopped at a denied read of
`Analytics.game_model_outcomes`. No write occurred. Because that legacy GET
path can persist model results, Chunk F did not grant further access or invoke
it in production. Existing-route separation and unchanged behavior remain
covered by the accepted endpoint tests; safe live compatibility reads used
`/games`.

## Production candidate build

- approved source commit:
  `aedd1885fb03aa7760a9edefd0879bf8778fca01`
- build ID: `9b39bec6-ee16-410a-a9a6-693295e2a63a`
- result: `SUCCESS`
- create: `2026-09-16T22:03:43.527721Z`
- start: `2026-09-16T22:03:44.262162848Z`
- finish: `2026-09-16T22:07:21.477106Z`
- revision: `nfl-games-app-main-00159-hog`
- image:
  `us-central1-docker.pkg.dev/nfl-stream-406420/meow-main/main@sha256:36eb07e133e2cc161d0e3ba1862724a0b70ef09ebc674a0c342add4e3d461d19`
- candidate URL:
  `https://packet4-candidate---nfl-games-app-main-ids7lwjjta-uc.a.run.app`
- production URL:
  `https://nfl-games-app-main-ids7lwjjta-uc.a.run.app`
- pre-promotion production revision:
  `nfl-games-app-main-00157-xed`
- pre-promotion traffic: 100 percent to
  `nfl-games-app-main-00157-xed`
- pre-promotion image digest:
  `sha256:a418b8f11a88b6efb5b218d53319423693aa3efb38f85542d744deff0226da5b`

Artifact Registry resolved the approved commit tag to the exact image digest
used by revision `nfl-games-app-main-00159-hog`.

## Authenticated candidate verification

Authenticated GET
`/game/20260917_DET@BUF/lens-context` against the tagged candidate returned:

- HTTP 200;
- `schema_version: matchup_lens_v1`;
- `available:true`;
- `reason:null`;
- ranking/as-of date `2026-09-14`;
- source data dates `[2026-09-13]`;
- catalog / DET / BUF / shared counts 63 / 62 / 63 / 62;
- only canonical DET and BUF team evidence;
- six readiness rows in frozen order;
- 16 known `context` metrics transported and absent from readiness missing
  lists;
- warning order:
  `ASYMMETRIC_LENS_EVIDENCE`,
  `LEAGUE_RANK_OUTPUT_SUPPRESSED`,
  `PARTIAL_LENS_EVIDENCE`;
- suppressed league context;
- no `numerator` or `denominator` fields;
- no internal traceback or exception detail.

Authenticated GET `/games?date=2026-09-17` against the candidate returned
HTTP 200 and the scheduled `20260917_DET@BUF` game.

These were read-only requests. No BigQuery write, scheduled job, learning flow,
or orchestrator flow was invoked.

## Traffic action and production verification

Approved promotion command:

```bash
gcloud run services update-traffic nfl-games-app-main \
  --project=nfl-stream-406420 \
  --region=us-central1 \
  --to-revisions=nfl-games-app-main-00159-hog=100
```

Cloud Run completed the update and routed 100 percent of production traffic to
`nfl-games-app-main-00159-hog`.

A fresh Firebase token was required after an expired token produced
authentication-layer HTTP 401 responses. With the fresh token:

- production Matchup Lens returned HTTP 200;
- the production response matched the passing candidate response byte for
  byte;
- production `/games?date=2026-09-17` returned HTTP 200.

Candidate availability and production availability were verified separately.
The endpoint is live through the intended production URL.

## Rollback

Rollback target: `nfl-games-app-main-00157-xed`.

Approved rollback command:

```bash
gcloud run services update-traffic nfl-games-app-main \
  --project=nfl-stream-406420 \
  --region=us-central1 \
  --to-revisions=nfl-games-app-main-00157-xed=100
```

Rollback was **not used**. The initial post-promotion 401 responses were
confirmed to be an expired Firebase token rather than a release failure.

## Repository scope and merge

Exact Chunk F changed repository path:

- `documentation/New API/handoffs/Chunk_F_Release_Handoff.md`

The final handoff commit uses `[skip ci]` so the authorized fast-forward of
`main` does not create a redundant candidate build. Before the fast-forward,
GitHub comparison reported the feature branch ahead of `main` and behind by
zero commits.

No frontend or Lovable repository was changed.

## Post-release repository closure

After the production endpoint passed, Christian explicitly approved the clean
fast-forward and branch cleanup.

- GitHub `main` was fast-forwarded from
  `b93c41c210288b9b4d450b4145e2d596e566aa67` to the completed Chunk F
  handoff commit `12cfc134cc67d9cecca924534e74ab4a81284558`.
- GitHub comparison then reported `main` and
  `feature/matchup-lens-api` as identical: ahead 0, behind 0, with no changed
  files.
- Christian fast-forwarded the local `main` branch and verified local
  `HEAD`, `origin/main`, and the feature reference all resolved to
  `12cfc134cc67d9cecca924534e74ab4a81284558`.
- The local `feature/matchup-lens-api` branch was deleted.
- The remote `feature/matchup-lens-api` branch was deleted.
- The approval-gated development trigger `matchup-lens-api-dev`
  (`e659b04e-153d-474d-bfcc-8f88ab08077f`) was verified
  `disabled: true` before remote-branch deletion.
- A final `git fetch --prune origin` showed no remaining
  `origin/feature/matchup-lens-api` reference.
- Local `main` remained clean and synchronized with `origin/main` at
  `12cfc134cc67d9cecca924534e74ab4a81284558`.

The Windows/OneDrive client reported failures while removing empty internal Git
parent directories under `.git/logs/refs` and `.git/refs`. The actual local
and remote branch references were removed successfully. No manual modification
inside `.git` was performed.

Repository merge and branch deletion did not change the running image. The
production service remains on revision `nfl-games-app-main-00159-hog`, built
from released source commit
`aedd1885fb03aa7760a9edefd0879bf8778fca01`.

## Next dependency

Begin Chunk G using
`documentation/New API/GameLens_Matchup_Lens_API_Chunk_G_Integration_Plan.md`.
Provide Lovable the authenticated production endpoint and map its dynamic
evidence into the existing six-lens frontend contract. Chunk G must handle
unavailable/partial responses, suppress rank-dependent output while league
context is unavailable, and never use static evidence as a live fallback.
