# Chunk F — Release Handoff

**Status:** in progress — approved dev rehearsal pending  
**Verdict:** **BLOCKED pending deployment and authenticated verification**  
**Branch:** `feature/matchup-lens-api`  
**Starting branch head:** `f89f684ac6265f138f3903c3329bf94cc05f3294`  
**Assigned model / effort:** `gpt-5.6-sol` / Medium  
**Scope:** Chunk F release coordination only

## Purpose

Make the verified Matchup Lens backend reachable through staged, reversible
release steps. This pre-release record intentionally does not claim candidate
or production availability.

No implementation, test, fixture, route, query, service, dependency, runtime
configuration, pipeline, schema, table, scheduled job, learning flow, or
orchestrator flow is changed by this commit.

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

## Release inspection

### Production mechanism

The checked-in `cloudbuild.yaml` builds and pushes
`us-central1-docker.pkg.dev/$PROJECT_ID/meow-main/main:$COMMIT_SHA`, then
deploys `nfl-games-app-main` in `us-central1` with `--no-traffic` and the
`packet4-candidate` tag. A successful build therefore creates a tagged
candidate revision; it does not promote production traffic.

Production trigger state, current production revision, candidate URL, and
production rollback target must be confirmed before any production build or
traffic action.

### Dev rehearsal mechanism

A dedicated approval-gated GitHub trigger was created for the release rehearsal:

- trigger name: `matchup-lens-api-dev`
- trigger ID: `e659b04e-153d-474d-bfcc-8f88ab08077f`
- source repository: `csells10/meow`
- branch regex: `^feature/matchup-lens-api$`
- build file: `cloudbuild-dev.yaml`
- approval required: true
- build service account:
  `362530996210-compute@nfl-stream-406420.iam.gserviceaccount.com`

The dev build creates
`us-central1-docker.pkg.dev/$PROJECT_ID/meow-main/dev:$COMMIT_SHA` and deploys
`nfl-games-app-dev` in `us-central1`. Unlike the production build, the dev
build moves dev traffic directly to the new revision after build approval.

## Dev baseline and rollback

Before the rehearsal:

- service: `nfl-games-app-dev`
- URL: `https://nfl-games-app-dev-ids7lwjjta-uc.a.run.app`
- current ready revision: `nfl-games-app-dev-00159-dfk`
- current traffic: 100% to `nfl-games-app-dev-00159-dfk`
- current source label:
  `4c84550ddc60f03b0cfd5d17cf91d892832eb90d`
- current image digest:
  `sha256:5a5a57e72f6d67677ff619524cbebc6400efa5f679b40007f8f253ba82d92bb7`
- runtime service account:
  `gamelens-dev-replay@nfl-stream-406420.iam.gserviceaccount.com`

Approved dev rollback action:

```bash
gcloud run services update-traffic nfl-games-app-dev \
  --project=nfl-stream-406420 \
  --region=us-central1 \
  --to-revisions=nfl-games-app-dev-00159-dfk=100
```

Rollback has not been used.

## Dev isolation

The dev service is intentionally configured for:

- environment: `dev`
- run mode: `controlled_replay`
- active season: `2025`
- replay date: `2025-09-18`
- datasets: `League_dev`, `Scores_dev`, and `Analytics_dev`
- raw bucket: `nfl-stream-406420-gamelens-dev-raw`

The dev rehearsal can prove image deployment, process health, route
reachability, authentication, existing endpoint reachability, and safe
read-only behavior. It cannot prove the required 2026 DET/BUF production
evidence or close Chunk F.

## Approved dev verification plan

After the approval-gated build deploys the exact handoff commit:

1. Record build ID, image digest, revision, start/end time, and result.
2. Confirm the revision maps to the approved commit and retains the dev runtime
   service account and isolated runtime settings.
3. Confirm `/health` returns HTTP 200.
4. Confirm unauthenticated Matchup Lens access retains the established
   authentication rejection.
5. Use authenticated read-only `/games` output to select a canonical 2025 dev
   game.
6. Confirm authenticated `/game/<game_id>` remains reachable.
7. Confirm authenticated `/game/<game_id>/lens-context` reaches the new route
   and returns only safe documented output for the available dev evidence.
8. Do not invoke scheduler, ingestion, replay, pipeline, learning, orchestrator,
   or BigQuery write paths.
9. If deployment succeeds but verification fails, execute only the approved
   rollback above.

## Production checkpoint

A dev pass is rehearsal evidence only. Before production, Chunk F must still
present and obtain approval for:

- the exact production candidate commit;
- production trigger and source-ref behavior;
- current production revision and traffic;
- no-traffic candidate URL and authenticated request method;
- exact promotion command;
- exact rollback revision and command.

Production promotion requires a separate approval after the authenticated
no-traffic candidate passes. No merge to `main` is authorized.

## Repository scope

Exact changed path in this pre-release commit:

- `documentation/New API/handoffs/Chunk_F_Release_Handoff.md`

## Next dependency

The new commit created by this handoff should queue the approval-gated dev
build automatically. Stop at the build approval gate, confirm the queued source
commit, and obtain Christian's approval before execution.
