# GameLens Packet 2 — Shadow Pregame Snapshot Plan

**Status:** Planning review; no implementation started  
**Created:** 2026-08-10  
**Branch:** `dev`  
**Production behavior changed:** No  
**Production data written:** No

## What Packet 2 is for

Packet 2 is the first safe rehearsal of the framework.

It will take one eligible scheduled game, build the normal pregame GameLens
response once, save it, and prove that an identical retry does not create a
second copy. Preseason is useful here because it lets us test the plumbing
without letting the result enter production Levels 1–4 evidence.

## What Packet 2 is not

Packet 2 does not:

- connect the Learning Pipeline to the 8:00 a.m. production run;
- run production Level 1;
- change the live `/game` response;
- change the frontend;
- grade a finished game;
- run Levels 2–4; or
- apply new calibration rules.

## Decisions that must be reviewed before code

1. Production phases are regular season and postseason. Preseason is
   dev/shadow only; unknown phases skip safely.
2. `services.game_service.py` remains the one response builder.
3. The capture path calls that service in-process with explicit read-only
   behavior; it never calls the public HTTP endpoint.
4. BigQuery is the normal lookup source.
5. GCS keeps the immutable raw payload and manifest.
6. The first valid capture is canonical; retries cannot silently replace it.

## Proposed saved records

### BigQuery snapshot row

Proposed table:

```text
Analytics.gamelens_pregame_snapshots
```

One row represents one canonical game snapshot. It should contain:

- `capture_id`, `learning_run_id`, `pipeline_run_id`, and `game_id`;
- environment, season, season phase, week, and scheduled kickoff;
- capture timestamp and state;
- the complete pregame payload in a BigQuery JSON field;
- metric/ranking source dates and ranking availability;
- model, ruleset, feature, and formula versions;
- payload hash;
- GCS payload/manifest object locations; and
- a plain skip, quarantine, or error reason when appropriate.

The game and capture identities must prevent duplicate canonical rows.

### GCS raw copy

Proposed location:

```text
gamelens_learning/<environment>/<learning_run_id>/<game_id>/<capture_id>/
```

It contains immutable `payload.json` and `manifest.json` objects.

### Learning run summary

Proposed table:

```text
Analytics.gamelens_learning_pipeline_runs
```

Packet 2 needs only a small shadow-run record: run identity, environment,
started/finished time, overall state, games checked/captured/skipped/
quarantined/failed, and plain reasons. Later packets add their own stage counts
to the same row shape.

## Thursday preseason rehearsal

For the expected preseason slate:

1. Refresh the normal schedule/data inputs.
2. Select one scheduled game before kickoff.
3. Build its pregame response in read-only mode.
4. Save it as `preseason_shadow` in isolated dev storage.
5. Read it back through the dev snapshot-reading path.
6. Compare the visible pregame sections with the normal response.
7. Repeat the exact capture and prove no second canonical row/object appears.

If usable metrics or rankings are missing, the snapshot should still save the
honest absence and its reason. That is valuable early-season testing.

## What Christian should be able to inspect

After the shadow rehearsal, the review should show:

| View | Evidence |
|---|---|
| Dev frontend or response preview | Game Profile, Core Area Advantage, Matchup Lean, Team Comparison, and Pregame Model Read from the saved snapshot |
| BigQuery snapshot row | Game/capture identity, phase, timestamps, state, source dates, versions, full payload, and GCS locations |
| GCS objects | One payload and one manifest |
| Admin/readable run summary | One game captured, skipped, or failed with a plain reason |
| Retry evidence | Same capture identity and unchanged row/object counts |

## Required edge-case checks

- no games scheduled;
- games present but none eligible;
- scheduled game before kickoff;
- exactly-at-kickoff rejection;
- regular-season eligibility;
- postseason eligibility;
- preseason shadow-only behavior;
- unknown phase safe skip;
- missing rankings;
- blank/malformed payload;
- identical retry;
- conflicting retry; and
- no Model Outcome write.

## DRY boundary

Packet 2 may add a thin capture service and storage adapter. It must reuse the
existing Schedule data, game service, identity/eligibility contract, configured
environment clients, and summary patterns. It must not copy matchup or claim
logic.

## Stop/go questions

Packet 2 receives GO only when Christian can answer yes to all of these:

- Can I see exactly which game was captured and when?
- Can I open the saved payload without searching logs?
- Can the dev frontend/read path use the saved payload?
- Did the retry create zero duplicates?
- Is preseason clearly excluded from production evidence?
- Are regular season and postseason both supported by the rule?
- Did the capture avoid every outcome write?
- Is production still unchanged?

## Documentation handoff

When Packet 2 is implemented, this file must be updated with exact files,
focused tests, observed row/object counts, production impact, commit, and the one
next packet. Packet 3 does not begin until that review is understandable.
