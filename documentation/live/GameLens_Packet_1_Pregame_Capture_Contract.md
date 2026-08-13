# GameLens Packet 1 — The Pregame Snapshot Rulebook

**Status:** Complete — rulebook written and tested; Packet 2 subsequently proved six canonical development snapshots  
**Reviewed with Christian:** 2026-08-10  
**Branch:** `dev`  
**Production behavior changed:** No  
**Production data written:** No

**Historical scope note:** This document preserves the decisions made before
Packet 2 existed, so portions intentionally describe capture as future work.
Current execution status lives in the Product Sprint and Packet 2 evidence
record. Packet 1 itself changed no production behavior.

## The short version

Your understanding is accurate:

> Before an eligible NFL game kicks off, GameLens should save one frozen copy of
> everything it was showing and saying about that game. That saved copy becomes
> the evidence used by Levels 1–4 later.

Packet 1 wrote and tested the **rules for doing that safely**. It did not run a
capture, save a game, run Level 1, change `/game`, or touch production data.

Think of Packet 1 as approving the blueprint and installing the guardrails.
Packet 2 is the first rehearsal that will actually create a saved snapshot.

## Names we will use

Earlier documents used “Gate H” because the backend rollout was divided into
temporary release gates. That name is useful only when reading the old cutover
history.

From now on, use these plain names:

| Plain name | What it does |
|---|---|
| **Daily Game Data Run** | The existing 8:00 a.m. Eastern job that refreshes Schedule, Stats, and Scores |
| **Metric Pipeline** | The existing Facts → Windowed Metrics → Rankings work |
| **Learning Pipeline** | The new, small coordinator that will eventually save pregame snapshots and run Levels 1–4 at the correct times |
| **Gate H** | Historical name for the final production-readiness check; not the name of a daily job |

The reviewed Gate H evidence showed that the production backend rollout and its
automatic run succeeded. It did **not** run Packet 1 or any learning Level.

## What should happen each morning

When the Learning Pipeline is eventually connected, the one 8:00 a.m. run should
finish in this order:

1. **Daily Game Data Run:** refresh Schedule, Stats, and Scores.
2. **Metric Pipeline:** when accepted Stats require it, rebuild Facts, Windowed
   Metrics, and Rankings.
3. **Finish older games:** check whether previously captured games now have the
   final score and Facts needed for postgame work.
4. **Prepare future games:** build and save one pregame snapshot for every
   eligible game in the today-plus-two-day Schedule window.
5. **Save a run summary:** record what succeeded, skipped, waited, or failed.

This uses the existing Scheduler. It does not add a second daily schedule.

## Which games may be captured

A production snapshot is allowed only when all five answers are yes:

| Question | Required answer |
|---|---|
| Did the game come from the stored Schedule window for today plus the next two dates? | Yes |
| Is `gameStatus` still `Scheduled`? | Yes |
| Is the capture time strictly before kickoff? | Yes |
| Is this an eligible season phase? | Regular season or postseason |
| Is the payload free of final-score and outcome information? | Yes |

A capture exactly at kickoff is too late.

### Season phase rule

Christian's review caught an important correction. Production learning must not
be limited to the regular season forever.

| Phase | Capture behavior | Learning cohort |
|---|---|---|
| Preseason | Dev/shadow rehearsal only | Never enters production Levels 1–4 |
| Regular season | Production eligible | Regular-season cohort |
| Postseason | Production eligible | Separate postseason cohort |
| Unknown or unexpected label | Skip safely and record the source values | No evidence until the label is reviewed |

Postseason includes Wild Card, Divisional Round, Conference Championship, and
Super Bowl. The schedule's `seasonType` should identify the broad phase, while
`gameWeek` identifies the round. An unfamiliar label must create a visible
skip, not crash the run.

The first Packet 1 code accepted only regular-season labels. No production
behavior changed, so no data was harmed. Before Packet 2 can receive a GO
decision, its focused contract tests must be expanded to cover postseason and
unknown labels. That small code correction belongs at the start of Packet 2.

## What is inside one frozen snapshot

The snapshot is the complete pregame product response built by the existing
`services.game_service.get_game_details(...)` logic. That includes the
pregame-facing sections such as:

- Game Profile;
- Core Area Advantage;
- Matchup Lean;
- Team Comparison; and
- Pregame Model Read / Model Trust reasoning.

It must not contain a populated final score, Model Outcome, actual winner,
result code, or final margin.

Missing rankings are allowed. Early in a season, “rankings are not available
yet” is honest information, not a failed capture.

## Where the saved information should live

Packet 1 originally described GCS objects but did not make the serving path
clear enough. The target is now explicit:

| Location | What is saved there | Why |
|---|---|---|
| **BigQuery — `GameLens_dev.pregame_snapshots`** during Packet 2 shadow work; eventual production `GameLens.pregame_snapshots` only after release approval | One searchable row per canonical game snapshot, with the exact `response_payload` stored separately from richer `evidence_context`, plus identity, phase, timestamps, source dates, versions, state/reason, hashes, `lens_tags`, and GCS object locations | Fast lookup by `/game`, Level 1, and Admin without placing learning tables in `Analytics` |
| **GCS — `gamelens_learning/<environment>/<learning_run_id>/<game_id>/<capture_id>/`** | Immutable `payload.json`, `evidence_context.json`, and `manifest.json` copies | Raw audit/backup evidence that cannot be silently replaced |
| **BigQuery — `GameLens_dev.pipeline_runs` and `GameLens_dev.stage_runs`** during Packet 2 shadow work; eventual production equivalents only after release approval | One summary per Learning Pipeline attempt plus readable stage receipts for Metric Pipeline, Snapshot Capture, and Levels 1–4 | Lets Admin show what ran, waited, skipped, or failed without requiring a log search |
| **Existing BigQuery claim table** | Level 1 claim rows linked back to the saved `capture_id` | Feeds Levels 2–4 without creating a second claim system |

The exact BigQuery field list, response/evidence separation, save-finalization
protocol, and partitioning are Packet 2 design details, but the responsibility
is decided: **BigQuery is the normal serving and monitoring source; GCS is the
immutable raw copy.** Packet 2's `GameLens_dev` locations supersede the earlier
provisional `Analytics.gamelens_*` names without changing Packet 1's safety
contract.

A retry with the same identity and identical payload becomes a clean no-op. A
different payload trying to replace the already-frozen game is quarantined and
shown in the run summary.

## What `/game` should do

The public `/game/<game_id>` request must not run Level 1 or rebuild this
analysis because someone opened the page.

The intended production path is:

```text
8:00 a.m. backend work
→ build the pregame response once
→ save it in BigQuery
→ save its immutable raw copy in GCS
→ /game reads the already-saved snapshot
→ frontend renders it
```

Internally, the capture service may call `game_service` once to build the
snapshot. It must not call the public HTTP route, and it must use explicit
read-only behavior so building a pregame snapshot cannot save an outcome.

The current public `/game` behavior has not changed yet. Packet 2 proves the
shadow write; a later controlled packet proves the saved-snapshot read before
production wiring. Until that release gate, the existing frontend continues to
work as it does today.

## What Level 1 does after capture exists

Capture and Level 1 are closely connected, but they are not the same action:

1. **Capture** saves the complete frozen pregame response.
2. **Level 1** reads that saved response and extracts the individual statements
   GameLens made, such as comparison or advantage claims.
3. Those claim rows keep a link back to the exact snapshot they came from.

Level 1 never needs to run when a user calls `/game`. It runs in the scheduled
backend workflow against data that has already been saved.

## The two postgame readiness checks

These checks apply to the **same game after it finishes**. They are unrelated to
the other dates in the today-plus-two-day Schedule window.

| Readiness check | What must exist | What may happen |
|---|---|---|
| **Game result check** | Frozen pregame snapshot + final score | Grade the saved Matchup Lean and reveal Model Outcome/Trust |
| **Claim learning check** | Frozen snapshot + final score + accepted completed-game Facts | Level 2 validates individual claims, then Level 3 attaches pregame-safe feature context |

Example: if the final score arrives before the complete box score, GameLens may
show the game-level result while Levels 2–3 wait. Waiting is not a failure and
does not recreate Level 1.

Level 4 waits for a later weekly batch with enough completed evidence.

## What becomes visible as each Level finishes

| Stage | Public game page | Admin / internal view |
|---|---|---|
| Snapshot saved | Pregame cards can come from the frozen response | Capture status, timestamp, source dates, versions, missing-data reasons |
| Level 1 | No new public card; it preserves the claims behind the pregame read | Claim count and trace back to the snapshot |
| Final score available | Final score may appear | Score readiness state |
| Model Outcome/Trust graded | Outcome section may appear after final | Grade, reason, and snapshot identity |
| Level 2 | No automatic public change | Claim validation results |
| Level 3 | No automatic public change | Feature/context health |
| Level 4 | No automatic public change | Weekly calibration and “What did we learn?” |

This distinction lets every packet be tested through a dev frontend and/or
Admin view without pretending all four Levels are separate public cards.

For the six preseason games expected on Thursday, dev/shadow may rehearse the
snapshot and frontend-read path. Those rows must be labeled preseason shadow
data and excluded from production learning evidence.

## Important edge cases

- No games in the window → successful no-op.
- A game exists but is no longer scheduled → skip it.
- Kickoff has arrived → do not capture or reconstruct it.
- Rankings are missing → save the truthful snapshot and reason.
- Expected source data is blank or malformed → fail only that game and preserve
  the others.
- The same capture is retried → no duplicate.
- A game is postponed or rescheduled → keep the old audit record and create only
  the correctly identified future capture.
- Final score exists but Facts do not → grade the game, but Levels 2–3 wait.
- Snapshot is missing after kickoff → show the score, but do not invent learning
  evidence.

## DRY promise

This framework reuses:

- the existing Schedule loader and today-plus-two-day window;
- the existing Metric Pipeline;
- `services/game_service.py` as the only GameLens payload builder;
- the existing Level 1 claim extractor;
- the existing final-score query;
- the existing Levels 2–4 workers;
- `metric_registry.py` as the source of truth for `lens_tags`; and
- the existing Admin query/service layer.

The new Learning Pipeline coordinates those pieces and records their results. It
does not re-create their football logic.

## Evidence from Packet 1

Fourteen focused tests passed for the original rulebook: timing, scheduled
status, preseason shadow isolation, postgame-field rejection, missing rankings,
stable identities, retry/no-op/conflict behavior, no-game days, and the two
postgame readiness checks.

Because Christian's review expanded production eligibility to postseason, the
postseason/unknown-phase tests are still required before Packet 2 is declared
complete.

## Stop/go decision

Packet 1's framework is understood and preserved. Production remains unchanged.

Packet 2 was authorized under these constraints and later passed its one-game
proof, deterministic retry, six-game slate rehearsal, exact live-`/game`
parity, per-game observability amendment, and coverage audit. Packet 1 remains
the capture rulebook; Packet 2 is the execution evidence. Packet 3 is now the
next reviewed plan and must continue to avoid `app.py` wiring, production
Level 1 writes, and frontend changes until its own release boundary.
