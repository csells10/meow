# GameLens Product Data Collection and Learning Handoff

**Document status:** Draft v0.1  
**Created:** 2026-08-03  
**Owner:** Senior Product Manager / GameLens product stewardship  
**Repository:** `csells10/meow`  
**Branch represented:** `main`  
**Current production checkpoint:** Gate G complete; Gate H pending the first scheduled production run  
**Companion release plan:** [go_plan.md](./go_plan.md)

---

## 1. Executive read

GameLens now has a season-aware, error-walled path that can move newly accepted game data through:

```text
Schedule -> Stats -> Scores
                  |
                  v
        accepted Stats game gate
                  |
                  v
Facts -> Windowed Metrics -> Rankings -> /game
```

This is a major improvement over the previous hard-coded annual trigger. The active season comes from runtime configuration, the builders run in a fixed order, each stage reports its own status and row count, and a failed metric stage prevents the remaining metric stages from being presented as successful.

The next product problem is different:

> How do we collect trustworthy 2026 evidence so GameLens can learn whether its pregame claims held up after each game, and eventually hand that evidence to a future product, data, or engineering owner?

The existing Levels 0–4 system is the foundation for that learning loop, but it is **not currently part of the scheduled production ETL**. This document separates what is already live from what still needs to be designed and proven.

---

## 2. One important logic correction

The idea that “Levels 1–4 should run after successful Scores and Stats ETL” is directionally correct, but the full lifecycle cannot start there.

**Level 1 must use a payload captured before kickoff.**

If GameLens waits until final Scores and Stats are available to create the original claim record, the supposed pregame claim may contain postgame knowledge. That would invalidate the learning loop.

The correct timing is:

```text
Before kickoff
  -> capture an immutable /game-style pregame payload
  -> Level 1 extracts and stores the claims

After the game is final and Stats are accepted
  -> rebuild Facts -> Windowed Metrics -> Rankings
  -> Level 2 validates the saved pregame claims against postgame facts
  -> Level 3 adds pregame-safe feature context
  -> Level 4 summarizes which claim patterns deserve stronger, measured,
     softened, blocked, or metadata-only treatment
```

Levels 2–4 can follow completed-game ETL. Level 1 cannot be postponed until then.

---

## 3. Product intent

The learning system should answer:

> When GameLens made a specific pregame matchup claim, did postgame data support that claim?

It should not silently turn into:

- winner prediction;
- betting guidance;
- a matchup-lean override;
- an automatic High Confidence generator;
- a Model Trust override; or
- a frontend copy rewrite without an explicit release decision.

A team may lose while a specific pregame claim still validates. Claim quality and winner accuracy are separate product questions.

---

## 4. Current production data path

### 4.1 Scheduled ingestion

The production application preserves this order:

```text
Schedule -> Stats -> Scores
```

The application then evaluates the number of **successfully accepted Stats games**.

Important meanings:

- Stats success means accepted games, not inserted row count.
- Zero accepted Stats games is a successful, explicit no-op.
- One or more accepted Stats games triggers the season-aware metric conductor with writes enabled.
- Scores are still collected and truthfully reported, but the current metric-pipeline gate is accepted Stats games.
- A partial or failed ingestion stage remains visible in the execution summary.
- A failed metric pipeline is returned as a failure rather than hidden behind HTTP 200.

### 4.2 Metric preparation

The conductor runs a deterministic full season-to-date rebuild in this exact order:

```text
Analytics.game_team_metric_facts_{season}
  -> Analytics.team_metrics_windowed_{season}
  -> Analytics.team_metric_rankings_{season}
```

Each builder validates that it produced a non-empty DataFrame. A failed stage stops the remaining stages. The default write behavior replaces the season-specific output table, which recalculates rolling windows and league rankings consistently after newly completed games.

### 4.3 `/game`

`/game` reads the prepared metric and ranking data and can degrade safely when early-season rankings do not exist.

The current response also computes claim-language metadata such as:

- `two_way_context_v1`;
- `offensive_efficiency_support_v1`;
- row-level `language_support`; and
- claim-strength metadata.

That runtime metadata is **not proof that the persisted Levels 1–4 learning pipeline ran**. The runtime layer currently derives bounded metadata from the available matchup objects and allowlists. Persisted historical calibration remains a separate workflow.

---

## 5. Levels 0–4: current truth

| Level | Purpose | Current implementation | Production automation status |
|---|---|---|---|
| Level 0 | Create or migrate the claim-training table | `create_claim_training_examples_table.py` | Setup-only; not a daily job |
| Level 1 | Extract one row per pregame GameLens claim | `build_claim_training_examples.py` reads saved payload JSON files | Working for QA/historical runs; not connected to production scheduling |
| Level 2 | Validate pregame claims against postgame facts | `update_claim_training_validation.py` reads `game_team_metric_facts_{season}` | Working for historical runs; not connected to production scheduling |
| Level 3 | Add pregame-safe engineered feature context | `update_claim_training_features.py` | Working and historically validated; not connected to production scheduling |
| Level 4 | Aggregate claim-language calibration evidence | `build_claim_language_calibration.py` | Working as an analysis/calibration job; not a production runtime steering job |
| Runtime exposure | Present safe metadata in `/game` | `claim_language_features.py`, `claim_language_response.py`, `game_service.py` | Active metadata layer; guarded and non-steering |

### Current Level 3 and Level 4 guardrails

Preserve these boundaries:

- feature engineering must remain pregame-only;
- Level 3 must not use final score, winner, `validation_result`, `actual_gap`, or other postgame outcomes as feature inputs;
- claim validation rate is not winner accuracy;
- `offensive_efficiency_support_v1` remains metadata-only;
- `metadata_only = true` and `language_boost_allowed = false` remain the safe defaults for that feature;
- claim-language support must not change matchup lean, outcome confidence, Model Trust, or winner logic;
- `core_area_durability_context_v0` remains audit/Admin-only and must not change production confidence labels yet.

---

## 6. Data that must be collected

### 6.1 Pregame snapshot record

For every eligible game, retain enough information to prove what GameLens knew before kickoff:

| Field | Why it matters |
|---|---|
| `game_id`, season, scheduled kickoff | Stable identity and timing |
| payload capture timestamp | Proves the snapshot was pregame |
| game status at capture | Prevents final-game contamination |
| complete `/game`-style payload | Source for Level 1 claim extraction |
| metric/ranking source dates | Shows how fresh the evidence was |
| model, feature, and formula versions | Makes later comparisons reproducible |
| capture status and error | Prevents silent gaps |
| immutable storage location or object identifier | Makes the evidence auditable |

The production collector must be explicitly read-only. It should not rely on final-game `/game` side effects. The current QA collector protects itself by disabling `save_model_results`; a production design should make this separation intentional rather than depend on monkeypatching.

### 6.2 Completed-game ETL record

For each scheduled run, retain:

- execution timestamp and active season;
- selected game IDs;
- Schedule, Stats, and Scores stage statuses;
- accepted Stats game count;
- failed-game counts and failure reasons;
- Facts, Windowed Metrics, and Rankings statuses;
- row counts for all three metric stages;
- failed metric stage, when applicable;
- total runtime;
- Cloud Run revision; and
- explicit success, no-op, partial-failure, or failure outcome.

### 6.3 Claim-learning record

For each learning run, retain:

- payload run ID;
- Level 1 run ID and claim-row count;
- unique game and claim counts;
- Level 1 extraction errors;
- Level 2 validation counts by result;
- unavailable validation count;
- Level 3 formula version and feature bucket counts;
- Level 4 calibration version and recommendation counts;
- BigQuery write mode;
- dry-run evidence location; and
- reviewer decision.

---

## 7. Proposed production lifecycle

The learning lifecycle should use separate pregame and postgame checkpoints.

### Checkpoint A — pregame evidence capture

Run only when:

- the game is scheduled and not final;
- the relevant season metrics/rankings are available or the payload can truthfully record their absence; and
- kickoff has not occurred.

Output:

- immutable payload snapshot;
- capture manifest;
- no claim validation and no production language change.

### Checkpoint B — Level 1 claim extraction

Run from the immutable pregame snapshot.

Output:

- one row per pregame claim;
- idempotent run identity;
- dry-run review before the first BigQuery write.

### Checkpoint C — completed-game data preparation

Run through the current scheduled production path:

```text
Schedule -> Stats -> Scores -> accepted Stats gate
-> Facts -> Windowed Metrics -> Rankings
```

Output:

- truthful ETL summary;
- season-specific postgame facts ready for validation;
- refreshed data for `/game`.

### Checkpoint D — Levels 2 and 3

Run only for games that have both:

- an accepted pregame Level 1 claim set; and
- completed-game facts.

Output:

- Level 2 validation labels;
- Level 3 pregame-safe feature metadata;
- no automatic runtime copy or confidence changes.

### Checkpoint E — Level 4 calibration

Run as a controlled batch after enough newly validated claims exist. It does not need to run after every individual game.

Output:

- calibration summary rows;
- sample sizes and validation rates;
- language action recommendations;
- explicit metadata-only or promotion decision.

**Product recommendation:** begin with a weekly or evidence-threshold batch, not a per-game Level 4 trigger. Calibration needs enough rows to avoid reacting to tiny samples.

---

## 8. Phased delivery plan

| Phase | Goal | Exit evidence | Product effect |
|---|---|---|---|
| P0 | Complete Gate H | First scheduled production ETL has truthful stage evidence | Confirms the new metric path in production |
| P1 | Define pregame capture contract | Timing, storage, idempotency, and read-only behavior approved | No user-facing change |
| P2 | Shadow-capture 2026 pregame payloads | Captures are pre-kickoff, complete, and reproducible | No learning writes yet |
| P3 | Prove Level 1 on 2026 shadow data | Dry-run counts reviewed; duplicate run is safe | Claim rows become available |
| P4 | Prove Level 2 after completed games | Postgame facts match the saved claims; unavailable rows explained | Validation evidence becomes available |
| P5 | Prove Level 3 and Level 4 in shadow mode | Feature and calibration summaries pass guardrails | No runtime steering |
| P6 | Handoff and adoption decision | Evidence packet, rollback boundary, and owner sign-off complete | Separate decision for Admin/frontend use |

Do not start P1 operational work until Gate H closes the current production cutover.

---

## 9. Acceptance criteria

The first 2026 learning-loop pilot is successful when:

- every included game has exactly one approved immutable pregame snapshot;
- every snapshot timestamp is before kickoff;
- no final score or postgame metric leaks into Level 1 or Level 3 inputs;
- accepted Stats games trigger the correct season-specific metric tables;
- failures and no-ops are distinguishable;
- the three metric stages report credible row counts;
- `lens_tags` remains a repeated string in BigQuery and an array in `/game`;
- Level 1 can be rerun without uncontrolled duplicate claims;
- Level 2 explains unavailable validations rather than hiding them;
- Level 3 records its formula version;
- Level 4 records sample sizes with every recommendation;
- metadata-only features remain non-steering;
- no learning step changes matchup lean, winner logic, Model Trust, or production copy without a separate release gate; and
- another owner can reproduce the run from the handoff evidence.

---

## 10. Senior Product Manager evidence log

Add one row after each meaningful checkpoint.

| Date | Season / games | Checkpoint | Result | Evidence | Risk or gap | Decision | Next owner/action |
|---|---|---|---|---|---|---|---|
| 2026-08-03 | 2026 | Gate G promotion | Passed | See `go_plan.md` Gate G | First scheduled production ETL not yet observed | Hold at Gate H | Observe Aug. 4 scheduled run |
| TBD | TBD | Pregame capture contract | Pending | — | Timing/storage not selected | — | — |
| TBD | TBD | Level 1 pilot | Pending | — | Production payload source not wired | — | — |
| TBD | TBD | Level 2 pilot | Pending | — | Requires completed facts | — | — |
| TBD | TBD | Levels 3–4 shadow run | Pending | — | Requires reviewed sample size | — | — |

For each update, record evidence rather than only changing a status label.

---

## 11. Open product decisions

These are intentionally unresolved in Draft v0.1:

1. **Pregame capture timing:** how long before kickoff should the canonical snapshot be taken, and what happens when rankings are not yet available?
2. **Immutable storage:** where should production pregame payloads and manifests live?
3. **Run identity:** should Level 1 use one run per game, slate, week, or season batch?
4. **Scope:** should the first shadow pilot capture every 2026 game or a controlled preseason sample?
5. **Level 4 cadence:** weekly, per completed week, or after a minimum number of new validated claims?
6. **Admin visibility:** which evidence should appear in an internal QA view before any frontend adoption?
7. **Runtime adoption:** what additional validation would allow a metadata-only feature to affect language, if ever?

Draft v0.1 makes no production decision on these questions.

---

## 12. Known feature evidence to preserve

The current feature work supports these product conclusions:

- `two_way_context = supportive` is a repeatable claim-support signal, not a winner signal.
- `offensive_efficiency_support_v1` found above-baseline claim-validation pockets and correctly isolated caution-only metrics.
- `points_per_play` is the current offensive-efficiency anchor.
- `td_rate`, `red_zone_efficiency`, and `turnover_margin_per_game` remain volatile/caution-only for automatic stronger language.
- `third_down_pct` and `1st_down_rate` remain context-only in the offensive-efficiency feature.
- broad Scoring Efficiency must not be treated as uniformly trustworthy.
- Core Area durability is promising for High-to-Medium confidence softening, but remains an audit feature.

These are evidence-backed boundaries, not permanent football laws. New 2026 data should test whether they continue to hold.

---

## 13. Handoff packet contents

A future handoff should include:

1. This living product document.
2. The latest production cutover evidence from `go_plan.md`.
3. The pregame capture manifest.
4. The scheduled ETL execution summary.
5. Level 1–4 run IDs and local/BigQuery output locations.
6. Data-quality exceptions and unresolved game IDs.
7. Feature and formula versions.
8. Guardrail verification.
9. Decisions made and decisions intentionally deferred.
10. The exact next bounded action.

The handoff should let a new owner answer four questions quickly:

- What ran?
- What data changed?
- What evidence passed or failed?
- What is the next safe decision?

---

## 14. Source references

### Product and feature definition

- [GameLens Feature Guide Book](../Gamelens_Feature_Guide_Book_20260524.md)
- [Offensive Efficiency Support](../Features/Feature_Offensive_Efficiency_Support.md)
- [Two-Way Context](../Features/Feature_two_way_context.md)
- [Runtime Claim Language Support](../Features/Feature_two_way_Runtime%20Claim%20Language%20Support%20Integration.md)
- [Claim-Strength Metadata](../Features/Feature_Claim_Strength_Metadata.md)
- [Core Area Durability Context](../Features/Feature_Core_Area_Durability_Context.md)
- [Claim Training and Level 4 Roadmap](../GameLens_Claim_Training_and_Level4_Roadmap.md)

### Current production and implementation

- [Controlled Production Cutover Plan](./go_plan.md)
- `../../app.py`
- `../../services/gamelens_metric_pipeline_conductor.py`
- `../../agg/build_metric_facts.py`
- `../../agg/build_windowed_metrics.py`
- `../../agg/build_metric_rankings.py`
- `../../qa_collect_gamelens_payloads.py`
- `../../agg/gamelens_training/build_claim_training_examples.py`
- `../../agg/gamelens_training/update_claim_training_validation.py`
- `../../agg/gamelens_training/update_claim_training_features.py`
- `../../agg/gamelens_training/build_claim_language_calibration.py`

---

## 15. Next bounded action

Complete Gate H exactly as documented in [go_plan.md](./go_plan.md).

After the first scheduled production run is proven, return to this document and decide only P1:

> Define the pregame capture contract—timing, immutable storage, idempotency, and read-only behavior.

Do not wire Levels 1–4 into `app.py` as part of Gate H.
