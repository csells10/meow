# GameLens QA Testing Notes — Next-Day Handoff

_Last updated from the 96-game Level 2 QA v2 run._

## TLDR

You are **not overthinking this**. The current results say GameLens has useful matchup structure, but the next step should be **better data validation + feature engineering**, not a full model rewrite.

The important shift is this:

> Do not only ask, “Did GameLens pick the winner?”  
> Ask, “Did the actual football claim happen?”

Examples:

- If GameLens says **MIN generated more pressure**, did MIN actually lead postgame pressure/disruption proxies?
- If GameLens says **Green Bay had Offensive Output**, did Green Bay actually lead the postgame offensive-output metrics?
- If GameLens gives a claim an **Elevated** or strong status, did the postgame evidence justify that level of language?

That is not silly. That is the right path if the goal is trustworthy matchup explanation.

---

## What We Have Built So Far

### Level 1 QA: Payload Baseline Collector

File used:

```bash
qa_collect_gamelens_payloads_bucket_v2.py
```

Purpose:

- Pull full local GameLens payloads.
- Avoid clicking around the UI repeatedly.
- Build a stable sample across seasons and schedule buckets.
- Produce `game_snapshots.csv`, `qa_review.md`, and `chatgpt_analysis_packet.json`.

Main value:

> Establishes whether GameLens picked correctly, missed, produced No Pick, or had confidence-label problems.

---

### Level 2 QA: Claim Validation

Current best file:

```bash
qa_validate_gamelens_claims_v2.py
```

Purpose:

- Reads existing GameLens payloads.
- Pulls postgame actual metric facts from BigQuery.
- Validates whether the pregame claims actually happened postgame.
- Separates headline claims from supporting claims.

Key output folder from latest run:

```text
qa/gamelens_claim_validation_runs/claims_96_v2
```

Main value:

> Establishes whether GameLens was right for the right reasons, wrong for the wrong reasons, or correct but with shaky reasoning.

---

## Current 96-Game QA Baseline

Latest Level 2 QA v2 run:

- **96 games selected**
- **32 games each** from 2023, 2024, and 2025
- **6,850 postgame metric rows loaded**
- **2,880 claims validated**
- Actual source used: `facts`
- Sample spread:
  - 24 early regular season games
  - 24 mid regular season games
  - 30 late regular season games
  - 18 postseason games

Run command used:

```bash
python qa_validate_gamelens_claims_v2.py --payload-run qa/gamelens_payload_runs/baseline_32_v2 --run-name claims_96_v2
```

---

## What The 96-Game Results Revealed

### 1. Headline claims are better than before, but still not strong enough

Headline claims validated at about **46%**.

Supporting claims validated at about **46%**.

That is useful, but not strong enough to say:

> “Several claims point one way, therefore confidence should be High.”

Better interpretation:

> GameLens has useful directional signal, but confidence needs stricter promotion rules.

---

### 2. Claim types are all clustered in the mid/high-40s

Approximate validation rates from the 96-game run:

| Claim Type | Validation Rate | Read |
|---|---:|---|
| Core Area Comparison | 48.3% | Best current layer, but still not dominant |
| Core Area Summary | 47.8% | Similar to Core Area Comparison |
| Team Comparison Metric | 46.9% | Useful, but some availability/noise issues |
| Metric Highlight | 46.4% | Useful, especially as context |
| Category Summary | 44.2% | Lower-confidence support layer |
| Game Profile | 42.6% | Needs caution, especially pressure/turnover claims |

Current read:

> Core Areas are directionally useful, but no single layer should be allowed to “steer the car” alone.

---

### 3. High Outcome Confidence is not calibrated cleanly yet

A major finding:

> High Outcome Confidence does not clearly validate better than Medium or Low in the current sample.

That does **not** mean the model is broken.

It means:

> The confidence promotion rules are too permissive.

High confidence should probably require:

- Strong headline claim support
- Strong Core Area alignment
- Strong rank/percentile context
- No major conflict flags
- No obvious Week 18/playoff/context caution unless the signal is overwhelming
- Actual gap quality, not just “team A is better than team B”

---

### 4. Game-level buckets are now useful

The 96-game v2 run produced these reasoning buckets:

| QA Bucket | Count | Meaning |
|---|---:|---|
| Good reasoning + correct outcome | 17 | Best cases: right team, good explanation |
| Bad reasoning + bad outcome | 17 | Highest-priority model-learning cases |
| Outcome correct but reasoning mixed | 19 | Correct pick, but story was shaky |
| No Pick claim review | 28 | Restraint cases; some may hide a lean |
| Mixed review | 8 | Not clean either way |
| Good reasoning + bad outcome | 1 | Useful read, bad football outcome/variance/context |

This is much better than winner accuracy alone.

---

## Highest-Priority Review Buckets

### Bucket A — Bad Headline Reasoning + Bad Outcome

These are the most valuable model-learning games.

They are not just “the pick lost.” They are cases where the main GameLens story did not validate either.

Examples from the 96-game run:

```text
20231023_SF@MIN
20231119_LAC@GB
20231210_JAX@CLE
20240114_GB@DAL
20241017_DEN@NO
20241229_LV@NO
20250111_LAC@HOU
20250113_MIN@LAR
20250118_WSH@DET
20251127_CIN@BAL
20260104_WSH@PHI
```

Review question:

> Did GameLens overvalue season aggregate data, miss recent form, miss injuries, miss QB volatility, or overweight noisy categories like turnovers/pressure?

---

### Bucket B — Outcome Correct But Headline Reasoning Mixed

These are sneaky.

GameLens got the winner/lean right, but the main explanation was not strongly validated.

Examples:

```text
20231130_SEA@DAL
20231224_GB@CAR
20231231_NE@BUF
20240120_HOU@BAL
20240922_HOU@MIN
20250105_WSH@DAL
20251026_SF@HOU
```

Review question:

> Was GameLens lucky, or did it have a real signal that the claim validator is not measuring well yet?

---

### Bucket C — No Pick With Strong Validated Claims

These are possible missed cautious-lean games.

Examples from the 96-game run:

```text
20231008_CAR@DET
20231124_MIA@NYJ
20240115_PIT@BUF
20241201_PIT@CIN
20241225_BAL@HOU
20251019_WSH@DAL
20251124_CAR@SF
20251228_NYG@LV
```

Review question:

> Did GameLens correctly show restraint, or were there enough validated headline claims to deserve a cautious lean?

Important: Do **not** automatically punish No Pick. Some No Picks are good. The goal is to identify when “No Clear Edge” might be too passive.

---

## Known Cleanup / Bug

### Tie handling needs fixing

Game:

```text
20250928_GB@DAL
Final: GB 40 - DAL 40
```

This should be treated as:

```text
Tie / Push / No Decision
```

Not a normal incorrect result.

Why it matters:

- It can pollute miss buckets.
- It can distort High Outcome Confidence review.
- It makes outcome scoring less trustworthy.

Priority: **small but important**.

---

## Key Product / Model Insight

The current problem is not:

> “GameLens is useless.”

The current problem is:

> “GameLens sometimes promotes directional evidence into stronger language than the evidence deserves.”

So the next improvement should be:

> Better evidence thresholds for strong language.

Language scale should probably be something like:

| Data Support | Suggested Language |
|---|---|
| Weak or split | No clear edge / mixed profile |
| Directional but not strong | slight lean / thin edge |
| Multiple headline claims agree | clear lean |
| Strong headline + percentile/rank support + low conflict | elevated / strong profile |
| Strong profile + context-safe + historically reliable combo | high outcome confidence |

---

## Is It Silly To Predict Whether A Claim Actually Happened?

No. It is not silly.

It is probably the right next step.

But the target should be framed carefully.

Do **not** ask:

> “Can we perfectly predict postgame pressure?”

Better question:

> “Does the pregame data justify saying this team has an elevated pressure advantage?”

That is a product-language and model-calibration question, not a perfect football prediction question.

---

## Recommended Next Feature Engineering Direction

### Build a claim-level feature dataset

Future file idea:

```text
claim_feature_matrix.csv
```

Possible row grain:

```text
one row per game_id + team + claim_group/core_area
```

Example rows:

```text
20231023_SF@MIN | SF | Pressure
20231023_SF@MIN | SF | Defensive Control
20231023_SF@MIN | MIN | Offensive Output
```

---

### Suggested columns

#### Game identifiers

```text
game_id
season
game_week
game_date
bucket
away
home
team
opponent
home_away
```

#### Pregame signal fields

```text
claim_type
claim_layer
claim_group
claimed_team
profile_type
raw_confidence
outcome_confidence_label
matchup_label
signal_gap
rank_gap
percentile_gap
team_percentile
opponent_percentile
team_rank
opponent_rank
core_area_edge_count
core_area_conflict_count
```

#### Context fields

```text
is_week_1
is_early_regular
is_late_regular
is_postseason
is_week_18
is_playoff
has_no_prior_data
profile_strength
has_matchup_caution
```

#### Postgame target fields

```text
actual_team_leader
validation_result
claim_validated_flag
actual_neutral_or_mixed_flag
actual_elevated_flag
actual_gap
actual_percentile
actual_rank
```

---

## Important Distinction: Directional vs Elevated

This is probably the biggest modeling idea.

There are two different questions:

### Directional validation

> Did the claimed team lead postgame?

Example:

```text
MIN had more pressure than opponent.
```

### Elevated validation

> Did the claimed team perform at a meaningfully high level, not merely better than the opponent?

Example:

```text
MIN pressure was not just better — it was meaningfully high compared with the league/game distribution.
```

This matters because a team can be better than the opponent but still not deserve strong language.

Suggested language rule:

```text
Better than opponent = edge / lean
Better than opponent + high percentile/rank = elevated
Better than opponent + high percentile/rank + multiple supporting features = strong/elevated profile
```

---

## Feature Engineering Ideas To Explore

These are not hardcoded rules yet. Treat them as candidate features.

### 1. Two-way strength feature

Example:

```text
high_defensive_control_percentile AND high_offensive_output_percentile
```

Possible meaning:

> Team has both the ability to suppress opponent efficiency and produce offense.

Could be stronger than either Core Area alone.

---

### 2. Scoring Efficiency + Offensive Output combo

Example:

```text
scoring_efficiency_edge + offensive_output_edge
```

Possible meaning:

> Team moves the ball and turns movement into points.

This may be more stable than raw yardage or red-zone metrics alone.

---

### 3. Defensive Control + Disruption combo

Example:

```text
defensive_control_edge + disruption_turnover_edge
```

Possible meaning:

> Team can limit opponent baseline efficiency and create negative events.

Caution:

> Turnovers can be noisy. This combo may need confidence caps unless supported by pressure/sacks/defensive efficiency.

---

### 4. Pressure claim reliability

Pressure had weaker validation in the current run.

Possible improvement:

```text
pressure_edge + opponent_pass_volume + opponent_sacks_taken + recent_pressure_form
```

Reason:

> A team may have a pressure advantage, but it only matters if the opponent drops back enough or has protection issues.

---

### 5. No Pick hidden-edge detector

Feature idea:

```text
no_pick + headline_claim_validation_like_profile + final_margin_large
```

Goal:

> Identify “No Clear Edge” games that had enough pregame signal to deserve a cautious lean.

Use carefully. Do not punish good No Picks.

---

### 6. Confidence suppression feature

Possible inputs:

```text
split_core_areas
conflicting_profile
week_18_or_playoff_context
low_claim_reliability_groups
turnover_heavy_signal
pressure_only_signal
near_even_percentile_gap
```

Goal:

> Prevent High Outcome Confidence unless the supporting evidence is unusually strong.

---

## Suggested Tomorrow Plan

### Step 1 — Lock in the QA baseline

Keep these as your current baseline folders:

```text
qa/gamelens_payload_runs/baseline_32_v2
qa/gamelens_claim_validation_runs/claims_96_v2
```

Do not overwrite them casually.

---

### Step 2 — Fix tie handling

Target:

```text
20250928_GB@DAL
```

Expected behavior:

```text
Tie / Push / No Decision
```

Then rerun Level 1 and Level 2 on the same 96-game sample.

---

### Step 3 — Review the worst reasoning misses

Start with 5–8 games from:

```text
bad_reasoning_bad_outcome
```

Focus on:

- What did GameLens claim?
- Which claims failed?
- Were the failed claims pressure, turnovers, scoring efficiency, offensive output, or defensive control?
- Did the model over-promote confidence?
- Was the issue data, language, weighting, or missing context?

---

### Step 4 — Review No Pick hidden-edge games

Start with:

```text
20231008_CAR@DET
20231124_MIA@NYJ
20241225_BAL@HOU
20251019_WSH@DAL
20251124_CAR@SF
20251228_NYG@LV
```

Question:

> Did these deserve a cautious lean, or was No Pick still appropriate?

---

### Step 5 — Define “Elevated” targets

Before building a model, define postgame labels.

Example target labels:

```text
actual_directional_edge
actual_elevated_edge
actual_dominant_edge
```

Possible definitions:

```text
actual_directional_edge = team beat opponent in that group
actual_elevated_edge = team beat opponent and ranked above a strong percentile threshold
actual_dominant_edge = team beat opponent by a large gap and ranked high overall
```

The exact thresholds can be distribution-based, not hand-maintained.

---

### Step 6 — Build feature matrix before model

Do not jump straight to a model.

First build a clean dataset where each row has:

- Pregame features
- Claim type
- Claim group/core area
- Predicted/claimed team
- Actual postgame result
- Actual directional/elevated target

Then model on top of that.

---

## Model Layer Idea

Eventually, a model can sit on top of this data.

But it should start simple:

1. Logistic regression / tree-based classifier for claim validation
2. Predict probability that a claim validates
3. Predict probability that a claim deserves Elevated language
4. Use that probability to calibrate language and confidence

Possible outputs:

```text
pressure_claim_probability
core_area_claim_probability
elevated_language_probability
confidence_promotion_probability
```

Important:

> The model should help calibrate language before it tries to predict winners.

---

## Do Not Fall Into These Traps

### Trap 1 — Building a manual decision tree

Avoid:

```text
if pressure and turnover then high confidence unless week 8 but only if...
```

Better:

```text
build features, measure reliability, let historical validation guide weights
```

---

### Trap 2 — Treating winner prediction as the only truth

A team can lose while a claim was valid.

A team can win while the reasoning was shaky.

Both are important.

---

### Trap 3 — Treating “better than opponent” as “elevated”

This is huge.

A team can be better than the opponent and still not deserve strong language.

Use:

```text
better = directional edge
elevated = directional edge + meaningful percentile/rank/gap support
```

---

### Trap 4 — Overreacting to one 96-game sample

This sample is useful, but it is still a baseline.

Do not make aggressive changes until the same patterns show up across:

- 96-game baseline
- larger 240-game baseline
- specific bad-reasoning games
- specific no-pick hidden-edge games

---

## Bottom Line

You are not wrong to want better data before building a smarter model.

That is the correct instinct.

The current evidence says:

> GameLens has useful signal, but the claim language and confidence labels need to be calibrated against actual postgame validation.

The next best project direction is:

1. Fix tie handling.
2. Keep Level 2 QA v2 as the baseline validator.
3. Build a claim/core-area feature matrix.
4. Define directional vs elevated postgame targets.
5. Use that dataset to learn which combinations deserve stronger language.
6. Only then layer a model on top.

That is how you get to better data, cleaner claims, and eventually better predictions.
