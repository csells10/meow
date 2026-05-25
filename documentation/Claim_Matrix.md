# GameLens Historical Claim Matrix Roadmap

_Last updated: 2026-05-24_

## Purpose

This roadmap describes a new **admin-facing Historical Claim Matrix** for GameLens.

The goal is to help me answer:

> How is GameLens doing at telling the correct pregame football story?

This is **not** a public accuracy scoreboard yet.

This is also **not** an automated recommendation engine yet.

The goal is to expose historical claim-validation data in a way that lets me inspect patterns myself.

I want to be able to notice things like:

- Defensive metrics are not telling the correct pregame story.
- Offensive Output claims are holding up better than Turnover claims.
- Medium Confidence games may be validating better than High Confidence games.
- No Pick situations may be increasing.
- Certain claim types are useful, while others are noisy.
- Some football areas should drive language, while others should stay contextual.

---

# 1. Core Concept

GameLens makes many pregame claims.

Examples:

```text
BUF has better Offensive Output.
PHI has better Defensive Control.
KC has a Scoring Efficiency edge.
BAL has a Disruption and Turnovers advantage.
DAL has a metric highlight in third_down_pct.
```

Each claim gets stored in:

```text
nfl-stream-406420.Analytics.gamelens_claim_training_examples
```

After the game, each claim receives validation fields such as:

```text
validation_result
validated_flag
actual_side
actual_team
actual_gap
actual_gap_bucket
headline_claim_validation_rate
unique_claim_validation_rate
```

The key measurement is:

```text
claim validation rate
```

Meaning:

> When GameLens made this type of pregame claim, how often did postgame data agree?

This is **not winner accuracy**.

A team can lose the game and still validate a GameLens claim if the postgame metric or matchup area supported the original pregame read.

---

# 2. What This Section Should Be Called

Recommended admin section name:

```text
Historical Claim Matrix
```

Alternative names:

```text
Claim Explorer
Historical Claim Explorer
GameLens Claim QA
Algorithm Review
```

Avoid names like:

```text
Model Accuracy
Prediction Accuracy
Betting Accuracy
```

Those names pull the product toward winner-prediction thinking, which is not the goal here.

---

# 3. What This Should Do

The Historical Claim Matrix should let me slice claim validation by:

```text
date
core_area
category
metric
claim_type
claim_layer
confidence
model_result
profile_type
matchup_label
```

The first version should be data-first.

It should **not** tell me:

```text
Promote this.
Retire that.
Use stronger language here.
```

At this stage, I want the matrix to show the data clearly so I can inspect it.

---

# 4. Five Initial Matrices

## Matrix 1 — `daily_by_core_area`

### Question

> By date, which broad football areas are telling the correct pregame story?

### Grain

```text
game_date × core_area
```

### Useful for spotting

```text
Defensive Control is weak lately.
Offensive Output is stable.
Disruption and Turnovers is noisy.
Scoring Efficiency is mixed.
```

### Metrics shown

```text
row_count
game_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
```

---

## Matrix 2 — `daily_by_category`

### Question

> By date, which more specific football categories are validating or failing?

### Grain

```text
game_date × category
```

### Useful for spotting

```text
Rushing Game is strong.
Passing Game is mixed.
Pressure claims are weak.
Turnover Risk is volatile.
Red Zone Finish is fragile.
```

### Metrics shown

```text
row_count
game_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
```

---

## Matrix 3 — `confidence_by_core_area`

### Question

> Does confidence actually line up with better claim validation by football area?

### Grain

```text
outcome_confidence_label × core_area
```

### Useful for spotting

```text
High Confidence + Defensive Control may be weak.
Medium Confidence + Offensive Output may be strong.
Low Confidence + Turnovers may be correctly cautious.
High Confidence may not always outperform Medium Confidence.
```

### Metrics shown

```text
row_count
game_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
```

---

## Matrix 4 — `claim_surface_by_confidence`

### Question

> Are headline claims, supporting claims, and team comparison claims behaving differently by confidence?

### Grain

```text
claim_type × claim_layer × outcome_confidence_label
```

### Useful for spotting

```text
Headline claims are cleaner than supporting claims.
Team Comparison claims are stable.
Game Profile claims are noisy.
High Confidence supporting claims are over-loud.
```

### Metrics shown

```text
row_count
game_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
```

---

## Matrix 5 — `no_pick_trend`

### Question

> Are No Pick / Low Confidence situations increasing, and are they useful restraint?

### Grain

```text
game_date × model_result × outcome_confidence_label
```

### Useful for spotting

```text
No Pick rate is increasing.
Low Confidence games have lower claim validation.
No Pick games still contain useful claim pockets.
High Confidence misses are clustered on certain dates.
```

### Metrics shown

```text
game_count
claim_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
```

---

# 5. Standard Calculation Rules

## Validation rate

Use:

```sql
SAFE_DIVIDE(
  COUNTIF(validation_result = 'validated'),
  COUNTIF(validation_result != 'unavailable')
) AS validation_rate
```

Why:

- `validated` means postgame data supported the pregame claim.
- `not_validated` means postgame data contradicted the claim.
- `actual_neutral_or_mixed` means the actual result was too close, tied, or mixed.
- `unavailable` means the system could not validate the claim.

For matrix review, it is usually better to exclude unavailable rows from the denominator.

## Minimum row count

Use basic row-count filters so tiny samples do not dominate.

Suggested first-pass thresholds:

```text
daily matrix: HAVING row_count >= 5
overall matrix: HAVING row_count >= 10
feature matrix later: HAVING row_count >= 20 or 30
```

These are review thresholds, not permanent model rules.

---

# 6. SQL Queries

## Query 1 — Daily by Core Area

```sql
SELECT
  game_date,
  COALESCE(core_area, 'missing_core_area') AS core_area,

  COUNT(*) AS row_count,
  COUNT(DISTINCT game_id) AS game_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  game_date,
  core_area
HAVING row_count >= 5
ORDER BY
  game_date,
  validation_rate DESC;
```

---

## Query 2 — Daily by Category

```sql
SELECT
  game_date,
  COALESCE(core_area, 'missing_core_area') AS core_area,
  COALESCE(category, 'missing_category') AS category,

  COUNT(*) AS row_count,
  COUNT(DISTINCT game_id) AS game_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  game_date,
  core_area,
  category
HAVING row_count >= 5
ORDER BY
  game_date,
  validation_rate DESC,
  row_count DESC;
```

---

## Query 3 — Confidence by Core Area

```sql
SELECT
  COALESCE(outcome_confidence_label, 'missing_confidence') AS outcome_confidence_label,
  COALESCE(core_area, 'missing_core_area') AS core_area,

  COUNT(*) AS row_count,
  COUNT(DISTINCT game_id) AS game_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  outcome_confidence_label,
  core_area
HAVING row_count >= 10
ORDER BY
  outcome_confidence_label,
  validation_rate DESC,
  row_count DESC;
```

---

## Query 4 — Claim Surface by Confidence

```sql
SELECT
  claim_type,
  claim_layer,
  COALESCE(outcome_confidence_label, 'missing_confidence') AS outcome_confidence_label,

  COUNT(*) AS row_count,
  COUNT(DISTINCT game_id) AS game_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  claim_type,
  claim_layer,
  outcome_confidence_label
HAVING row_count >= 10
ORDER BY
  claim_type,
  claim_layer,
  outcome_confidence_label;
```

---

## Query 5 — No Pick Trend

```sql
SELECT
  game_date,
  COALESCE(model_result, 'missing_model_result') AS model_result,
  COALESCE(outcome_confidence_label, 'missing_confidence') AS outcome_confidence_label,

  COUNT(DISTINCT game_id) AS game_count,
  COUNT(*) AS claim_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  game_date,
  model_result,
  outcome_confidence_label
ORDER BY
  game_date,
  model_result,
  outcome_confidence_label;
```

---

# 7. Optional Extra Query — Overall Baseline

Run this first so every matrix has context.

```sql
SELECT
  run_id,

  COUNT(*) AS row_count,
  COUNT(DISTINCT game_id) AS game_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  run_id;
```

This gives the overall claim-validation baseline.

Then the matrices can be interpreted against that baseline.

---

# 8. Optional Extra Query — Metric-Level Review

Use this after the broader matrices.

```sql
SELECT
  COALESCE(core_area, 'missing_core_area') AS core_area,
  COALESCE(category, 'missing_category') AS category,
  COALESCE(metric, 'missing_metric') AS metric,

  COUNT(*) AS row_count,
  COUNT(DISTINCT game_id) AS game_count,

  COUNTIF(validation_result = 'validated') AS validated_count,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
  COUNTIF(validation_result = 'unavailable') AS unavailable_count,

  SAFE_DIVIDE(
    COUNTIF(validation_result = 'validated'),
    COUNTIF(validation_result != 'unavailable')
  ) AS validation_rate

FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  core_area,
  category,
  metric
HAVING row_count >= 20
ORDER BY
  validation_rate DESC,
  row_count DESC;
```

This helps answer:

```text
Which specific metrics are telling the right story?
Which metrics are noisy?
Which parent categories hide good or bad children?
```

---

# 9. API Design

## Recommended endpoint

```http
GET /admin/gamelens/claim-matrix?run_id=larger_240_level3_qa_20260517&matrix=daily_by_core_area
```

## Supported `matrix` values

```text
daily_by_core_area
daily_by_category
confidence_by_core_area
claim_surface_by_confidence
no_pick_trend
```

## Example response shape

```json
{
  "available": true,
  "scope": "admin_historical_claim_matrix",
  "run_id": "larger_240_level3_qa_20260517",
  "matrix": "daily_by_core_area",
  "grain": ["game_date", "core_area"],
  "metrics": [
    "row_count",
    "game_count",
    "validated_count",
    "not_validated_count",
    "neutral_or_mixed_count",
    "unavailable_count",
    "validation_rate"
  ],
  "rows": [
    {
      "game_date": "2025-10-13",
      "core_area": "Defensive Control",
      "row_count": 18,
      "game_count": 3,
      "validated_count": 6,
      "not_validated_count": 9,
      "neutral_or_mixed_count": 3,
      "unavailable_count": 0,
      "validation_rate": 0.333
    }
  ]
}
```

## Important API principle

The endpoint should return data, not conclusions.

Good:

```json
{
  "core_area": "Defensive Control",
  "validation_rate": 0.333,
  "row_count": 18
}
```

Avoid for v1:

```json
{
  "recommended_action": "downgrade Defensive Control"
}
```

That kind of action card can come later after I understand the data better.

---

# 10. Suggested File Structure

## Backend API files

```text
routes/admin_claim_matrix_routes.py
services/admin_claim_matrix_service.py
queries/admin_claim_matrix_queries.py
```

## Optional later builder

```text
agg/gamelens_training/build_claim_validation_matrices.py
```

## Why this split

```text
queries/
  Owns BigQuery SQL.

services/
  Shapes response payload.

routes/
  Handles Flask endpoint and query params.

agg/
  Optional later batch builder if the queries become too heavy.
```

---

# 11. Implementation Timeline

## Phase 1 — Manual BigQuery Exploration

Status:

```text
Next step
```

Run the five SQL queries manually in BigQuery.

Goal:

```text
Confirm the matrices are useful before coding anything.
```

Success condition:

```text
I can look at the outputs and start noticing patterns.
```

Example discoveries to look for:

```text
Offensive Output is consistently above baseline.
Defensive Control is weaker than expected.
Medium Confidence is cleaner than High Confidence.
No Pick games are increasing.
Turnover-related claims are volatile.
```

---

## Phase 2 — Pick the First Two Matrices to API-ize

Recommended first two:

```text
daily_by_core_area
confidence_by_core_area
```

Reason:

```text
These answer the biggest questions with the least complexity:
1. Which football areas are working over time?
2. Does confidence actually line up with claim quality?
```

Do not build all five into the API immediately unless the manual query outputs look useful.

---

## Phase 3 — Create Query Functions

Suggested file:

```text
queries/admin_claim_matrix_queries.py
```

Suggested functions:

```python
get_daily_by_core_area(run_id: str) -> list[dict]
get_daily_by_category(run_id: str) -> list[dict]
get_confidence_by_core_area(run_id: str) -> list[dict]
get_claim_surface_by_confidence(run_id: str) -> list[dict]
get_no_pick_trend(run_id: str) -> list[dict]
```

Also add:

```python
get_overall_baseline(run_id: str) -> dict
```

---

## Phase 4 — Create Service Layer

Suggested file:

```text
services/admin_claim_matrix_service.py
```

Suggested function:

```python
get_claim_matrix(run_id: str, matrix: str) -> dict
```

Response should include:

```text
available
scope
run_id
matrix
grain
metrics
baseline
rows
```

---

## Phase 5 — Create Admin Route

Suggested file:

```text
routes/admin_claim_matrix_routes.py
```

Suggested route:

```http
GET /admin/gamelens/claim-matrix
```

Query params:

```text
run_id
matrix
```

Example:

```http
/admin/gamelens/claim-matrix?run_id=larger_240_level3_qa_20260517&matrix=daily_by_core_area
```

---

## Phase 6 — Add Admin UI Later

Do not start with UI.

Once the endpoint exists, the admin UI can display:

```text
Matrix selector
Run selector
Table output
Validation rate column
Row count column
Game count column
Optional CSV export
```

Possible tabs:

```text
By Date
By Core Area
By Category
By Confidence
By Claim Type
No Pick Trend
```

---

## Phase 7 — Optional BigQuery Summary Table

Only do this if direct query performance becomes annoying.

Possible table:

```text
Analytics.gamelens_claim_validation_matrices
```

Possible schema:

```text
run_id STRING
matrix_name STRING
row_key STRING
row_label STRING
column_key STRING
column_label STRING
game_date DATE
core_area STRING
category STRING
claim_type STRING
claim_layer STRING
outcome_confidence_label STRING
model_result STRING
row_count INT64
game_count INT64
validated_count INT64
not_validated_count INT64
neutral_or_mixed_count INT64
unavailable_count INT64
validation_rate FLOAT64
created_at TIMESTAMP
```

Possible builder:

```text
agg/gamelens_training/build_claim_validation_matrices.py
```

This would let the API read from a prepared matrix table instead of running grouped queries live.

---

# 12. What Not To Do Yet

Do not add:

```text
AI-generated recommendations
automatic promotion decisions
frontend language boosts
public accuracy labels
winner-prediction dashboards
complicated ML model outputs
```

Do not make this another label-heavy API.

The first version should be:

```text
Show me the rows.
Show me the rates.
Let me inspect the story.
```

---

# 13. Recommended Immediate Next Step

Run these two queries first:

```text
1. daily_by_core_area
2. confidence_by_core_area
```

Then inspect:

```text
Do the results make sense?
Are there enough rows?
Are any football areas obviously strong or weak?
Does confidence behave the way I expected?
```

If those are useful, build the first API endpoint.

Recommended first endpoint:

```http
GET /admin/gamelens/claim-matrix?run_id=larger_240_level3_qa_20260517&matrix=daily_by_core_area
```

---

# 14. One-Sentence Summary

> Build a Historical Claim Matrix so I can inspect whether GameLens is telling the right pregame football story by date, football area, claim type, and confidence — before asking the system to make automatic decisions.