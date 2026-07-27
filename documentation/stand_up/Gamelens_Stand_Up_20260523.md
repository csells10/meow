# GameLens Next Work — After clean_hierarchy_context_v1

## Current stopping point

Committed `clean_hierarchy_context_v1`.

Level 3 now attaches registry-backed hierarchy metadata to claim rows:
- registry_core_area
- registry_category
- registry_metric_label
- registry_signal_strength
- registry_ranking_usage
- clean_hierarchy_path
- clean_hierarchy_status
- clean_hierarchy_path_flag
- missing_hierarchy_parent_flag

Validation passed:
- 6,775 rows updated
- 1,791 registry_metric_clean
- 3,537 registry_metric_parent_recovered
- 834 row_metadata_core_area
- 613 row_metadata_missing_core_area
- zero registry/original hierarchy mismatches

No Level 4 rules, frontend behavior, matchup lean, confidence, Model Trust, or winner logic changed.

## Next task

Run a clean-path-only hierarchy analysis using the new Level 3 fields.

Goal:
Confirm that the exploratory findings still hold when using:
- clean_hierarchy_path
- registry_core_area
- registry_category
- clean_hierarchy_path_flag = true

Question:
Do the same patterns hold after hierarchy cleanup?

Expected patterns to verify:
- Offensive Output remains positive
- Disruption and Turnovers remains negative
- yards_per_play, points_per_play, yards_per_pass, yards_per_rush remain strong opportunity metrics
- td_rate, red_zone_efficiency, turnover_margin_per_game remain warning metrics
- third_down_pct and 1st_down_rate remain context/noisy

## After that

If clean-path analysis confirms the pattern, start Level 3 candidate:
1. `offensive_efficiency_support_v1`
2. Then `volatile_finish_warning_v1`

###########
# GameLens Next Work Handoff — After `clean_hierarchy_context_v1`

## Current stopping point

Committed the Level 3 `clean_hierarchy_context_v1` patch.

This was the first foundation task after the 2026-05-22 feature-testing and exploratory hierarchy work. The purpose was not to add a new football score yet. The purpose was to make Level 3 claim rows carry canonical registry-backed hierarchy metadata so future feature work can trust:

```text
Core Area → Category → Metric
```

instead of relying only on raw/parser-populated `core_area` and `category` fields.

## Files changed

Primary file changed:

```text
agg/gamelens_training/update_claim_training_features.py
```

Expected behavior after patch:

- Existing Level 3 formulas stay unchanged:
  - `offense_finish_score`
  - `defensive_suppression_score`
  - `two_way_edge_score`
  - `two_way_context`
- No winner logic changed.
- No matchup lean changed.
- No confidence changed.
- No Model Trust changed.
- No Level 4 registry rules changed.
- No frontend behavior changed.

## New Level 3 output fields

The patch added these fields:

```text
registry_core_area
registry_category
registry_metric_label
registry_signal_strength
registry_ranking_usage
clean_hierarchy_path
clean_hierarchy_status
clean_hierarchy_path_flag
missing_hierarchy_parent_flag
```

## BigQuery schema patch

BigQuery initially hit a table-update rate limit because multiple `ALTER TABLE ADD COLUMN` statements were run against the same table too quickly.

The first 5 fields landed:

```text
registry_core_area
registry_category
registry_metric_label
registry_signal_strength
registry_ranking_usage
```

The remaining 4 fields were added using a one-shot Python schema patch script:

```text
clean_hierarchy_path
clean_hierarchy_status
clean_hierarchy_path_flag
missing_hierarchy_parent_flag
```

The temporary schema patch script was named:

```text
delete_me.py
```

It successfully added:

```text
clean_hierarchy_path STRING
clean_hierarchy_status STRING
clean_hierarchy_path_flag BOOLEAN
missing_hierarchy_parent_flag BOOLEAN
```

This script can be deleted if not already deleted.

## Formula version

The Level 3 formula version is now:

```text
clean_hierarchy_context_v1__offense_finish_v2__defensive_suppression_v3__two_way_context_v1
```

## Dry-run command used

```bash
python -m agg.gamelens_training.update_claim_training_features \
  --run-id larger_240_level3_qa_20260517 \
  --dry-run
```

## Dry-run result

Dry-run passed.

Summary:

```text
Training rows loaded: 6,775
Feature rows built: 6,775

Clean hierarchy status distribution:
registry_metric_clean: 1,791
registry_metric_parent_recovered: 3,537
row_metadata_core_area: 834
row_metadata_missing_core_area: 613

Clean hierarchy path flag:
true: 6,162
false: 613

Missing hierarchy parent flag:
true: 4,150
false: 2,625

Rows with offense_finish_score: 1,883
Rows with defensive_suppression_score: 878
Rows with two_way_edge_score: 5,378

two_way_context:
supportive: 1,902
available_mixed: 3,476
unavailable: 1,397
```

Important interpretation:

- `registry_metric_clean` = original row already had clean parent hierarchy and registry agreed.
- `registry_metric_parent_recovered` = original row was missing `core_area`, `category`, or both, and registry filled it.
- `row_metadata_core_area` = non-metric row with usable `core_area`.
- `row_metadata_missing_core_area` = non-metric row still missing clean parent hierarchy.
- No `metric_not_in_registry` appeared.
- No `no_hierarchy_context` appeared.

This means registry lookup worked for metric rows, and incomplete non-metric hierarchy rows are now clearly flagged instead of silently trusted. :contentReference[oaicite:0]{index=0}

## BigQuery write

After dry-run passed, Level 3 was written back to BigQuery.

Validation query used:

```sql
SELECT
  clean_hierarchy_status,
  clean_hierarchy_path_flag,
  missing_hierarchy_parent_flag,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  clean_hierarchy_status,
  clean_hierarchy_path_flag,
  missing_hierarchy_parent_flag
ORDER BY
  row_count DESC;
```

BigQuery result:

```text
registry_metric_parent_recovered | true  | true  | 3,537
registry_metric_clean            | true  | false | 1,791
row_metadata_core_area           | true  | false | 834
row_metadata_missing_core_area   | false | true  | 613
```

This matched dry-run exactly.

## Spot-check query used

This was used to confirm key metrics map to expected canonical hierarchy paths:

```sql
SELECT
  metric,
  registry_core_area,
  registry_category,
  clean_hierarchy_path,
  clean_hierarchy_status,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
  AND metric IN (
    'points_per_play',
    'yards_per_play',
    'yards_per_pass',
    'yards_per_rush',
    'turnover_margin_per_game',
    'red_zone_efficiency',
    'td_rate',
    'third_down_pct',
    '1st_down_rate',
    'points_allowed_per_play'
  )
GROUP BY
  metric,
  registry_core_area,
  registry_category,
  clean_hierarchy_path,
  clean_hierarchy_status
ORDER BY
  metric,
  row_count DESC;
```

## Spot-check results

The expected paths showed up correctly:

```text
1st_down_rate
→ Offensive Output > Offensive Rhythm > 1st_down_rate

points_allowed_per_play
→ Defensive Control > Scoring Suppression > points_allowed_per_play

points_per_play
→ Scoring Efficiency > Scoring Production > points_per_play

red_zone_efficiency
→ Scoring Efficiency > Red Zone Finish > red_zone_efficiency

td_rate
→ Scoring Efficiency > Scoring Production > td_rate

third_down_pct
→ Scoring Efficiency > Drive Conversion > third_down_pct

turnover_margin_per_game
→ Disruption and Turnovers > Turnovers > turnover_margin_per_game

yards_per_pass
→ Offensive Output > Passing Game > yards_per_pass

yards_per_play
→ Offensive Output > Offensive Rhythm > yards_per_play

yards_per_rush
→ Offensive Output > Rushing Game > yards_per_rush
```

Rows appeared in both `registry_metric_clean` and `registry_metric_parent_recovered`, but both statuses mapped to the same canonical path for each metric. That is expected and good. :contentReference[oaicite:1]{index=1}

## Recovery audit query used

This query checked why rows were being marked as `registry_metric_parent_recovered`:

```sql
SELECT
  metric,
  core_area AS original_core_area,
  category AS original_category,
  registry_core_area,
  registry_category,
  clean_hierarchy_status,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
  AND metric IN (
    'points_per_play',
    'yards_per_play',
    'yards_per_pass',
    'yards_per_rush',
    'turnover_margin_per_game',
    'red_zone_efficiency',
    'td_rate',
    'third_down_pct',
    '1st_down_rate',
    'points_allowed_per_play'
  )
GROUP BY
  metric,
  original_core_area,
  original_category,
  registry_core_area,
  registry_category,
  clean_hierarchy_status
ORDER BY
  metric,
  clean_hierarchy_status,
  row_count DESC;
```

## Recovery audit finding

Recovered rows were recovered because original rows were missing `core_area`, `category`, or both.

Examples:

```text
original_core_area = null
original_category = Rushing Game
registry_core_area = Offensive Output
registry_category = Rushing Game
```

or:

```text
original_core_area = Scoring Efficiency
original_category = null
registry_core_area = Scoring Efficiency
registry_category = Scoring Production
```

This means recovery behavior is doing what it was designed to do. It is not masking a mismatch. :contentReference[oaicite:2]{index=2}

## Mismatch audit query used

Final safety query:

```sql
SELECT
  metric,
  core_area AS original_core_area,
  category AS original_category,
  registry_core_area,
  registry_category,
  clean_hierarchy_status,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
  AND metric IS NOT NULL
  AND (
    (core_area IS NOT NULL AND registry_core_area IS NOT NULL AND core_area != registry_core_area)
    OR
    (category IS NOT NULL AND registry_category IS NOT NULL AND category != registry_category)
  )
GROUP BY
  metric,
  original_core_area,
  original_category,
  registry_core_area,
  registry_category,
  clean_hierarchy_status
ORDER BY
  row_count DESC;
```

Result:

```text
zero rows
```

This is the strongest validation result. It means there were no non-null original hierarchy values that disagreed with registry hierarchy values.

## Current decision

```text
clean_hierarchy_context_v1: PASS
Dry-run: PASS
BigQuery write: PASS
Spot-check: PASS
Recovery audit: PASS
Mismatch audit: PASS
Commit: DONE
```

## Important distinction

The labels are not errors.

They are audit/status labels:

```text
registry_metric_clean
registry_metric_parent_recovered
row_metadata_core_area
row_metadata_missing_core_area
```

The only caution bucket is:

```text
row_metadata_missing_core_area
```

There are 613 of these. These are non-metric rows with incomplete hierarchy metadata. They should not be used for clean-path hierarchy analysis unless intentionally studying incomplete non-metric/profile rows.

## What this enables next

Now future analysis can use registry-backed hierarchy fields instead of raw hierarchy fields.

Preferred fields going forward:

```text
registry_core_area
registry_category
clean_hierarchy_path
clean_hierarchy_status
clean_hierarchy_path_flag
missing_hierarchy_parent_flag
```

For clean exploratory analysis, prefer:

```sql
WHERE clean_hierarchy_path_flag = TRUE
```

For clean metric-level analysis, prefer:

```sql
WHERE metric IS NOT NULL
  AND registry_core_area IS NOT NULL
  AND registry_category IS NOT NULL
```

## Next recommended work

Do not start by coding another feature immediately.

First rerun the hierarchy/opportunity analysis using the new clean hierarchy fields.

Goal:

Confirm that the same exploratory findings still hold when using canonical registry-backed fields.

Expected patterns to verify:

```text
Positive / opportunity:
- Offensive Output
- Offensive Output > Passing Game
- Offensive Output > Rushing Game
- Offensive Output > Offensive Rhythm > yards_per_play
- Scoring Efficiency > Scoring Production > points_per_play
- Offensive Output > Passing Game > yards_per_pass
- Offensive Output > Rushing Game > yards_per_rush

Negative / warning:
- Disruption and Turnovers
- Disruption and Turnovers > Turnovers
- Disruption and Turnovers > Turnovers > turnover_margin_per_game
- Scoring Efficiency > Red Zone Finish > red_zone_efficiency
- Scoring Efficiency > Scoring Production > td_rate

Neutral / context:
- Scoring Efficiency > Drive Conversion > third_down_pct
- Offensive Output > Offensive Rhythm > 1st_down_rate
```

## Suggested next query: clean-path hierarchy inventory

Use this as the next analysis query.

Purpose:

- Use `registry_core_area`, `registry_category`, and `clean_hierarchy_path`.
- Exclude incomplete hierarchy rows with `clean_hierarchy_path_flag = TRUE`.
- Compare Core Area, Category, and Metric levels.
- Confirm the same feature families before adding new Level 3 score fields.

```sql
WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claim_type,
    claim_layer,
    claim_name,

    registry_core_area,
    registry_category,
    metric,
    clean_hierarchy_path,
    clean_hierarchy_status,
    clean_hierarchy_path_flag,
    missing_hierarchy_parent_flag,

    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
    AND clean_hierarchy_path_flag = TRUE
),

baseline_by_run AS (
  SELECT
    run_id,
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
  GROUP BY run_id
),

baseline_all_runs AS (
  SELECT
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
),

hierarchy_rows AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'core_area' AS grain_level,
    registry_core_area AS core_area,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS metric,
    registry_core_area AS hierarchy_path

  FROM base
  WHERE registry_core_area IS NOT NULL

  UNION ALL

  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'category' AS grain_level,
    registry_core_area AS core_area,
    registry_category AS category,
    CAST(NULL AS STRING) AS metric,
    CONCAT(registry_core_area, ' > ', registry_category) AS hierarchy_path

  FROM base
  WHERE registry_core_area IS NOT NULL
    AND registry_category IS NOT NULL

  UNION ALL

  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'metric' AS grain_level,
    registry_core_area AS core_area,
    registry_category AS category,
    metric AS metric,
    CONCAT(registry_core_area, ' > ', registry_category, ' > ', metric) AS hierarchy_path

  FROM base
  WHERE registry_core_area IS NOT NULL
    AND registry_category IS NOT NULL
    AND metric IS NOT NULL
),

node_by_run AS (
  SELECT
    h.run_id AS test_run,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,

    COUNT(*) AS row_count,
    COUNT(DISTINCT h.game_id) AS distinct_game_count,
    COUNT(DISTINCT h.claim_key) AS unique_claim_count,
    COUNT(DISTINCT h.claim_type) AS distinct_claim_type_count,
    COUNT(DISTINCT h.claim_layer) AS distinct_claim_layer_count,

    SUM(h.validated_flag_int) AS validated_count,
    COUNTIF(h.validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(h.validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(h.validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'actual_neutral_or_mixed'), COUNT(*)), 4) AS neutral_or_mixed_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'unavailable'), COUNT(*)), 4) AS unavailable_rate,

    ROUND(b.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)) - b.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM hierarchy_rows h
  LEFT JOIN baseline_by_run b
    ON h.run_id = b.run_id
  GROUP BY
    h.run_id,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,
    b.baseline_validation_rate
),

repeatability AS (
  SELECT
    grain_level,
    hierarchy_path,

    COUNT(DISTINCT test_run) AS runs_present,
    COUNTIF(row_count >= 30) AS runs_with_enough_rows,

    COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) AS positive_lift_runs,
    COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(row_count >= 30) < 2
        THEN 'too_small'
      WHEN COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) >= 2
        THEN 'repeat_positive'
      WHEN COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) >= 2
        THEN 'repeat_negative'
      ELSE 'mixed_or_unclear'
    END AS repeatability_label

  FROM node_by_run
  GROUP BY
    grain_level,
    hierarchy_path
),

node_all_runs AS (
  SELECT
    'ALL_SELECTED_RUNS' AS test_run,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,

    COUNT(*) AS row_count,
    COUNT(DISTINCT h.game_id) AS distinct_game_count,
    COUNT(DISTINCT h.claim_key) AS unique_claim_count,
    COUNT(DISTINCT h.claim_type) AS distinct_claim_type_count,
    COUNT(DISTINCT h.claim_layer) AS distinct_claim_layer_count,

    SUM(h.validated_flag_int) AS validated_count,
    COUNTIF(h.validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(h.validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(h.validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'actual_neutral_or_mixed'), COUNT(*)), 4) AS neutral_or_mixed_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'unavailable'), COUNT(*)), 4) AS unavailable_rate,

    ROUND(b.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)) - b.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM hierarchy_rows h
  CROSS JOIN baseline_all_runs b
  GROUP BY
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,
    b.baseline_validation_rate
),

final AS (
  SELECT
    n.*,
    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN n.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN n.row_count >= 100 AND ABS(n.lift_vs_baseline) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM node_all_runs n
  LEFT JOIN repeatability r
    ON n.grain_level = r.grain_level
   AND n.hierarchy_path = r.hierarchy_path

  UNION ALL

  SELECT
    n.*,
    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN n.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN n.row_count >= 100 AND ABS(n.lift_vs_baseline) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM node_by_run n
  LEFT JOIN repeatability r
    ON n.grain_level = r.grain_level
   AND n.hierarchy_path = r.hierarchy_path
)

SELECT *
FROM final
ORDER BY
  CASE WHEN test_run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  CASE grain_level
    WHEN 'core_area' THEN 1
    WHEN 'category' THEN 2
    WHEN 'metric' THEN 3
    ELSE 4
  END,
  CASE exploratory_label
    WHEN 'opportunity_candidate' THEN 1
    WHEN 'warning_candidate' THEN 2
    WHEN 'high_volume_neutral' THEN 3
    WHEN 'review' THEN 4
    ELSE 5
  END,
  row_count DESC,
  ABS(lift_vs_baseline) DESC;
```

## After clean-path analysis

If clean-path analysis confirms the expected pattern, the next Level 3 feature candidate should be:

```text
offensive_efficiency_support_v1
```

Possible ingredients:

```text
points_per_play
yards_per_play
yards_per_pass
yards_per_rush
```

But do not implement it until the clean-path hierarchy query confirms the feature families again using the new registry-backed fields.

## Important product boundary

Keep this rule:

```text
Level 3 computes evidence.
Level 4 decides language calibration.
Frontend waits until backend contract is stable.
```

Level 3 should not directly change:

```text
matchup_lean
confidence
Model Trust
winner logic
frontend copy
```

## Status for tomorrow

Start here:

```text
Run clean-path hierarchy inventory query.
Compare results to the 2026-05-22 exploratory hierarchy findings.
If confirmed, start designing offensive_efficiency_support_v1.
```

--planned work short term--

re run query
analyze results

-query:
WITH base AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    season,
    claim_type,
    claim_layer,
    claim_name,

    registry_core_area,
    registry_category,
    metric,
    clean_hierarchy_path,
    clean_hierarchy_status,
    clean_hierarchy_path_flag,
    missing_hierarchy_parent_flag,

    validation_result,

    CASE
      WHEN validation_result = 'validated' THEN 1
      ELSE 0
    END AS validated_flag_int

  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id IN (
    'baseline_96_stage1_v2',
    'fresh_96_features_qa_v2',
    'larger_240_level3_qa_20260517'
  )
    AND validation_result IN (
      'validated',
      'not_validated',
      'actual_neutral_or_mixed',
      'unavailable'
    )
    AND clean_hierarchy_path_flag = TRUE
),

baseline_by_run AS (
  SELECT
    run_id,
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
  GROUP BY run_id
),

baseline_all_runs AS (
  SELECT
    COUNT(*) AS baseline_row_count,
    SAFE_DIVIDE(SUM(validated_flag_int), COUNT(*)) AS baseline_validation_rate
  FROM base
),

hierarchy_rows AS (
  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'core_area' AS grain_level,
    registry_core_area AS core_area,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS metric,
    registry_core_area AS hierarchy_path

  FROM base
  WHERE registry_core_area IS NOT NULL

  UNION ALL

  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'category' AS grain_level,
    registry_core_area AS core_area,
    registry_category AS category,
    CAST(NULL AS STRING) AS metric,
    CONCAT(registry_core_area, ' > ', registry_category) AS hierarchy_path

  FROM base
  WHERE registry_core_area IS NOT NULL
    AND registry_category IS NOT NULL

  UNION ALL

  SELECT
    run_id,
    claim_key,
    game_id,
    claim_type,
    claim_layer,
    validation_result,
    validated_flag_int,

    'metric' AS grain_level,
    registry_core_area AS core_area,
    registry_category AS category,
    metric AS metric,
    CONCAT(registry_core_area, ' > ', registry_category, ' > ', metric) AS hierarchy_path

  FROM base
  WHERE registry_core_area IS NOT NULL
    AND registry_category IS NOT NULL
    AND metric IS NOT NULL
),

node_by_run AS (
  SELECT
    h.run_id AS test_run,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,

    COUNT(*) AS row_count,
    COUNT(DISTINCT h.game_id) AS distinct_game_count,
    COUNT(DISTINCT h.claim_key) AS unique_claim_count,
    COUNT(DISTINCT h.claim_type) AS distinct_claim_type_count,
    COUNT(DISTINCT h.claim_layer) AS distinct_claim_layer_count,

    SUM(h.validated_flag_int) AS validated_count,
    COUNTIF(h.validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(h.validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(h.validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'actual_neutral_or_mixed'), COUNT(*)), 4) AS neutral_or_mixed_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'unavailable'), COUNT(*)), 4) AS unavailable_rate,

    ROUND(b.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)) - b.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM hierarchy_rows h
  LEFT JOIN baseline_by_run b
    ON h.run_id = b.run_id
  GROUP BY
    h.run_id,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,
    b.baseline_validation_rate
),

repeatability AS (
  SELECT
    grain_level,
    hierarchy_path,

    COUNT(DISTINCT test_run) AS runs_present,
    COUNTIF(row_count >= 30) AS runs_with_enough_rows,

    COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) AS positive_lift_runs,
    COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) AS negative_lift_runs,

    CASE
      WHEN COUNTIF(row_count >= 30) < 2
        THEN 'too_small'
      WHEN COUNTIF(lift_vs_baseline > 0.03 AND row_count >= 30) >= 2
        THEN 'repeat_positive'
      WHEN COUNTIF(lift_vs_baseline < -0.03 AND row_count >= 30) >= 2
        THEN 'repeat_negative'
      ELSE 'mixed_or_unclear'
    END AS repeatability_label

  FROM node_by_run
  GROUP BY
    grain_level,
    hierarchy_path
),

node_all_runs AS (
  SELECT
    'ALL_SELECTED_RUNS' AS test_run,
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,

    COUNT(*) AS row_count,
    COUNT(DISTINCT h.game_id) AS distinct_game_count,
    COUNT(DISTINCT h.claim_key) AS unique_claim_count,
    COUNT(DISTINCT h.claim_type) AS distinct_claim_type_count,
    COUNT(DISTINCT h.claim_layer) AS distinct_claim_layer_count,

    SUM(h.validated_flag_int) AS validated_count,
    COUNTIF(h.validation_result = 'not_validated') AS not_validated_count,
    COUNTIF(h.validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
    COUNTIF(h.validation_result = 'unavailable') AS unavailable_count,

    ROUND(SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)), 4) AS validation_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'actual_neutral_or_mixed'), COUNT(*)), 4) AS neutral_or_mixed_rate,
    ROUND(SAFE_DIVIDE(COUNTIF(h.validation_result = 'unavailable'), COUNT(*)), 4) AS unavailable_rate,

    ROUND(b.baseline_validation_rate, 4) AS baseline_validation_rate,
    ROUND(
      SAFE_DIVIDE(SUM(h.validated_flag_int), COUNT(*)) - b.baseline_validation_rate,
      4
    ) AS lift_vs_baseline

  FROM hierarchy_rows h
  CROSS JOIN baseline_all_runs b
  GROUP BY
    h.grain_level,
    h.hierarchy_path,
    h.core_area,
    h.category,
    h.metric,
    b.baseline_validation_rate
),

final AS (
  SELECT
    n.*,
    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN n.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN n.row_count >= 100 AND ABS(n.lift_vs_baseline) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM node_all_runs n
  LEFT JOIN repeatability r
    ON n.grain_level = r.grain_level
   AND n.hierarchy_path = r.hierarchy_path

  UNION ALL

  SELECT
    n.*,
    r.repeatability_label,
    r.positive_lift_runs,
    r.negative_lift_runs,

    CASE
      WHEN n.row_count < 30 THEN 'ignore_too_small'
      WHEN r.repeatability_label = 'repeat_positive' THEN 'opportunity_candidate'
      WHEN r.repeatability_label = 'repeat_negative' THEN 'warning_candidate'
      WHEN n.row_count >= 100 AND ABS(n.lift_vs_baseline) < 0.015 THEN 'high_volume_neutral'
      ELSE 'review'
    END AS exploratory_label

  FROM node_by_run n
  LEFT JOIN repeatability r
    ON n.grain_level = r.grain_level
   AND n.hierarchy_path = r.hierarchy_path
)

SELECT *
FROM final
ORDER BY
  CASE WHEN test_run = 'ALL_SELECTED_RUNS' THEN 0 ELSE 1 END,
  CASE grain_level
    WHEN 'core_area' THEN 1
    WHEN 'category' THEN 2
    WHEN 'metric' THEN 3
    ELSE 4
  END,
  CASE exploratory_label
    WHEN 'opportunity_candidate' THEN 1
    WHEN 'warning_candidate' THEN 2
    WHEN 'high_volume_neutral' THEN 3
    WHEN 'review' THEN 4
    ELSE 5
  END,
  row_count DESC,
  ABS(lift_vs_baseline) DESC;

  ## Clean-Path Revalidation Result — 2026-05-23

After backfilling `clean_hierarchy_context_v1` fields for:

- `baseline_96_stage1_v2`
- `fresh_96_features_qa_v2`
- `larger_240_level3_qa_20260517`

the clean-path hierarchy inventory now evaluates all three runs correctly.

### Main confirmation

`points_per_play` is the strongest repeat-positive offensive efficiency signal.

Across all selected runs:

- validation_rate: 56.53%
- baseline_validation_rate: 49.22%
- lift_vs_baseline: +7.31 points
- repeatability_label: repeat_positive
- positive_lift_runs: 3
- negative_lift_runs: 0

Interpretation:

Claims tied to `points_per_play` were more likely than average to be supported by postgame data across all three test runs.

This does not mean `points_per_play` predicts winners.

It means `points_per_play` appears useful as a claim-quality / claim-language support signal.

### Other useful offensive efficiency signals

- `yards_per_rush` showed positive support overall, but one run was weaker.
- `yards_per_play` showed positive support overall, but one run was negative.
- `yards_per_pass` showed positive overall lift, but did not meet repeat-positive labeling under the current query rule.

These should be treated as scoped or secondary support candidates, not automatic boost rules.

### Warning / no-boost signals

The following repeatedly underperformed baseline and should not be used for stronger claim language:

- `turnover_margin_per_game`
- `red_zone_efficiency`
- `td_rate`

`td_rate` was especially unstable, with a high neutral/mixed rate.

### Context-only signals

The following appear better suited for explanation/context than feature support:

- `third_down_pct`
- `1st_down_rate`

### Decision

The data supports designing a new Level 3 candidate:

`offensive_efficiency_support_v1`

But this feature has not been proven yet.

Next step is design only, not implementation.

The first design should be narrow:

Primary support:
- `points_per_play`

Secondary scoped support:
- `yards_per_rush`
- `yards_per_pass`
- `yards_per_play`

Excluded / caution:
- `td_rate`
- `red_zone_efficiency`
- `turnover_margin_per_game`
- `third_down_pct`
- `1st_down_rate`

Boundary:

`offensive_efficiency_support_v1` should support claim-language calibration only.

It should not change:

- winner logic
- matchup lean
- outcome confidence
- Model Trust
- frontend behavior
- betting logic

--step 1 code up offensive_efficiency_support_v1 to test--

Summary: offensive_efficiency_support_v1 looks like a successful Level 3 feature ✅

The new feature is doing what we wanted: it separates offensive-efficiency claims that are more likely to validate from claims that should stay cautious or context-only.

Big finding

The two positive buckets performed well:

Bucket	Validation Rate	Meaning
repeat_positive_strong	59.7%	Best candidate for stronger language review
repeat_positive_supportive	57.1%	Useful, but should use measured wording

That is meaningfully above the overall run baseline of about 49.2%.

Metric-level read

The strongest offensive-efficiency metrics inside this feature were:

Metric	Best Read
yards_per_pass	Surprisingly strong inside this feature
yards_per_play	Useful when anchored by points_per_play
points_per_play	Still the correct anchor
yards_per_rush	Useful, but weird/scoped — needs separate follow-up

For example, yards_per_pass in repeat_positive_strong validated at 62.7%, and yards_per_play in repeat_positive_supportive validated at 62.3%.

Caution logic was validated too 🚫

The metrics we blocked or softened performed poorly:

Metric	Validation Rate
td_rate	32.6%
turnover_margin_per_game	40.9%
red_zone_efficiency	44.3%

So the feature is not only finding good claims — it is also correctly identifying risky ones.

Product-surface test

The feature also survived when split by claim surface.

Strong examples:

Surface	Metric	Validation
metric_highlight headline	yards_per_pass	80.8%
core_area_summary supporting	yards_per_pass	73.1%
core_area_summary supporting	points_per_play	66.7%
metric_highlight headline	yards_per_play	65.5%

That means it is not just statistically interesting; it may be useful for actual GameLens language calibration.

Final decision

I would write this down as:

offensive_efficiency_support_v1 passed Level 3 dry-run and validation review.

It should be kept as metadata and promoted into Level 4 calibration exploration.

It should not directly change runtime language, matchup lean, confidence, Model Trust, frontend behavior, or winner logic yet.
Plain-English version

This feature helps answer:

“When GameLens says a team has an offensive-efficiency edge, is that claim more likely to be backed up after the game?”

Right now the answer is:

Yes — especially when the claim is anchored by repeat-positive points_per_play and supported by efficiency metrics like yards_per_pass, yards_per_play, or scoped yards_per_rush. 🎯