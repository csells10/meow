# NFL Metric Classification Map — Option B

This is the current production-friendly metric structure:

```text
core_area
  category
    metric
```

## Summary

- Core areas: 5
- Categories: 13
- Metrics: 58
- Removed stale metric: `sacks_plus_taken`
- `turnover_margin` is kept only once as a derived/cumulative metric.

---

# Defensive Control

## Defensive Workload

- `defensive_snap_load`
- `total_defensive_snaps`

## Scoring Suppression

- `points_allowed`
- `points_allowed_per_play`

## Yardage Suppression

- `defensive_success_rate`
- `opponent_total_plays`
- `points_allowed_per_yard`
- `yards_allowed`

---

# Disruption and Turnovers

## Pressure

- `pressure_rate`
- `sack_to_turnover_ratio`
- `sack_yards_lost`
- `sacks`
- `sacks_plus_sacks_taken`
- `sacks_taken`

## Turnovers

- `defensive_interceptions`
- `fumbles_lost`
- `fumbles_recovered`
- `interceptions_thrown`
- `turnover_margin`

---

# Field Control (Special Teams)

## Special Teams Usage

- `special_teams_snap_pct`
- `total_snaps`
- `total_special_teams_snaps`

---

# Offensive Output

## Offensive Rhythm

- `1st_down_rate`
- `first_downs`
- `offensive_snap_load`
- `pass_play_pct`
- `pass_run_ratio`
- `pass_td_share`
- `passing_tds_rushing_tds_sum`
- `run_play_pct`
- `rush_td_share`
- `time_of_possession`
- `total_drives`
- `total_offensive_snaps`
- `total_plays`
- `total_yards`
- `yards_per_play`

## Passing Game

- `pass_attempts`
- `pass_completions`
- `passing_tds`
- `passing_yards`
- `yards_per_pass`

## Rushing Game

- `rushing_attempts`
- `rushing_tds`
- `rushing_yards`
- `yards_per_rush`

---

# Scoring Efficiency

## Drive Conversion

- `fourth_down_attempts`
- `fourth_down_conversions`
- `fourth_down_pct`
- `third_down_attempts`
- `third_down_conversions`
- `third_down_pct`

## Red Zone Finish

- `red_zone_attempts`
- `red_zone_efficiency`
- `red_zone_tds`

## Scoring Production

- `actual_points`
- `points_per_play`
- `td_rate`
