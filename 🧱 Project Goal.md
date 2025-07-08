🧱 Project Goal
Build season-to-date and rolling aggregate metrics for all NFL teams using data from:

nfl-stream-406420.Analytics.game_metrics_flat

This will allow analysis of performance across different time windows with correctly recalculated stats.

✅ Time Windows to Support
Each time window will generate its own derived output table:

today — (already handled in base table)

last_3_games — last 3 games per team

last_7_games — last 7 games per team

this_season — current season (starting ~Aug 2024)

last_season — previous season (e.g., Aug 2023–Feb 2024)

🧮 Key Metrics to Recalculate
For each time window, metrics like:

points_per_play = sum(points) / sum(total_plays)

red_zone_efficiency = sum(rz_tds) / sum(rz_attempts)

turnover_margin = sum(turnover_margin)

defensive_success_rate = sum(yards_allowed) / sum(opponent_plays)

Other metrics rebuilt from raw ingredients

Avoiding naïve averages ensures more accurate, normalized comparisons.

📄 Target Output Schema (per window)
Each output table (e.g., team_metrics_last_3_games) will have this structure:

Column	Description
team_id	Unique ID of the team
team_abv	Team abbreviation (e.g., KC, PHI)
metric	Name of the metric (e.g., points_per_play)
category	Logical grouping (e.g., Scoring)
core_area	High-level type (e.g., Offense)
value	Recalculated value for that window

This avoids wide-column tables and makes it easier to query, display, or chart dynamically.

📌 Notes
Each table contains only one window — no need for a window column

This approach scales easily for more windows or custom date filters

Derived tables can be UNIONed later for comparisons if needed

🛠️ Next Step
Start by writing a Python script that:

Pulls raw game-level stats from game_metrics_flat

Pivots them wide

Filters by window

Recalculates all derived metrics correctly

Outputs one table per time window in the long format shown above

You’re set up perfectly to run with this — feel free to drop back in once you're ready to build it out. I’ll be here.