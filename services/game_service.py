from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
)
from google.cloud import bigquery
from datetime import datetime, timezone


FINAL_STATUSES = {"Final", "Final/OT"}
MODEL_OUTCOMES_TABLE = "nfl-stream-406420.Analytics.game_model_outcomes"


def fmt(val):
    return round(val, 3) if isinstance(val, float) else val


def build_team_comparison(away_metrics: dict, home_metrics: dict):
    METRICS = [
        ("Scoring & Efficiency::points_per_play", "Points per Play", "higher"),
        ("Scoring & Efficiency::points_allowed_per_play", "Points Allowed per Play", "lower"),
        ("Red Zone & Conversion::third_down_pct", "3rd Down %", "higher"),
        ("Red Zone & Conversion::red_zone_efficiency", "Red Zone TD %", "higher"),
        ("Defense::turnover_margin_per_game", "Turnover Margin / Game", "higher"),
    ]

    comparison = []

    for key, label, direction in METRICS:
        away_val = away_metrics.get(key)
        home_val = home_metrics.get(key)

        if away_val is None or home_val is None:
            continue

        if direction == "higher":
            better = "away" if away_val > home_val else "home"
        else:
            better = "away" if away_val < home_val else "home"

        comparison.append({
            "label": label,
            "away": fmt(away_val),
            "home": fmt(home_val),
            "better": better,
        })

    return comparison


def build_game_profile(away_metrics: dict, home_metrics: dict, header: dict):
    profile = []

    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    def compare(a, b):
        if a is None or b is None:
            return None
        return a - b

    away_pressure = away_metrics.get("Pressure & Turnovers::pressure_rate")
    home_pressure = home_metrics.get("Pressure & Turnovers::pressure_rate")

    if away_pressure is not None and home_pressure is not None:
        diff = compare(away_pressure, home_pressure)

        if abs(diff) > 0.05:
            level = "Elevated"
        elif abs(diff) > 0.02:
            level = "Moderate"
        else:
            level = "Neutral"

        profile.append({
            "category": "Pressure",
            "level": level,
            "tilt": f"{away} generating more pressure" if diff > 0 else f"{home} generating more pressure",
        })

    away_to = away_metrics.get("Defense::turnover_margin_per_game")
    home_to = home_metrics.get("Defense::turnover_margin_per_game")

    if away_to is not None and home_to is not None:
        diff = compare(away_to, home_to)

        if abs(diff) > 0.5:
            level = "Elevated"
        elif abs(diff) > 0.2:
            level = "Moderate"
        else:
            level = "Neutral"

        profile.append({
            "category": "Turnover Environment",
            "level": level,
            "tilt": f"{away} better turnover profile" if diff > 0 else f"{home} better turnover profile",
        })

    away_ppp = away_metrics.get("Scoring & Efficiency::points_per_play")
    home_ppp = home_metrics.get("Scoring & Efficiency::points_per_play")

    if away_ppp is not None and home_ppp is not None:
        diff = compare(away_ppp, home_ppp)

        if abs(diff) > 0.07:
            level = "Elevated"
        elif abs(diff) > 0.03:
            level = "Moderate"
        else:
            level = "Neutral"

        profile.append({
            "category": "Scoring",
            "level": level,
            "tilt": f"{away} more efficient scoring" if diff > 0 else f"{home} more efficient scoring",
        })

    return profile


def get_week_number(header: dict):
    game_week = header.get("game_week")

    if not game_week:
        return None

    if isinstance(game_week, int):
        return game_week

    if isinstance(game_week, str) and game_week.startswith("Week "):
        try:
            return int(game_week.replace("Week ", ""))
        except ValueError:
            return None

    return None


def build_matchup_lean(game_profile: list, team_comparison: list, header: dict):
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    score = {away: 0, home: 0}

    for metric in team_comparison:
        better = metric.get("better")

        if better == "away":
            score[away] += 1
        elif better == "home":
            score[home] += 1

    for signal in game_profile:
        tilt = signal.get("tilt", "")
        level = signal.get("level", "Neutral")

        if level == "Elevated":
            weight = 2
        elif level == "Moderate":
            weight = 1
        else:
            weight = 0

        if away in tilt:
            score[away] += weight
        elif home in tilt:
            score[home] += weight

    diff = abs(score[away] - score[home])

    if diff < 3:
        return {
            "target_team": "None",
            "lean_summary": "No strong directional edge",
            "focus_summary": "Signal gap too small to justify a lean",
            "confidence": "Low",
            "confidence_context": None,
        }

    if score[away] > score[home]:
        target = away
        opponent = home
    else:
        target = home
        opponent = away

    if diff >= 5:
        confidence = "High"
    elif diff >= 3:
        confidence = "Medium"
    else:
        confidence = "Low"

    confidence_context = None
    week_num = get_week_number(header)

    if week_num is not None:
        if week_num <= 2:
            confidence = "Low"
            confidence_context = "Early season — limited sample size"
        elif week_num == 3 and confidence == "High":
            confidence = "Medium"
            confidence_context = "Early season — limited sample size"

    return {
        "target_team": f"{target} edge",
        "lean_summary": f"{target} holds the overall matchup edge vs {opponent}",
        "focus_summary": f"{target} advantage driven by efficiency, pressure, and turnover profile",
        "confidence": confidence,
        "confidence_context": confidence_context,
    }


def build_model_outcome(matchup_lean: dict, final_score: dict, header: dict):
    if not final_score:
        return None

    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    predicted = matchup_lean.get("target_team", "").replace(" edge", "")

    away_total = final_score["away"]["total"]
    home_total = final_score["home"]["total"]

    if away_total > home_total:
        actual_winner = away
    elif home_total > away_total:
        actual_winner = home
    else:
        actual_winner = "Tie"

    if predicted not in [away, home]:
        result = "No Pick"
    elif predicted == actual_winner:
        result = "Correct"
    else:
        result = "Incorrect"

    return {
        "result": result,
        "actual_winner": actual_winner,
        "predicted_team": predicted,
    }


def save_model_outcome_if_needed(header: dict, matchup_lean: dict, model_outcome: dict):
    if not header or not matchup_lean or not model_outcome:
        return

    if header.get("game_status") not in FINAL_STATUSES:
        return

    game_id = header.get("game_id")
    if not game_id:
        return

    client = bigquery.Client()

    check_query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{MODEL_OUTCOMES_TABLE}`
        WHERE game_id = @game_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", str(game_id))
        ]
    )

    existing_rows = list(client.query(check_query, job_config=job_config).result())
    existing_count = existing_rows[0]["row_count"] if existing_rows else 0

    if existing_count > 0:
        return

    row = {
        "game_id": str(game_id),
        "season": str(header.get("season") or ""),
        "game_week": str(header.get("game_week") or ""),
        "predicted_team": model_outcome.get("predicted_team"),
        "actual_winner": model_outcome.get("actual_winner"),
        "result": str(model_outcome.get("result") or ""),
        "confidence": matchup_lean.get("confidence"),
        "confidence_context": matchup_lean.get("confidence_context"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    errors = client.insert_rows_json(MODEL_OUTCOMES_TABLE, [row])

    if errors:
        raise RuntimeError(f"Failed to insert model outcome: {errors}")


def get_game_details(game_id: str) -> dict:
    header = get_game_header(game_id)

    if not header:
        return {
            "header": {},
            "final_score": None,
            "game_profile": [],
            "matchup_lean": {},
            "model_outcome": None,
            "team_comparison": [],
        }

    away_metrics, home_metrics = get_team_metrics(game_id)

    game_status = header.get("game_status")
    final_score = get_final_score(game_id) if game_status in FINAL_STATUSES else None

    team_comparison = build_team_comparison(away_metrics, home_metrics)
    game_profile = build_game_profile(away_metrics, home_metrics, header)
    matchup_lean = build_matchup_lean(game_profile, team_comparison, header)
    model_outcome = build_model_outcome(matchup_lean, final_score, header)

    save_model_outcome_if_needed(header, matchup_lean, model_outcome)

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": game_profile,
        "matchup_lean": matchup_lean,
        "model_outcome": model_outcome,
        "team_comparison": team_comparison,
    }