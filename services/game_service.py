from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
)
from services.model_trust_service import build_model_trust
from google.cloud import bigquery
from datetime import datetime, timezone


FINAL_STATUSES = {"Final", "Final/OT"}

MODEL_OUTCOMES_TABLE = "nfl-stream-406420.Analytics.game_model_outcomes"
MODEL_TRUST_DETAILS_TABLE = "nfl-stream-406420.Analytics.game_model_trust_details"


def fmt(val):
    return round(val, 3) if isinstance(val, float) else val


# =========================
# EXISTING BUILD FUNCTIONS
# =========================

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

    # Pressure
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

    # Turnovers
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

    # Scoring
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

    if isinstance(game_week, str) and game_week.startswith("Week "):
        try:
            return int(game_week.replace("Week ", ""))
        except:
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

        weight = 2 if level == "Elevated" else 1 if level == "Moderate" else 0

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

    target = away if score[away] > score[home] else home

    confidence = "High" if diff >= 5 else "Medium"

    week_num = get_week_number(header)
    if week_num and week_num <= 2:
        confidence = "Low"

    return {
        "target_team": f"{target} edge",
        "lean_summary": f"{target} holds the overall matchup edge",
        "focus_summary": f"{target} advantage driven by efficiency, pressure, and turnover profile",
        "confidence": confidence,
        "confidence_context": None,
    }


def build_model_outcome(matchup_lean: dict, final_score: dict, header: dict):
    if not final_score:
        return None

    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    predicted = matchup_lean.get("target_team", "").replace(" edge", "")

    away_total = final_score["away"]["total"]
    home_total = final_score["home"]["total"]

    actual_winner = away if away_total > home_total else home

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


# =========================
# NEW: BIGQUERY SAVE LOGIC
# =========================

def save_model_results(header, matchup_lean, model_outcome, model_trust):
    if not header or not matchup_lean or not model_outcome or not model_trust:
        return

    if header.get("game_status") not in FINAL_STATUSES:
        return

    game_id = header.get("game_id")
    if not game_id:
        return

    client = bigquery.Client()
    created_at = datetime.now(timezone.utc).isoformat()

    # Prevent duplicate outcome/detail inserts for this game
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

    predicted_team = model_outcome.get("predicted_team")
    actual_winner = model_outcome.get("actual_winner")
    result = model_outcome.get("result")

    away_abbr = header["away_team"]["abbreviation"]
    home_abbr = header["home_team"]["abbreviation"]

    if predicted_team == away_abbr:
        predicted_side = "away"
    elif predicted_team == home_abbr:
        predicted_side = "home"
    else:
        predicted_side = None

    result_lower = str(result or "").lower()

    if result_lower == "correct":
        result_code = "correct"
    elif result_lower == "incorrect":
        result_code = "incorrect"
    elif result_lower == "no pick":
        result_code = "no_pick"
    else:
        result_code = "unknown"

    confidence = matchup_lean.get("confidence")
    confidence_lower = str(confidence or "").lower()

    if confidence_lower in {"high", "medium", "low"}:
        confidence_tier = confidence_lower
    else:
        confidence_tier = "unknown"

    edge = model_trust.get("edge", {})
    signal_alignment = model_trust.get("signal_alignment", {})
    matchup_advantage = model_trust.get("matchup_advantage", {})

    outcome_row = {
        "game_id": str(game_id),
        "season": str(header.get("season") or ""),
        "game_week": str(header.get("game_week") or ""),

        "predicted_team": predicted_team,
        "predicted_side": predicted_side,
        "actual_winner": actual_winner,
        "result": str(result or ""),
        "result_code": result_code,

        "confidence": confidence,
        "confidence_tier": confidence_tier,
        "confidence_context": matchup_lean.get("confidence_context"),

        "edge_strength": edge.get("strength"),
        "edge_score": edge.get("score"),

        "signal_alignment_code": signal_alignment.get("summary_code"),
        "aligned_signal_count": signal_alignment.get("aligned_count"),
        "total_signal_count": signal_alignment.get("total_count"),

        "matchup_advantage_away": matchup_advantage.get("away"),
        "matchup_advantage_home": matchup_advantage.get("home"),
        "matchup_advantage_leader": matchup_advantage.get("leader"),

        "reason_tag": model_trust.get("learning_label"),

        "created_at": created_at,
    }

    outcome_errors = client.insert_rows_json(MODEL_OUTCOMES_TABLE, [outcome_row])

    if outcome_errors:
        raise RuntimeError(f"Failed to insert model outcome: {outcome_errors}")

    detail_rows = []

    reasoning = model_trust.get("reasoning", {})
    for driver in reasoning.get("drivers", []) or []:
        detail_rows.append({
            "game_id": str(game_id),
            "season": str(header.get("season") or ""),
            "game_week": str(header.get("game_week") or ""),
            "section": "reasoning",
            "category": driver.get("category"),
            "team_side": driver.get("team"),
            "favored_side": driver.get("team"),
            "aligns": None,
            "label": driver.get("label"),
            "sentence": driver.get("sentence"),
            "gap": driver.get("gap"),
            "impact": None,
            "created_at": created_at,
        })

    for signal in signal_alignment.get("signals", []) or []:
        detail_rows.append({
            "game_id": str(game_id),
            "season": str(header.get("season") or ""),
            "game_week": str(header.get("game_week") or ""),
            "section": "signal_alignment",
            "category": signal.get("category"),
            "team_side": None,
            "favored_side": signal.get("favored_side"),
            "aligns": signal.get("aligns"),
            "label": signal.get("category"),
            "sentence": signal.get("sentence"),
            "gap": None,
            "impact": None,
            "created_at": created_at,
        })

    if detail_rows:
        detail_errors = client.insert_rows_json(
            MODEL_TRUST_DETAILS_TABLE,
            detail_rows
        )

        if detail_errors:
            raise RuntimeError(f"Failed to insert model trust details: {detail_errors}")


# =========================
# MAIN FUNCTION
# =========================

def get_game_details(game_id: str) -> dict:
    header = get_game_header(game_id)

    if not header:
        return {
            "header": {},
            "final_score": None,
            "game_profile": [],
            "matchup_lean": {},
            "model_outcome": None,
            "model_trust": {},
            "team_comparison": [],
        }

    away_metrics, home_metrics = get_team_metrics(game_id)

    final_score = get_final_score(game_id)

    team_comparison = build_team_comparison(away_metrics, home_metrics)
    game_profile = build_game_profile(away_metrics, home_metrics, header)
    matchup_lean = build_matchup_lean(game_profile, team_comparison, header)
    model_outcome = build_model_outcome(matchup_lean, final_score, header)

    model_trust = build_model_trust(
        game_profile=game_profile,
        team_comparison=team_comparison,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        header=header,
    )

    save_model_results(
        header=header,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        model_trust=model_trust,
    )

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": game_profile,
        "matchup_lean": matchup_lean,
        "model_outcome": model_outcome,
        "model_trust": model_trust,
        "team_comparison": team_comparison,
    }