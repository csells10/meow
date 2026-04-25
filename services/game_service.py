from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
)


FINAL_STATUSES = {"Final", "Final/OT"}


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

        tilt = (
            f"{away} generating more pressure"
            if diff > 0
            else f"{home} generating more pressure"
        )

        profile.append({
            "category": "Pressure",
            "level": level,
            "tilt": tilt,
        })

    # Turnover Environment
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

        tilt = (
            f"{away} better turnover profile"
            if diff > 0
            else f"{home} better turnover profile"
        )

        profile.append({
            "category": "Turnover Environment",
            "level": level,
            "tilt": tilt,
        })

    # Scoring Efficiency
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

        tilt = (
            f"{away} more efficient scoring"
            if diff > 0
            else f"{home} more efficient scoring"
        )

        profile.append({
            "category": "Scoring",
            "level": level,
            "tilt": tilt,
        })

    return profile


def build_matchup_lean(game_profile: list, team_comparison: list, header: dict):
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    score = {
        away: 0,
        home: 0,
    }

    # Score direct metric advantages
    for metric in team_comparison:
        better = metric.get("better")

        if better == "away":
            score[away] += 1
        elif better == "home":
            score[home] += 1

    # Score profile signals with weighting
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

    if score[away] > score[home]:
        target = away
    elif score[home] > score[away]:
        target = home
    else:
        target = None

    if target is None:
        return {
            "target_team": "None",
            "lean_summary": "No strong directional edge",
            "focus_summary": "Balanced matchup — no clear prop direction",
            "confidence": "Low",
        }

    opponent = home if target == away else away
    diff = abs(score[away] - score[home])

    if diff >= 5:
        confidence = "High"
    elif diff >= 3:
        confidence = "Medium"
    else:
        confidence = "Low"

    return {
        "target_team": f"{target} edge",
        "lean_summary": f"{target} holds the overall matchup edge vs {opponent}",
        "focus_summary": f"{target} advantage driven by efficiency, pressure, and turnover profile",
        "confidence": confidence,
    }


def build_model_outcome(matchup_lean: dict, final_score: dict, header: dict):
    """
    Evaluates whether the matchup lean was correct.

    V1 logic:
    - Only evaluates final games
    - If predicted team wins: Correct
    - If predicted team loses: Incorrect
    - If no target team: No Pick
    """
    if not final_score:
        return None

    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    predicted = matchup_lean.get("target_team", "")
    predicted = predicted.replace(" edge", "")

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


def get_game_details(game_id: str) -> dict:
    """
    Build the V1 game details response.

    Includes:
    - header
    - final_score
    - team_comparison
    - game_profile
    - matchup_lean
    - model_outcome
    """
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

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": game_profile,
        "matchup_lean": matchup_lean,
        "model_outcome": model_outcome,
        "team_comparison": team_comparison,
    }