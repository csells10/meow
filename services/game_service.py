from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
    metric_value,
)
from services.model_trust_service import build_model_trust
from services.core_area_analysis import build_core_area_comparison
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
        ("Scoring Production::points_per_play", "Points per Play", "higher"),
        ("Scoring Suppression::points_allowed_per_play", "Points Allowed per Play", "lower"),
        ("Drive Conversion::third_down_pct", "3rd Down %", "higher"),
        ("Red Zone Finish::red_zone_efficiency", "Red Zone TD %", "higher"),
        ("Turnovers::turnover_margin", "Turnover Margin", "higher"),
    ]

    comparison = []

    for key, label, direction in METRICS:
        away_val = metric_value(away_metrics, key)
        home_val = metric_value(home_metrics, key)

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

    def get_level_index(level: str):
        return {
            "Neutral": 0,
            "Moderate": 1,
            "Elevated": 2,
            "High": 3
        }.get(level, 0)

    # --------------------
    # Pressure
    # --------------------
    away_pressure = metric_value(away_metrics, "Pressure::pressure_rate")
    home_pressure = metric_value(home_metrics, "Pressure::pressure_rate")

    if away_pressure is not None and home_pressure is not None:
        diff = compare(away_pressure, home_pressure)

        if abs(diff) > 0.05:
            level = "Elevated"
        elif abs(diff) > 0.02:
            level = "Moderate"
        else:
            level = "Neutral"

        if abs(diff) < 0.01:
            tilt_text = "Even matchup"
            tilt_team = "neutral"
        else:
            tilt_text = f"{away} generating more pressure" if diff > 0 else f"{home} generating more pressure"
            tilt_team = "away" if diff > 0 else "home"

        profile.append({
            "category": "Pressure",
            "level": level,
            "tilt": tilt_text,

            "level_index": get_level_index(level),
            "icon": "alert-triangle",
            "tilt_team": tilt_team,
            "tilt_text": tilt_text,
        })

    # --------------------
    # Turnover Environment
    # --------------------
    away_to = metric_value(away_metrics, "Turnovers::turnover_margin")
    home_to = metric_value(home_metrics, "Turnovers::turnover_margin")

    if away_to is not None and home_to is not None:
        diff = compare(away_to, home_to)

        if abs(diff) > 0.5:
            level = "Elevated"
        elif abs(diff) > 0.2:
            level = "Moderate"
        else:
            level = "Neutral"

        if abs(diff) < 0.01:
            tilt_text = "Even matchup"
            tilt_team = "neutral"
        else:
            tilt_text = f"{away} better turnover profile" if diff > 0 else f"{home} better turnover profile"
            tilt_team = "away" if diff > 0 else "home"

        profile.append({
            "category": "Turnover Risk",
            "level": level,
            "tilt": tilt_text,

            "level_index": get_level_index(level),
            "icon": "target",
            "tilt_team": tilt_team,
            "tilt_text": tilt_text,
        })

    # --------------------
    # Scoring
    # --------------------
    away_ppp = metric_value(away_metrics, "Scoring Production::points_per_play")
    home_ppp = metric_value(home_metrics, "Scoring Production::points_per_play")

    if away_ppp is not None and home_ppp is not None:
        diff = compare(away_ppp, home_ppp)

        if abs(diff) > 0.07:
            level = "Elevated"
        elif abs(diff) > 0.03:
            level = "Moderate"
        else:
            level = "Neutral"

        if abs(diff) < 0.01:
            tilt_text = "Even matchup"
            tilt_team = "neutral"
        else:
            tilt_text = f"{away} more efficient scoring" if diff > 0 else f"{home} more efficient scoring"
            tilt_team = "away" if diff > 0 else "home"

        profile.append({
            "category": "Scoring Efficiency",
            "level": level,
            "tilt": tilt_text,

            "level_index": get_level_index(level),
            "icon": "trending-up",
            "tilt_team": tilt_team,
            "tilt_text": tilt_text,
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


def build_core_area_context(core_area_comparison: list, lean_side=None):
    """
    Summarize Core Area Advantage into a simple matchup context.

    This is intentionally v1/simple:
    - Counts Core Area wins
    - Calculates average Core Area score
    - Detects split, coin-flip, confirmed, or conflicting profiles
    """

    context = {
        "available": False,
        "away_core_wins": 0,
        "home_core_wins": 0,
        "neutral_core_areas": 0,
        "total_core_areas": 0,
        "away_core_avg": None,
        "home_core_avg": None,
        "core_gap": None,
        "core_area_leader": "neutral",
        "core_area_split": None,
        "profile_type": "insufficient_core_area_context",
    }

    if not core_area_comparison:
        return context

    away_scores = []
    home_scores = []

    for area in core_area_comparison:
        leader = area.get("leader")

        if leader == "away":
            context["away_core_wins"] += 1
        elif leader == "home":
            context["home_core_wins"] += 1
        else:
            context["neutral_core_areas"] += 1

        away_score = area.get("away_score")
        home_score = area.get("home_score")

        if isinstance(away_score, (int, float)) and isinstance(home_score, (int, float)):
            away_scores.append(away_score)
            home_scores.append(home_score)

    total_core_areas = (
        context["away_core_wins"]
        + context["home_core_wins"]
        + context["neutral_core_areas"]
    )

    context["total_core_areas"] = total_core_areas

    if not away_scores or not home_scores:
        return context

    away_avg = sum(away_scores) / len(away_scores)
    home_avg = sum(home_scores) / len(home_scores)
    core_gap = abs(away_avg - home_avg)

    context["available"] = True
    context["away_core_avg"] = round(away_avg, 3)
    context["home_core_avg"] = round(home_avg, 3)
    context["core_gap"] = round(core_gap, 3)
    context["core_area_split"] = f"{context['away_core_wins']}-{context['home_core_wins']}"

    # Treat very small average gap as neutral / coin-flip
    if core_gap < 0.08:
        core_area_leader = "neutral"
    else:
        core_area_leader = "away" if away_avg > home_avg else "home"

    context["core_area_leader"] = core_area_leader

    away_wins = context["away_core_wins"]
    home_wins = context["home_core_wins"]

    # Profile classification
    if core_gap < 0.08:
        profile_type = "coin_flip_profile"
    elif away_wins >= 2 and home_wins >= 2:
        profile_type = "split_profile"
    elif lean_side in {"away", "home"} and core_area_leader == lean_side:
        profile_type = "confirmed_edge"
    elif lean_side in {"away", "home"} and core_area_leader not in {"neutral", lean_side}:
        profile_type = "conflicting_profile"
    else:
        profile_type = "mixed_profile"

    context["profile_type"] = profile_type

    return context

def build_matchup_lean(
    game_profile: list,
    team_comparison: list,
    header: dict,
    core_area_comparison: list = None,
):
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    score = {away: 0, home: 0}

    # --------------------
    # Team comparison scoring
    # --------------------
    for metric in team_comparison:
        better = metric.get("better")

        if better == "away":
            score[away] += 1
        elif better == "home":
            score[home] += 1

    # --------------------
    # Game profile signal scoring
    # --------------------
    for signal in game_profile:
        level = signal.get("level", "Neutral")
        weight = 2 if level == "Elevated" else 1 if level == "Moderate" else 0

        # Prefer structured backend field.
        # Keep tilt text fallback for backward compatibility.
        tilt_team = signal.get("tilt_team")

        if tilt_team == "away":
            score[away] += weight
        elif tilt_team == "home":
            score[home] += weight
        else:
            tilt = signal.get("tilt", "")
            if away in tilt:
                score[away] += weight
            elif home in tilt:
                score[home] += weight

    diff = abs(score[away] - score[home])

    signal_score = {
        "away": score[away],
        "home": score[home],
        "gap": diff,
    }

    # --------------------
    # No lean scenario
    # --------------------
    if diff < 3:
        core_area_context = build_core_area_context(
            core_area_comparison=core_area_comparison or [],
            lean_side=None,
        )

        return {
            "target_team": "None",
            "target_side": None,
            "lean_summary": "No strong directional edge",
            "focus_summary": "Signal gap too small to justify a lean",
            "confidence": "Low",
            "confidence_context": "Signals are not separated enough to support a clear lean",
            "profile_type": "no_clear_edge",
            "signal_score": signal_score,
            "core_area_context": core_area_context,
        }

    target = away if score[away] > score[home] else home
    target_side = "away" if target == away else "home"

    confidence = "High" if diff >= 5 else "Medium"

    week_num = get_week_number(header)
    if week_num and week_num <= 2:
        confidence = "Low"

    core_area_context = build_core_area_context(
        core_area_comparison=core_area_comparison or [],
        lean_side=target_side,
    )

    profile_type = core_area_context.get("profile_type")

    # --------------------
    # Core Area sanity check
    # --------------------
    if profile_type == "confirmed_edge":
        lean_summary = f"{target} holds the broader matchup edge"
        focus_summary = (
            f"{target} is supported by both signal scoring and Core Area advantage"
        )
        confidence_context = "Core Areas support the same side as the signal lean"

    elif profile_type == "coin_flip_profile":
        lean_summary = f"This matchup is close overall, with a slight lean toward {target}"
        focus_summary = (
            f"{target} has the stronger signal score, but Core Areas are nearly even overall"
        )
        confidence = "Low"
        confidence_context = "Core Areas are nearly even, limiting confidence"

    elif profile_type == "split_profile":
        lean_summary = f"{target} shows a slight signal lean, but the broader profile is mixed"
        focus_summary = (
            f"{target} has signal support, but Core Area wins are split across the matchup"
        )
        confidence = "Low" if confidence != "Low" else confidence
        confidence_context = "Core Areas are split, limiting confidence"

    elif profile_type == "conflicting_profile":
        lean_summary = f"{target} has signal support, but Core Areas do not fully confirm the edge"
        focus_summary = (
            f"{target} leads the signal score, but the broader Core Area profile points elsewhere"
        )
        confidence = "Low"
        confidence_context = "Core Area context conflicts with the signal lean"

    else:
        lean_summary = f"{target} shows a directional signal lean"
        focus_summary = (
            f"{target} advantage is driven by efficiency, pressure, and turnover signals"
        )
        confidence_context = "Core Area context is mixed or limited"

    return {
        "target_team": f"{target} edge",
        "target_side": target_side,
        "lean_summary": lean_summary,
        "focus_summary": focus_summary,
        "confidence": confidence,
        "confidence_context": confidence_context,
        "profile_type": profile_type,
        "signal_score": signal_score,
        "core_area_context": core_area_context,
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
    """
    Build the full /game/<game_id> API response.

    Backend owns all matchup/model logic.
    Frontend should render the structured response directly.
    """

    header = get_game_header(game_id)

    if not header:
        return {
            "header": {},
            "final_score": None,
            "game_profile": [],
            "matchup_lean": {},
            "model_outcome": None,
            "model_trust": {
                "reasoning": {
                    "headline": None,
                    "summary": None,
                    "has_content": False,
                    "drivers": [],
                },
                "matchup_advantage": {},
                "edge": {},
                "signal_alignment": {},
                "learning_label": "Outcome not available yet",
            },
            "team_comparison": [],
            "core_area_comparison": [],
        }

    away_metrics, home_metrics = get_team_metrics(game_id)
    final_score = get_final_score(game_id)

    team_comparison = build_team_comparison(
        away_metrics=away_metrics,
        home_metrics=home_metrics,
    )

    core_area_comparison = build_core_area_comparison(
        away_metrics=away_metrics,
        home_metrics=home_metrics,
        header=header,
    )

    game_profile = build_game_profile(
        away_metrics=away_metrics,
        home_metrics=home_metrics,
        header=header,
    )

    matchup_lean = build_matchup_lean(
        game_profile=game_profile,
        team_comparison=team_comparison,
        header=header,
        core_area_comparison=core_area_comparison,
    )

    model_outcome = build_model_outcome(
        matchup_lean=matchup_lean,
        final_score=final_score,
        header=header,
    )

    model_trust = build_model_trust(
        game_profile=game_profile,
        team_comparison=team_comparison,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        header=header,
    )

    game_status = str(header.get("game_status") or "").lower()

    if game_status in {"final", "final/ot"}:
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
        "model_trust": {
            "reasoning": model_trust.get("reasoning", {}),
            "matchup_advantage": model_trust.get("matchup_advantage", {}),
            "edge": model_trust.get("edge", {}),
            "signal_alignment": model_trust.get("signal_alignment", {}),
            "learning_label": model_trust.get("learning_label"),
        },
        "team_comparison": team_comparison,
        "core_area_comparison": core_area_comparison,
    }