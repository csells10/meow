from collections import defaultdict


ACTIVE_CORE_AREAS = {
    "Defensive Control",
    "Disruption and Turnovers",
    "Offensive Output",
    "Scoring Efficiency",
    "Field Control (Special Teams)",
}


LOWER_IS_BETTER = {
    "points_allowed",
    "yards_allowed",
    "points_allowed_per_yard",
    "points_allowed_per_play",
    "defensive_success_rate",
    "sacks_taken",
    "sack_yards_lost",
    "interceptions_thrown",
    "fumbles_lost",
}


def normalize_pair(away_value, home_value, lower_is_better=False):
    if away_value == home_value:
        return 0.5, 0.5

    min_value = min(away_value, home_value)
    max_value = max(away_value, home_value)

    if max_value - min_value == 0:
        return 0.5, 0.5

    away_score = (away_value - min_value) / (max_value - min_value)
    home_score = (home_value - min_value) / (max_value - min_value)

    if lower_is_better:
        away_score = 1 - away_score
        home_score = 1 - home_score

    return away_score, home_score


def extract_metric_value(metric_payload):
    """
    Supports either:
    - raw numeric value
    - dict payload: {"value": 0.308, "core_area": "...", "metric": "..."}
    """

    if isinstance(metric_payload, dict):
        return metric_payload.get("value")

    return metric_payload


def extract_metric_metadata(metric_key, metric_payload):
    """
    Preferred shape:
    {
        "value": 0.308,
        "metric": "points_per_play",
        "core_area": "Scoring Efficiency"
    }

    Legacy fallback:
    "Category::metric_name"
    """

    if isinstance(metric_payload, dict):
        metric = metric_payload.get("metric")
        core_area = metric_payload.get("core_area")
        return metric, core_area

    if "::" in metric_key:
        _, metric = metric_key.split("::", 1)
        return metric, None

    return metric_key, None


def build_core_area_comparison(away_metrics: dict, home_metrics: dict, header: dict) -> list:
    """
    Build Core Area comparison scores between away and home teams.

    Preferred input shape:

    {
        "Scoring & Efficiency::points_per_play": {
            "value": 0.308,
            "metric": "points_per_play",
            "core_area": "Scoring Efficiency"
        }
    }

    Output shape:

    [
        {
            "core_area": "Scoring Efficiency",
            "away_score": 0.667,
            "home_score": 0.333,
            "leader": "away",
            "metric_count": 3
        }
    ]
    """

    core_scores = defaultdict(lambda: {"away": [], "home": []})

    for key, away_payload in (away_metrics or {}).items():
        if key not in (home_metrics or {}):
            continue

        home_payload = home_metrics.get(key)

        metric, core_area = extract_metric_metadata(key, away_payload)

        if not core_area:
            continue

        if core_area not in ACTIVE_CORE_AREAS:
            continue

        away_value = extract_metric_value(away_payload)
        home_value = extract_metric_value(home_payload)

        if away_value is None or home_value is None:
            continue

        try:
            away_num = float(away_value)
            home_num = float(home_value)
        except (TypeError, ValueError):
            continue

        lower_is_better = metric in LOWER_IS_BETTER

        away_score, home_score = normalize_pair(
            away_value=away_num,
            home_value=home_num,
            lower_is_better=lower_is_better,
        )

        core_scores[core_area]["away"].append(away_score)
        core_scores[core_area]["home"].append(home_score)

    results = []

    for core_area, scores in core_scores.items():
        away_values = scores["away"]
        home_values = scores["home"]

        if not away_values or not home_values:
            continue

        away_score = sum(away_values) / len(away_values)
        home_score = sum(home_values) / len(home_values)

        if away_score > home_score:
            leader = "away"
        elif home_score > away_score:
            leader = "home"
        else:
            leader = "neutral"

        results.append({
            "core_area": core_area,
            "away_score": round(away_score, 3),
            "home_score": round(home_score, 3),
            "leader": leader,
            "metric_count": len(away_values),
        })

    return sorted(results, key=lambda row: row["core_area"])