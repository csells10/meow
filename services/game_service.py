from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
    get_team_rankings_for_game,
    metric_value,
)
from services.model_trust_service import build_model_trust
from services.core_area_analysis import build_core_area_comparison
from services.claim_language_features import (
    build_runtime_offensive_efficiency_support_by_side,
    build_runtime_two_way_context_by_side,
)
from services.claim_language_response import apply_claim_language_support_to_response_sections
from services.confidence_calibration import (
    INSUFFICIENT_DURABILITY_REASON,
    apply_core_area_durability_confidence_calibration,
)
from services.gamelens_pregame_contract import (
    identify_pregame_payload,
    require_pregame_payload,
)
from utils.logging_setup import log_event
from google.cloud import bigquery
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


FINAL_STATUSES = {"Final", "Final/OT"}

MODEL_OUTCOMES_TABLE = "nfl-stream-406420.Analytics.game_model_outcomes"
MODEL_TRUST_DETAILS_TABLE = "nfl-stream-406420.Analytics.game_model_trust_details"


# Featured ranking metrics are only for lightweight QA/API visibility.
# This avoids dumping every ranking row into /game while still proving the
# ranking layer is connected and useful.
RANKING_FEATURED_METRICS = [
    "points_per_play",
    "points_allowed_per_play",
    "yards_per_play",
    "red_zone_efficiency",
    "third_down_pct",
    "turnover_margin",
    "pass_attempts",
]


@dataclass(frozen=True)
class GameDetailsEvidence:
    """Inputs consumed by the one canonical game-response builder."""

    header: dict
    away_metrics: dict
    home_metrics: dict
    ranking_context: dict
    away_rankings: dict
    home_rankings: dict
    final_score: Optional[dict] = None


def fmt(val):
    return round(val, 3) if isinstance(val, float) else val


def build_unavailable_ranking_context(reason: str, game_id: str = None) -> dict:
    return {
        "available": False,
        "reason": reason,
        "game_id": game_id,
        "meta": {},
        "featured_metrics": [],
    }


def _ranking_side_payload(row: dict) -> dict:
    """
    Keep ranking response lightweight.

    Full ranking rows stay query-side for now. The API exposes enough for QA
    and future product summaries without overwhelming the frontend.
    """

    if not row:
        return None

    return {
        "team_id": row.get("team_id"),
        "team_abv": row.get("team_abv"),
        "value": row.get("value"),
        "league_rank": row.get("league_rank"),
        "league_percentile": row.get("league_percentile"),
        "tier": row.get("tier"),
        "tier_label": row.get("tier_label"),
        "teams_ranked": row.get("teams_ranked"),
        "source_data_date": row.get("source_data_date"),
        "data_lag_days": row.get("data_lag_days"),
    }


def build_ranking_context(
    away_rankings: dict,
    home_rankings: dict,
    ranking_meta: dict,
) -> dict:
    """
    Build a small ranking_context payload for /game.

    Purpose:
    - Confirm rankings are available for the requested game.
    - Show a few useful metric examples.
    - Preserve freshness metadata.
    - Do not change model/pick/confidence behavior yet.
    """

    if not ranking_meta or not ranking_meta.get("available"):
        return {
            "available": False,
            "reason": (ranking_meta or {}).get("reason", "ranking_context_unavailable"),
            "meta": ranking_meta or {},
            "featured_metrics": [],
        }

    featured_metrics = []

    for metric in RANKING_FEATURED_METRICS:
        away_row = away_rankings.get(metric)
        home_row = home_rankings.get(metric)

        if not away_row and not home_row:
            continue

        source_row = away_row or home_row

        featured_metrics.append({
            "metric": metric,
            "label": source_row.get("label"),
            "definition": source_row.get("definition"),
            "category": source_row.get("category"),
            "core_area": source_row.get("core_area"),

            "comparison_direction": source_row.get("comparison_direction"),
            "ranking_usage": source_row.get("ranking_usage"),
            "ranking_kind": source_row.get("ranking_kind"),
            "rank_direction": source_row.get("rank_direction"),
            "rank_interpretation": source_row.get("rank_interpretation"),

            "signal_strength": source_row.get("signal_strength"),
            "edge_language_allowed": source_row.get("edge_language_allowed"),
            "include_in_core_area_advantage": source_row.get("include_in_core_area_advantage"),
            "confidence_eligible": source_row.get("confidence_eligible"),
            "data_quality_status": source_row.get("data_quality_status"),
            "lens_tags": source_row.get("lens_tags") or [],

            "away": _ranking_side_payload(away_row),
            "home": _ranking_side_payload(home_row),
        })

    return {
        "available": True,
        "meta": ranking_meta,
        "featured_metrics": featured_metrics,
    }


# =========================
# EXISTING BUILD FUNCTIONS
# =========================

def build_team_comparison(
    away_metrics: dict,
    home_metrics: dict,
    away_rankings: dict = None,
    home_rankings: dict = None,
):
    """
    Build the visible Team Comparison rows.

    Notes:
    - This is currently a curated V1 comparison set.
    - Exact equal values return neutral instead of defaulting to home/away.
    - Near-even percentile gaps also return neutral for scoring/model purposes.
    - technical_better preserves which side was numerically better for display/debug.
    - Dynamic metric selection should be handled later through rankings,
      category summaries, or Core Area summaries.
    """

    NEAR_EVEN_PERCENTILE_GAP = 3.5

    METRICS = [
        (
            "Scoring Production::points_per_play",
            "points_per_play",
            "Points per Play",
            "higher",
        ),
        (
            "Scoring Suppression::points_allowed_per_play",
            "points_allowed_per_play",
            "Points Allowed per Play",
            "lower",
        ),
        (
            "Drive Conversion::third_down_pct",
            "third_down_pct",
            "3rd Down %",
            "higher",
        ),
        (
            "Red Zone Finish::red_zone_efficiency",
            "red_zone_efficiency",
            "Red Zone TD %",
            "higher",
        ),
        (
            "Turnovers::turnover_margin_per_game",
            "turnover_margin_per_game",
            "Turnover Margin / Game",
            "higher",
        ),
    ]

    away_rankings = away_rankings or {}
    home_rankings = home_rankings or {}

    def _safe_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _ranking_gap(away_ranking: dict = None, home_ranking: dict = None):
        percentile_gap = None
        rank_gap = None

        if away_ranking and home_ranking:
            away_percentile = _safe_float(away_ranking.get("league_percentile"))
            home_percentile = _safe_float(home_ranking.get("league_percentile"))

            if away_percentile is not None and home_percentile is not None:
                percentile_gap = abs(away_percentile - home_percentile)

            away_rank = _safe_int(away_ranking.get("league_rank"))
            home_rank = _safe_int(home_ranking.get("league_rank"))

            if away_rank is not None and home_rank is not None:
                rank_gap = abs(away_rank - home_rank)

        return percentile_gap, rank_gap

    def resolve_comparison(
        away_val,
        home_val,
        direction: str,
        away_ranking: dict = None,
        home_ranking: dict = None,
    ):
        """
        Return comparison details for a visible Team Comparison metric.

        better:
            The side that should count for model scoring.
            Near-even rows are intentionally scored as neutral.

        technical_better:
            The side that was numerically better before guardrails.
            This is useful for UI/debug language.
        """

        away_num = _safe_float(away_val)
        home_num = _safe_float(home_val)
        percentile_gap, rank_gap = _ranking_gap(away_ranking, home_ranking)

        if away_num is None or home_num is None:
            return {
                "better": "neutral",
                "comparison_strength": "neutral",
                "technical_better": "neutral",
                "raw_gap": None,
                "percentile_gap": None,
                "rank_gap": None,
                "near_even_reason": None,
            }

        raw_gap = abs(away_num - home_num)

        # Tiny tolerance only protects against floating point weirdness.
        # This is not a football threshold.
        if raw_gap < 1e-9:
            return {
                "better": "neutral",
                "comparison_strength": "neutral",
                "technical_better": "neutral",
                "raw_gap": 0,
                "percentile_gap": (
                    round(percentile_gap, 3)
                    if percentile_gap is not None
                    else None
                ),
                "rank_gap": rank_gap,
                "near_even_reason": "exact_tie",
            }

        if direction == "higher":
            technical_better = "away" if away_num > home_num else "home"
        elif direction == "lower":
            technical_better = "away" if away_num < home_num else "home"
        else:
            technical_better = "neutral"

        # Ranking-aware near-even guardrail:
        # if two teams are basically adjacent in league percentile, do not
        # count the row as a full model edge even if one raw value is better.
        if percentile_gap is not None and percentile_gap <= NEAR_EVEN_PERCENTILE_GAP:
            return {
                "better": "neutral",
                "comparison_strength": "near_even",
                "technical_better": technical_better,
                "raw_gap": round(raw_gap, 6),
                "percentile_gap": round(percentile_gap, 3),
                "rank_gap": rank_gap,
                "near_even_reason": "small_percentile_gap",
            }

        return {
            "better": technical_better,
            "comparison_strength": "edge",
            "technical_better": technical_better,
            "raw_gap": round(raw_gap, 6),
            "percentile_gap": (
                round(percentile_gap, 3)
                if percentile_gap is not None
                else None
            ),
            "rank_gap": rank_gap,
            "near_even_reason": None,
        }

    comparison = []

    for key, metric_name, label, direction in METRICS:
        away_val = metric_value(away_metrics, key)
        home_val = metric_value(home_metrics, key)

        if away_val is None or home_val is None:
            continue

        comparison_result = resolve_comparison(
            away_val=away_val,
            home_val=home_val,
            direction=direction,
            away_ranking=away_rankings.get(metric_name),
            home_ranking=home_rankings.get(metric_name),
        )

        comparison.append({
            "label": label,
            "metric": metric_name,
            "away": fmt(away_val),
            "home": fmt(home_val),
            "better": comparison_result["better"],
            "comparison_strength": comparison_result["comparison_strength"],
            "technical_better": comparison_result["technical_better"],
            "raw_gap": comparison_result["raw_gap"],
            "percentile_gap": comparison_result["percentile_gap"],
            "rank_gap": comparison_result["rank_gap"],
            "near_even_reason": comparison_result["near_even_reason"],
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
    away_to = metric_value(away_metrics, "Turnovers::turnover_margin_per_game")
    home_to = metric_value(home_metrics, "Turnovers::turnover_margin_per_game")

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
        except Exception:
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
def build_team_comparison_edge_context(team_comparison: list) -> dict:
    """
    Summarize visible Team Comparison strength for confidence guardrails.

    This intentionally mirrors the idea behind model_trust.edge, but it runs
    before model_trust is built so matchup_lean can avoid overconfident labels.

    Notes:
    - near_even rows should already have better="neutral"
    - neutral and near_even rows do not count as away/home edges
    - this is a guardrail, not a replacement for Core Area or Game Profile logic
    """

    context = {
        "available": bool(team_comparison),
        "away": 0,
        "home": 0,
        "neutral": 0,
        "near_even": 0,
        "decisive": 0,
        "total_visible": len(team_comparison or []),
        "leader": "tie",
        "gap": 0,
        "edge_score": 0,
        "edge_strength": "none",
    }

    for row in team_comparison or []:
        better = row.get("better")
        comparison_strength = row.get("comparison_strength")

        if comparison_strength == "near_even":
            context["near_even"] += 1

        if better == "away":
            context["away"] += 1
        elif better == "home":
            context["home"] += 1
        else:
            context["neutral"] += 1

    away_count = context["away"]
    home_count = context["home"]

    decisive = away_count + home_count
    gap = abs(away_count - home_count)

    context["decisive"] = decisive
    context["gap"] = gap

    if away_count > home_count:
        context["leader"] = "away"
    elif home_count > away_count:
        context["leader"] = "home"
    else:
        context["leader"] = "tie"

    context["edge_score"] = round(gap / decisive, 2) if decisive else 0

    if gap >= 3:
        edge_strength = "strong"
    elif gap >= 2:
        edge_strength = "moderate"
    elif gap >= 1:
        edge_strength = "low"
    else:
        edge_strength = "none"

    context["edge_strength"] = edge_strength

    return context


def build_profile_strength_labels(
    target: str = None,
    confidence: str = "Low",
    profile_type: str = None,
    core_area_context: dict = None,
    team_edge_context: dict = None,
    signal_score: dict = None,
) -> dict:
    """
    Separate matchup profile strength from outcome confidence.

    profile_strength answers:
    - How clean/strong does the matchup shape look?

    outcome_confidence answers:
    - How much should we trust the lean as a winner/outcome read?

    This does not change picks, legacy signal confidence, target_team,
    profile_type, or model_outcome. It may soften only the user-facing
    outcome-confidence label through the shared calibration helper.
    """

    core_area_context = core_area_context or {}
    team_edge_context = team_edge_context or {}
    signal_score = signal_score or {}

    signal_gap = signal_score.get("gap") or 0
    edge_strength = team_edge_context.get("edge_strength", "none")
    core_gap = core_area_context.get("core_gap")
    total_core_areas = core_area_context.get("total_core_areas") or 0

    cautions = []

    if not target or profile_type == "no_clear_edge":
        profile_strength = {
            "code": "no_clear_edge",
            "label": "No Clear Edge",
            "summary": "The available signals do not create a clean matchup lean.",
        }

    elif profile_type == "confirmed_edge":
        if (
            signal_gap >= 7
            and edge_strength in {"strong", "moderate"}
            and total_core_areas >= 4
            and core_gap is not None
            and core_gap >= 0.15
        ):
            profile_strength = {
                "code": "strong_profile",
                "label": "Strong Profile",
                "summary": f"{target} has a clean pregame profile across multiple matchup layers.",
            }
        else:
            profile_strength = {
                "code": "clear_lean",
                "label": "Clear Lean",
                "summary": f"{target} has the matchup lean, but the profile is not overwhelming.",
            }

    elif profile_type == "coin_flip_profile":
        profile_strength = {
            "code": "thin_edge",
            "label": "Thin Edge",
            "summary": f"{target} has a lean, but the matchup is close overall.",
        }
        cautions.append("core_area_gap_is_small")

    elif profile_type == "split_profile":
        profile_strength = {
            "code": "mixed_profile",
            "label": "Mixed Profile",
            "summary": f"{target} has signal support, but the matchup is split across key areas.",
        }
        cautions.append("core_areas_are_split")

    elif profile_type == "conflicting_profile":
        profile_strength = {
            "code": "mixed_profile",
            "label": "Mixed Profile",
            "summary": f"{target} has signal support, but the broader matchup pushes back.",
        }
        cautions.append("core_areas_do_not_fully_confirm_lean")

    else:
        profile_strength = {
            "code": "lean_with_context",
            "label": "Lean With Context",
            "summary": f"{target} has the lean, but the supporting profile is not fully settled.",
        }
        cautions.append("supporting_context_is_mixed_or_limited")

    if not target or confidence == "Low":
        outcome_confidence = {
            "code": "low",
            "label": "Low",
            "summary": "This should be treated cautiously rather than as a strong outcome read.",
        }

    elif confidence == "Medium":
        outcome_confidence = {
            "code": "medium",
            "label": "Medium",
            "summary": "The lean is usable, but not strong enough to treat as a high-confidence outcome.",
        }

    elif confidence == "High":
        if (
            profile_strength.get("code") == "strong_profile"
            and edge_strength == "strong"
            and signal_gap >= 8
            and core_gap is not None
            and core_gap >= 0.25
        ):
            outcome_confidence = {
                "code": "high",
                "label": "High",
                "summary": "The lean is backed by a clean profile and strong separation.",
            }
        else:
            outcome_confidence = {
                "code": "medium",
                "label": "Medium",
                "summary": "The profile is strong, but outcome confidence is kept measured.",
            }
            cautions.append("strong_profile_does_not_guarantee_outcome")

    else:
        outcome_confidence = {
            "code": str(confidence or "unknown").lower(),
            "label": confidence or "Unknown",
            "summary": "Outcome confidence is based on the current matchup lean.",
        }

    confidence_calibration = (
        apply_core_area_durability_confidence_calibration(
            confidence_label=outcome_confidence.get("label"),
            core_gap=core_gap,
        )
    )

    if confidence_calibration["confidence_calibrated"]:
        outcome_confidence = {
            "code": "medium",
            "label": "Medium",
            "summary": (
                "The matchup lean remains usable, but broader Core Area "
                "support is not durable enough for High confidence."
            ),
        }
        cautions.append(INSUFFICIENT_DURABILITY_REASON)

    return {
        "profile_strength": profile_strength,
        "outcome_confidence": outcome_confidence,
        "confidence_calibration": confidence_calibration,
        "display_label": (
            f"{profile_strength['label']} / "
            f"{outcome_confidence['label']} Outcome Confidence"
        ),
        "cautions": cautions,
    }


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _team_label(side: str, header: dict) -> str:
    if side == "away":
        return header["away_team"]["abbreviation"]
    if side == "home":
        return header["home_team"]["abbreviation"]
    return "Neither team"


def classify_percentile_gap(percentile_gap):
    """
    Convert ranking percentile gap into product-safe language.

    This is descriptive, not predictive.
    """

    if percentile_gap is None:
        return "insufficient"

    if percentile_gap < 3.5:
        return "near_even"

    if percentile_gap < 10:
        return "slight_advantage"

    if percentile_gap < 20:
        return "advantage"

    return "clear_advantage"

def is_headline_edge_metric(row: dict) -> bool:
    """
    Decide whether a metric is allowed to become a headline matchup driver.

    This intentionally keeps supporting, workload, volume, and watch metrics out
    of the main summary layer.

    They can still appear as context notes, but they should not steer:
    - metric_highlights
    - category_summaries
    - core_area_summaries
    """

    if not row:
        return False

    return (
        row.get("ranking_usage") == "edge"
        and row.get("ranking_kind") == "edge"
        and row.get("edge_language_allowed") is True
        and row.get("confidence_eligible") is True
        and row.get("signal_strength") == "strong"
        and row.get("data_quality_status") == "good"
    )


def format_advantage_phrase(summary_label: str) -> str:
    """
    Human wording helper.

    Avoids awkward strings like:
    - "has a advantage"

    Prefer:
    - "shows an advantage"
    - "shows a slight advantage"
    - "shows a clear advantage"
    """

    if summary_label == "clear_advantage":
        return "a clear advantage"

    if summary_label == "advantage":
        return "an advantage"

    if summary_label == "slight_advantage":
        return "a slight advantage"

    return "an edge"


def build_non_headline_context_note(comparison: dict, reason: str) -> dict:
    """
    Convert a ranking comparison into a context note when it should not be
    used as a headline edge.

    This keeps the data visible for future UI use without letting it drive
    the main matchup story too hard.
    """

    label = comparison.get("label") or comparison.get("metric")
    ranking_usage = comparison.get("ranking_usage")
    ranking_kind = comparison.get("ranking_kind")

    if ranking_usage == "context_only" or ranking_kind == "context":
        summary_label = "context_only"
        summary = f"{label} is descriptive context, not a better/worse edge."
    else:
        summary_label = "supporting_context"
        summary = f"{label} is supporting context, so it is not used as a headline driver."

    return {
        "metric": comparison.get("metric"),
        "label": label,
        "category": comparison.get("category"),
        "core_area": comparison.get("core_area"),
        "summary_label": summary_label,
        "summary": summary,
        "reason": reason,
        "leader": comparison.get("leader"),
        "leader_team": comparison.get("leader_team"),
        "away": comparison.get("away"),
        "home": comparison.get("home"),
        "percentile_gap": comparison.get("percentile_gap"),
        "rank_gap": comparison.get("rank_gap"),
        "ranking_usage": comparison.get("ranking_usage"),
        "ranking_kind": comparison.get("ranking_kind"),
        "signal_strength": comparison.get("signal_strength"),
        "confidence_eligible": comparison.get("confidence_eligible"),
        "edge_language_allowed": comparison.get("edge_language_allowed"),
        "data_quality_status": comparison.get("data_quality_status"),
    }

def compare_ranking_metric(
    metric_name: str,
    away_row: dict,
    home_row: dict,
    header: dict,
) -> dict:
    away_percentile = _safe_float(away_row.get("league_percentile"))
    home_percentile = _safe_float(home_row.get("league_percentile"))

    away_rank = _safe_int(away_row.get("league_rank"))
    home_rank = _safe_int(home_row.get("league_rank"))

    percentile_gap = None
    if away_percentile is not None and home_percentile is not None:
        percentile_gap = abs(away_percentile - home_percentile)

    rank_gap = None
    if away_rank is not None and home_rank is not None:
        rank_gap = abs(away_rank - home_rank)

    if percentile_gap is None:
        leader = "neutral"
    elif percentile_gap < 3.5:
        leader = "neutral"
    elif away_percentile > home_percentile:
        leader = "away"
    else:
        leader = "home"

    gap_type = classify_percentile_gap(percentile_gap)
    leader_team = _team_label(leader, header)

    label = away_row.get("label") or home_row.get("label") or metric_name
    category = away_row.get("category") or home_row.get("category")
    core_area = away_row.get("core_area") or home_row.get("core_area")

    if leader == "neutral":
        summary = f"{label} is near even."
    else:
        advantage_phrase = format_advantage_phrase(gap_type)
        summary = f"{leader_team} shows {advantage_phrase} in {label}."

    ranking_usage = away_row.get("ranking_usage") or home_row.get("ranking_usage")
    ranking_kind = away_row.get("ranking_kind") or home_row.get("ranking_kind")
    signal_strength = away_row.get("signal_strength") or home_row.get("signal_strength")
    data_quality_status = (
        away_row.get("data_quality_status")
        or home_row.get("data_quality_status")
    )

    edge_language_allowed = _as_bool(
        away_row.get("edge_language_allowed")
        or home_row.get("edge_language_allowed")
    )

    confidence_eligible = _as_bool(
        away_row.get("confidence_eligible")
        or home_row.get("confidence_eligible")
    )

    comparison = {
        "metric": metric_name,
        "label": label,
        "category": category,
        "core_area": core_area,
        "leader": leader,
        "leader_team": leader_team if leader in {"away", "home"} else None,
        "summary_label": gap_type,
        "summary": summary,
        "away": {
            "team": header["away_team"]["abbreviation"],
            "value": away_row.get("value"),
            "league_rank": away_rank,
            "league_percentile": away_percentile,
            "tier": away_row.get("tier"),
            "tier_label": away_row.get("tier_label"),
        },
        "home": {
            "team": header["home_team"]["abbreviation"],
            "value": home_row.get("value"),
            "league_rank": home_rank,
            "league_percentile": home_percentile,
            "tier": home_row.get("tier"),
            "tier_label": home_row.get("tier_label"),
        },
        "percentile_gap": round(percentile_gap, 3) if percentile_gap is not None else None,
        "rank_gap": rank_gap,
        "ranking_usage": ranking_usage,
        "ranking_kind": ranking_kind,
        "signal_strength": signal_strength,
        "edge_language_allowed": edge_language_allowed,
        "confidence_eligible": confidence_eligible,
        "data_quality_status": data_quality_status,
        "data_lag_days": max(
            _safe_int(away_row.get("data_lag_days")) or 0,
            _safe_int(home_row.get("data_lag_days")) or 0,
        ),
    }

    comparison["headline_eligible"] = is_headline_edge_metric(comparison)

    return comparison


def build_group_summary(
    group_name: str,
    rows: list,
    group_type: str,
    header: dict,
) -> dict:
    """
    Summarize headline-eligible metric comparisons into a category or Core Area read.

    Important:
    - This should only receive headline-eligible rows.
    - Supporting/context metrics should be kept in context_notes instead.
    - This summarizes matchup shape. It does not create winner logic.
    """

    away_score = 0
    home_score = 0
    near_even_count = 0
    drivers = []
    cautions = []

    for row in rows or []:
        summary_label = row.get("summary_label")
        leader = row.get("leader")

        if summary_label == "near_even":
            near_even_count += 1
            continue

        weight = 2 if summary_label in {"advantage", "clear_advantage"} else 1

        if leader == "away":
            away_score += weight
            drivers.append(row)
        elif leader == "home":
            home_score += weight
            drivers.append(row)

    if not rows:
        summary_label = "insufficient"
        leader = "neutral"

    elif away_score == 0 and home_score == 0:
        summary_label = "near_even"
        leader = "neutral"

    elif abs(away_score - home_score) <= 1 and away_score > 0 and home_score > 0:
        summary_label = "mixed"
        leader = "neutral"
        cautions.append("group_has_headline_edges_for_both_teams")

    elif away_score > home_score:
        leader = "away"
        summary_label = "advantage" if away_score - home_score >= 2 else "slight_advantage"

    else:
        leader = "home"
        summary_label = "advantage" if home_score - away_score >= 2 else "slight_advantage"

    leader_team = _team_label(leader, header)

    if summary_label == "insufficient":
        summary = f"{group_name} does not have enough headline-eligible ranking data for a clean read."
    elif summary_label == "near_even":
        summary = f"{group_name} looks close to even."
    elif summary_label == "mixed":
        summary = f"{group_name} is mixed, with useful headline signals on both sides."
    elif summary_label == "clear_advantage":
        summary = f"{group_name} clearly leans toward {leader_team}."
    elif summary_label == "advantage":
        summary = f"{group_name} leans toward {leader_team}."
    elif summary_label == "slight_advantage":
        summary = f"{group_name} slightly leans toward {leader_team}."
    else:
        summary = f"{group_name} has a matchup read for {leader_team}."

    top_drivers = sorted(
        drivers,
        key=lambda row: row.get("percentile_gap") or 0,
        reverse=True,
    )[:3]

    return {
        "type": group_type,
        "name": group_name,
        "leader": leader,
        "leader_team": leader_team if leader in {"away", "home"} else None,
        "summary_label": summary_label,
        "summary": summary,
        "away_score": away_score,
        "home_score": home_score,
        "near_even_metric_count": near_even_count,
        "metric_count": len(rows or []),
        "drivers": [
            {
                "metric": row.get("metric"),
                "label": row.get("label"),
                "leader": row.get("leader"),
                "leader_team": row.get("leader_team"),
                "summary_label": row.get("summary_label"),
                "percentile_gap": row.get("percentile_gap"),
                "summary": row.get("summary"),
            }
            for row in top_drivers
        ],
        "cautions": cautions,
    }


def build_matchup_breakdown(
    away_rankings: dict = None,
    home_rankings: dict = None,
    header: dict = None,
    max_metric_highlights: int = 8,
    max_context_notes: int = 12,
) -> dict:
    """
    Build ranking-based matchup summaries.

    Output levels:
    - metric_highlights
    - category_summaries
    - core_area_summaries
    - context_notes

    This is descriptive. It should not force winner/confidence decisions.

    Important:
    - Only headline-eligible metrics drive metric/category/core-area summaries.
    - Supporting/context/workload metrics are kept in context_notes.
    """

    away_rankings = away_rankings or {}
    home_rankings = home_rankings or {}
    header = header or {}

    common_metrics = sorted(set(away_rankings.keys()) & set(home_rankings.keys()))

    if not common_metrics:
        return {
            "available": False,
            "reason": "ranking_context_unavailable",
            "metric_highlights": [],
            "category_summaries": [],
            "core_area_summaries": [],
            "context_notes": [],
            "freshness": {},
            "summary_counts": {
                "common_metric_count": 0,
                "headline_metric_count": 0,
                "context_note_count": 0,
            },
        }

    headline_rows = []
    context_notes = []

    max_lag = 0
    as_of_dates = set()
    source_data_dates = set()
    window_types = set()

    for metric in common_metrics:
        away_row = away_rankings.get(metric) or {}
        home_row = home_rankings.get(metric) or {}

        ranking_usage = away_row.get("ranking_usage") or home_row.get("ranking_usage")
        ranking_kind = away_row.get("ranking_kind") or home_row.get("ranking_kind")
        data_quality_status = (
            away_row.get("data_quality_status")
            or home_row.get("data_quality_status")
        )

        if data_quality_status == "exclude" or ranking_usage == "exclude":
            continue

        for row in [away_row, home_row]:
            if row.get("as_of_date"):
                as_of_dates.add(str(row.get("as_of_date")))
            if row.get("source_data_date"):
                source_data_dates.add(str(row.get("source_data_date")))
            if row.get("window_type"):
                window_types.add(str(row.get("window_type")))

            lag = _safe_int(row.get("data_lag_days"))
            if lag is not None:
                max_lag = max(max_lag, lag)

        comparison = compare_ranking_metric(
            metric_name=metric,
            away_row=away_row,
            home_row=home_row,
            header=header,
        )

        if comparison.get("headline_eligible"):
            headline_rows.append(comparison)
            continue

        if ranking_usage == "context_only" or ranking_kind == "context":
            reason = "context_only_metric"
        elif data_quality_status != "good":
            reason = f"data_quality_{data_quality_status or 'unknown'}"
        elif comparison.get("confidence_eligible") is not True:
            reason = "not_confidence_eligible"
        elif comparison.get("signal_strength") != "strong":
            reason = "not_strong_signal"
        elif comparison.get("edge_language_allowed") is not True:
            reason = "edge_language_not_allowed"
        else:
            reason = "not_headline_eligible"

        context_notes.append(
            build_non_headline_context_note(
                comparison=comparison,
                reason=reason,
            )
        )

    metric_highlights = sorted(
        headline_rows,
        key=lambda row: (
            0 if row.get("summary_label") == "near_even" else 1,
            row.get("percentile_gap") or 0,
        ),
        reverse=True,
    )[:max_metric_highlights]

    by_category = {}
    by_core_area = {}

    for row in headline_rows:
        category = row.get("category")
        core_area = row.get("core_area")

        if category:
            by_category.setdefault(category, []).append(row)

        if core_area:
            by_core_area.setdefault(core_area, []).append(row)

    category_summaries = [
        build_group_summary(
            group_name=category,
            rows=rows,
            group_type="category",
            header=header,
        )
        for category, rows in sorted(by_category.items())
    ]

    core_area_summaries = [
        build_group_summary(
            group_name=core_area,
            rows=rows,
            group_type="core_area",
            header=header,
        )
        for core_area, rows in sorted(by_core_area.items())
    ]

    sorted_context_notes = sorted(
        context_notes,
        key=lambda row: row.get("percentile_gap") or 0,
        reverse=True,
    )

    return {
        "available": True,
        "metric_highlights": metric_highlights,
        "category_summaries": category_summaries,
        "core_area_summaries": core_area_summaries,
        "context_notes": sorted_context_notes[:max_context_notes],
        "freshness": {
            "as_of_dates": sorted(as_of_dates),
            "source_data_dates": sorted(source_data_dates),
            "window_types": sorted(window_types),
            "max_data_lag_days": max_lag,
            "data_lag_note": (
                "Data lag is freshness context only; it is not an automatic confidence penalty."
            ),
        },
        "summary_counts": {
            "common_metric_count": len(common_metrics),
            "headline_metric_count": len(headline_rows),
            "context_note_count": len(context_notes),
            "category_summary_count": len(category_summaries),
            "core_area_summary_count": len(core_area_summaries),
        },
    }



def _core_area_display_strength(away_score, home_score, leader: str) -> str:
    """
    Convert broad Core Area score gap into user-facing display strength.

    This uses core_area_comparison, not headline-driver summary scores.
    """
    if leader not in {"away", "home"}:
        return "near_even"

    away_num = _safe_float(away_score)
    home_num = _safe_float(home_score)

    if away_num is None or home_num is None:
        return "near_even"

    gap = abs(away_num - home_num)

    if gap < 0.08:
        return "near_even"
    if gap < 0.18:
        return "lean"
    if gap < 0.30:
        return "edge"

    return "strong_edge"


def _core_area_summary_label_from_display_strength(display_strength: str) -> str:
    """
    Keep summary_label compatible with the existing API language buckets.
    """
    if display_strength == "near_even":
        return "near_even"
    if display_strength == "lean":
        return "slight_advantage"
    if display_strength == "edge":
        return "advantage"
    if display_strength == "strong_edge":
        return "clear_advantage"

    return "advantage"


def _core_area_driver_alignment(
    *,
    display_leader: str,
    headline_driver_leader: str,
    headline_driver_summary_label: str,
) -> str:
    """
    Describe whether headline-driver summaries support the broad Core Area read.

    This is diagnostic/context only. It should not become a second visible leader.
    """
    if not headline_driver_leader:
        return "no_headline_drivers"

    if display_leader not in {"away", "home"}:
        if headline_driver_leader in {"away", "home"}:
            return "broad_neutral_driver_directional"
        return "aligned_neutral"

    if headline_driver_leader == display_leader:
        return "aligned"

    if headline_driver_leader == "neutral":
        if headline_driver_summary_label == "mixed":
            return "mixed"
        return "thin_or_neutral"

    return "conflicting"


def _core_area_display_summary(
    *,
    core_area: str,
    display_leader: str,
    display_leader_team: str,
    display_strength: str,
    driver_alignment: str,
) -> str:
    """
    Build the user-facing Core Area summary.

    The visible direction comes from core_area_comparison.
    Headline-driver disagreement is described as context, not as a competing verdict.
    """
    if display_strength == "near_even" or display_leader not in {"away", "home"}:
        base = f"{core_area} looks close to even overall."
    elif display_strength == "lean":
        base = f"{display_leader_team} has a broad lean in {core_area}."
    elif display_strength == "edge":
        base = f"{display_leader_team} has a broad edge in {core_area}."
    elif display_strength == "strong_edge":
        base = f"{display_leader_team} has a strong broad edge in {core_area}."
    else:
        base = f"{display_leader_team} has a broad read in {core_area}."

    if driver_alignment == "aligned":
        return f"{base} Headline drivers generally support that read."

    if driver_alignment in {"thin_or_neutral", "no_headline_drivers", "aligned_neutral"}:
        return f"{base} Headline-driver support is thin or close to even."

    if driver_alignment == "mixed":
        return f"{base} Headline drivers are mixed, so treat this as a nuanced read."

    if driver_alignment == "conflicting":
        return f"{base} Headline drivers point differently, so treat this as a nuanced read."

    if driver_alignment == "broad_neutral_driver_directional":
        return (
            f"{base} A few headline drivers point one way, but the broad Core Area read "
            "does not create a clean visible direction."
        )

    return base


def align_core_area_summaries_to_core_area_comparison(
    *,
    matchup_breakdown: dict,
    core_area_comparison: list,
    header: dict,
) -> dict:
    """
    Align user-facing Core Area summaries to core_area_comparison.

    Product rule:
    - core_area_comparison owns the visible Core Area leader.
    - core_area_summaries explain headline-driver support quality.
    - headline-driver leader is preserved as diagnostic metadata, not as a second
      visible directional verdict.
    """
    breakdown = dict(matchup_breakdown or {})

    if not breakdown.get("available"):
        return breakdown

    comparison_by_core_area = {
        row.get("core_area"): row
        for row in core_area_comparison or []
        if row.get("core_area")
    }

    aligned_summaries = []

    for summary in breakdown.get("core_area_summaries") or []:
        core_area = summary.get("name")
        broad_row = comparison_by_core_area.get(core_area)

        if not broad_row:
            aligned_summaries.append(summary)
            continue

        display_leader = broad_row.get("leader") or "neutral"
        display_leader_team = (
            _team_label(display_leader, header)
            if display_leader in {"away", "home"}
            else None
        )

        broad_away_score = broad_row.get("away_score")
        broad_home_score = broad_row.get("home_score")

        away_num = _safe_float(broad_away_score)
        home_num = _safe_float(broad_home_score)
        broad_score_gap = (
            round(abs(away_num - home_num), 3)
            if away_num is not None and home_num is not None
            else None
        )

        display_strength = _core_area_display_strength(
            away_score=broad_away_score,
            home_score=broad_home_score,
            leader=display_leader,
        )

        headline_driver_leader = summary.get("leader")
        headline_driver_summary_label = summary.get("summary_label")

        driver_alignment = _core_area_driver_alignment(
            display_leader=display_leader,
            headline_driver_leader=headline_driver_leader,
            headline_driver_summary_label=headline_driver_summary_label,
        )

        display_summary = _core_area_display_summary(
            core_area=core_area,
            display_leader=display_leader,
            display_leader_team=display_leader_team,
            display_strength=display_strength,
            driver_alignment=driver_alignment,
        )

        updated_summary = dict(summary)

        updated_summary.update({
            # User-facing direction now comes from core_area_comparison.
            "leader": display_leader,
            "leader_team": display_leader_team,
            "leader_source": "core_area_comparison",

            # User-facing display language.
            "display_strength": display_strength,
            "display_summary": display_summary,
            "summary_label": _core_area_summary_label_from_display_strength(
                display_strength
            ),
            "summary": display_summary,

            # Broad Core Area diagnostic metadata.
            "broad_away_score": broad_away_score,
            "broad_home_score": broad_home_score,
            "broad_score_gap": broad_score_gap,

            # Preserve prior headline-driver result as diagnostic context.
            "headline_driver_leader": headline_driver_leader,
            "headline_driver_leader_team": summary.get("leader_team"),
            "headline_driver_summary_label": headline_driver_summary_label,
            "headline_driver_summary": summary.get("summary"),
            "headline_driver_away_score": summary.get("away_score"),
            "headline_driver_home_score": summary.get("home_score"),
            "driver_alignment": driver_alignment,
        })

        aligned_summaries.append(updated_summary)

    breakdown["core_area_summaries"] = aligned_summaries

    return breakdown

def get_ranking_context_for_game_safe(game_id: str) -> tuple:
    """
    Fetch ranking context from queries.game_queries without making game_service.py
    brittle to one exact function name.

    Expected preferred shape from the ranking fetcher:

    (
        ranking_context,
        away_rankings,
        home_rankings
    )

    ranking_context should include:
    {
        "available": true,
        ...
    }

    away_rankings/home_rankings should be dicts keyed by metric name.
    """

    default_context = {
        "available": False,
        "reason": "ranking_fetch_function_missing_or_failed",
        "game_id": game_id,
    }

    try:
        from queries import game_queries
    except Exception as exc:
        default_context["reason"] = "game_queries_import_failed"
        default_context["error"] = str(exc)[:200]
        return default_context, {}, {}

    possible_function_names = [
        "get_ranking_context_for_game",
        "get_team_metric_rankings_for_game",
        "get_metric_rankings_for_game",
        "get_game_metric_rankings",
        "get_game_rankings",
        "get_team_rankings_for_game",
    ]

    for function_name in possible_function_names:
        fetcher = getattr(game_queries, function_name, None)

        if not callable(fetcher):
            continue

        try:
            result = fetcher(game_id)
        except Exception:
            continue

        if isinstance(result, tuple) and len(result) == 3:
            first, second, third = result

            if isinstance(first, dict) and "available" in first:
                return first, second or {}, third or {}

            if isinstance(third, dict) and "available" in third:
                return third, first or {}, second or {}

        if isinstance(result, dict):
            ranking_context = result.get("ranking_context") or result.get("context")

            if ranking_context:
                return (
                    ranking_context,
                    result.get("away_rankings") or {},
                    result.get("home_rankings") or {},
                )

            if "available" in result and result.get("away_rankings") is not None:
                return (
                    result,
                    result.get("away_rankings") or {},
                    result.get("home_rankings") or {},
                )

    return default_context, {}, {}

def build_matchup_lean(
    game_profile: list,
    team_comparison: list,
    header: dict,
    core_area_comparison: list = None,
):
    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    score = {away: 0, home: 0}

    team_edge_context = build_team_comparison_edge_context(
        team_comparison=team_comparison or []
    )

    confidence_guardrails = {
        "applied": False,
        "capped_from": None,
        "capped_to": None,
        "reasons": [],
        "team_comparison_edge_context": team_edge_context,
    }

    # --------------------
    # Team comparison scoring
    # --------------------
    for metric in team_comparison or []:
        better = metric.get("better")

        if better == "away":
            score[away] += 1
        elif better == "home":
            score[home] += 1

    # --------------------
    # Game profile signal scoring
    # --------------------
    for signal in game_profile or []:
        level = signal.get("level", "Neutral")
        weight = 2 if level == "Elevated" else 1 if level == "Moderate" else 0

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

        confidence = "Low"

        profile_labels = build_profile_strength_labels(
            target=None,
            confidence=confidence,
            profile_type="no_clear_edge",
            core_area_context=core_area_context,
            team_edge_context=team_edge_context,
            signal_score=signal_score,
        )

        outcome_confidence = profile_labels["outcome_confidence"]

        return {
            "target_team": "None",
            "target_side": None,
            "lean_summary": "No clear matchup edge",
            "focus_summary": "The available signals are too close to call this a clean lean",
            "confidence": confidence,
            "raw_signal_confidence": confidence,
            "confidence_role": "legacy_raw_signal_confidence",
            "user_facing_confidence": {
                "label": outcome_confidence.get("label"),
                "source": "outcome_confidence",
            },
            "confidence_context": "The signal gap is small, so this stays in cautious territory",
            "profile_type": "no_clear_edge",
            "signal_score": signal_score,
            "core_area_context": core_area_context,
            "confidence_guardrails": confidence_guardrails,
            "profile_strength": profile_labels["profile_strength"],
            "outcome_confidence": outcome_confidence,
            "confidence_calibration": profile_labels["confidence_calibration"],
            "matchup_label": profile_labels["display_label"],
            "matchup_cautions": profile_labels["cautions"],
        }

    target = away if score[away] > score[home] else home
    target_side = "away" if target == away else "home"

    confidence = "High" if diff >= 5 else "Medium"

    week_num = get_week_number(header)
    if week_num and week_num <= 2:
        original_confidence = confidence
        confidence = "Low"

        confidence_guardrails["applied"] = True
        confidence_guardrails["capped_from"] = original_confidence
        confidence_guardrails["capped_to"] = "Low"
        confidence_guardrails["reasons"].append("early_season_week_1_or_2")

    core_area_context = build_core_area_context(
        core_area_comparison=core_area_comparison or [],
        lean_side=target_side,
    )

    profile_type = core_area_context.get("profile_type")

    # --------------------
    # Core Area sanity check
    # --------------------
    if profile_type == "confirmed_edge":
        lean_summary = f"{target} has the stronger overall matchup profile"
        focus_summary = (
            f"{target} is supported by both the signal score and the broader Core Area read"
        )
        confidence_context = "The broader matchup profile lines up with the signal lean"

    elif profile_type == "coin_flip_profile":
        lean_summary = f"{target} has a small lean, but the matchup is pretty tight overall"
        focus_summary = (
            f"{target} has the stronger signal score, but the Core Areas are close enough to keep this cautious"
        )

        if confidence != "Low":
            confidence_guardrails["applied"] = True
            confidence_guardrails["capped_from"] = confidence
            confidence_guardrails["capped_to"] = "Low"
            confidence_guardrails["reasons"].append("coin_flip_core_area_profile")

        confidence = "Low"
        confidence_context = "The Core Area gap is small, so this is treated as a cautious lean"

    elif profile_type == "split_profile":
        lean_summary = f"{target} gets the lean, though the matchup is not clean"
        focus_summary = (
            f"{target} has signal support, but the Core Areas are split across both teams"
        )

        if confidence != "Low":
            confidence_guardrails["applied"] = True
            confidence_guardrails["capped_from"] = confidence
            confidence_guardrails["capped_to"] = "Low"
            confidence_guardrails["reasons"].append("split_core_area_profile")

        confidence = "Low"
        confidence_context = "The profile is split, so confidence stays low"

    elif profile_type == "conflicting_profile":
        lean_summary = (
            f"{target} has signal support, but the broader matchup does not fully back it up"
        )
        focus_summary = (
            f"{target} leads the signal score, but the Core Area read is pushing back"
        )

        if confidence != "Low":
            confidence_guardrails["applied"] = True
            confidence_guardrails["capped_from"] = confidence
            confidence_guardrails["capped_to"] = "Low"
            confidence_guardrails["reasons"].append("conflicting_core_area_profile")

        confidence = "Low"
        confidence_context = "The broader profile does not fully agree with the signal lean"

    else:
        lean_summary = f"The signals lean toward {target}, but the matchup is not fully settled"
        focus_summary = (
            f"{target} has a directional lean, but the supporting context is still mixed"
        )
        confidence_context = "The matchup has some signal support, but the full profile is not clean"

    # --------------------
    # Guardrail:
    # Do not allow High confidence when visible Team Comparison is weak.
    # --------------------
    if (
        confidence == "High"
        and team_edge_context.get("edge_strength") in {"low", "none"}
    ):
        confidence_guardrails["applied"] = True
        confidence_guardrails["capped_from"] = "High"
        confidence_guardrails["capped_to"] = "Medium"
        confidence_guardrails["reasons"].append("low_visible_team_comparison_edge")

        confidence = "Medium"
        confidence_context = (
            f"{confidence_context}; the visible Team Comparison edge is limited, "
            "so this is capped at Medium instead of High"
        )

    profile_labels = build_profile_strength_labels(
        target=target,
        confidence=confidence,
        profile_type=profile_type,
        core_area_context=core_area_context,
        team_edge_context=team_edge_context,
        signal_score=signal_score,
    )

    outcome_confidence = profile_labels["outcome_confidence"]

    return {
        "target_team": f"{target} edge",
        "target_side": target_side,
        "lean_summary": lean_summary,
        "focus_summary": focus_summary,
        "confidence": confidence,
        "raw_signal_confidence": confidence,
        "confidence_role": "legacy_raw_signal_confidence",
        "user_facing_confidence": {
            "label": outcome_confidence.get("label"),
            "source": "outcome_confidence",
        },
        "confidence_context": confidence_context,
        "profile_type": profile_type,
        "signal_score": signal_score,
        "core_area_context": core_area_context,
        "confidence_guardrails": confidence_guardrails,
        "profile_strength": profile_labels["profile_strength"],
        "outcome_confidence": outcome_confidence,
        "confidence_calibration": profile_labels["confidence_calibration"],
        "matchup_label": profile_labels["display_label"],
        "matchup_cautions": profile_labels["cautions"],
    }


def build_model_outcome(matchup_lean: dict, final_score: dict, header: dict):
    if not final_score:
        return None

    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    predicted = matchup_lean.get("target_team", "").replace(" edge", "").strip()

    away_total = final_score["away"]["total"]
    home_total = final_score["home"]["total"]

    if away_total is None or home_total is None:
        return None

    is_tie = away_total == home_total

    if is_tie:
        actual_winner = "TIE"
    elif away_total > home_total:
        actual_winner = away
    else:
        actual_winner = home

    if is_tie:
        result = "No Decision"
    elif predicted not in [away, home]:
        result = "No Pick"
    elif predicted == actual_winner:
        result = "Correct"
    else:
        result = "Incorrect"

    return {
        "result": result,
        "actual_winner": actual_winner,
        "predicted_team": predicted if predicted in [away, home] else None,
        "is_tie": is_tie,
        "outcome_note": (
            "Final score was tied, so model accuracy is not graded."
            if is_tie
            else None
        ),
    }

# =========================
# BIGQUERY SAVE LOGIC
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
    elif result_lower in {"tie", "push", "no decision", "no_decision"}:
        result_code = "tie"
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



def build_unavailable_game_details_response(reason: str) -> dict:
    """
    Build a stable empty /game response when the requested game cannot be loaded.

    Keeping this in one helper avoids having a large inline return object inside
    get_game_details().
    """

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
        "ranking_context": {
            "available": False,
            "reason": reason,
        },
        "claim_language_context": {
            "available": False,
            "reason": reason,
            "scope": "claim_language_support",
            "feature_versions": {
                "two_way_context": "two_way_context_v1",
                "offensive_efficiency_support": "offensive_efficiency_support_v1",
            },
            "two_way_context_by_side": {},
            "offensive_efficiency_support_by_side": {},
        },
        "matchup_breakdown": {
            "available": False,
            "reason": reason,
            "metric_highlights": [],
            "category_summaries": [],
            "core_area_summaries": [],
            "context_notes": [],
            "freshness": {},
        },
    }


def build_claim_language_context(
    *,
    core_area_comparison: list,
    team_comparison: list,
    away_rankings: dict = None,
    home_rankings: dict = None,
    matchup_breakdown: dict = None,
) -> dict:
    """
    Build runtime claim-language context for /game.

    Important:
    - This is claim-language support only.
    - This does not affect winner pick logic.
    - This does not affect outcome confidence.
    - This does not change model_trust.
    """

    two_way_context_by_side = build_runtime_two_way_context_by_side(
        core_area_comparison=core_area_comparison,
        team_comparison=team_comparison,
    )

    offensive_efficiency_support_by_side = build_runtime_offensive_efficiency_support_by_side(
        away_rankings=away_rankings or {},
        home_rankings=home_rankings or {},
        team_comparison=team_comparison,
        matchup_breakdown=matchup_breakdown or {},
    )

    return {
        "available": True,
        "scope": "claim_language_support",
        "feature_versions": {
            "two_way_context": "two_way_context_v1",
            "offensive_efficiency_support": "offensive_efficiency_support_v1",
        },
        "two_way_context_by_side": two_way_context_by_side,
        "offensive_efficiency_support_by_side": offensive_efficiency_support_by_side,
    }

def load_game_details_evidence(
    game_id: str,
    *,
    include_final_score: bool = True,
) -> GameDetailsEvidence:
    """Load one request's evidence without constructing product sections."""
    header = get_game_header(game_id)
    if not header:
        return GameDetailsEvidence(
            header={},
            away_metrics={},
            home_metrics={},
            ranking_context=build_unavailable_ranking_context(
                reason="missing_game_header",
                game_id=game_id,
            ),
            away_rankings={},
            home_rankings={},
            final_score=None,
        )

    away_metrics, home_metrics = get_team_metrics(game_id)
    final_score = get_final_score(game_id) if include_final_score else None
    ranking_context, away_rankings, home_rankings = (
        get_ranking_context_for_game_safe(game_id=game_id)
    )

    return GameDetailsEvidence(
        header=header,
        away_metrics=away_metrics,
        home_metrics=home_metrics,
        ranking_context=ranking_context,
        away_rankings=away_rankings,
        home_rankings=home_rankings,
        final_score=final_score,
    )


# =========================
# MAIN RESPONSE BUILDER
# =========================

def build_game_details_from_evidence(
    evidence: GameDetailsEvidence,
    *,
    pregame_only: bool = False,
) -> dict:
    """
    Build the full /game/<game_id> API response.

    Backend owns all matchup/model logic.
    Frontend should render the structured response directly.

    This version includes:
    - ranking_context
    - matchup_breakdown
    - profile_strength / outcome_confidence inside matchup_lean
    - claim_language_context for runtime claim-language support
    - language_support annotations on response-only sections

    Important:
    language_support annotations are applied after model logic is built.
    They do not change matchup_lean, model_outcome, or model_trust.
    """

    header = evidence.header

    if not header:
        return build_unavailable_game_details_response(
            reason="missing_game_header"
        )

    away_metrics = evidence.away_metrics
    home_metrics = evidence.home_metrics
    ranking_context = evidence.ranking_context
    away_rankings = evidence.away_rankings
    home_rankings = evidence.home_rankings
    final_score = None if pregame_only else evidence.final_score

    # Support both old and new build_team_comparison signatures.
    # Newer version can accept away_rankings/home_rankings for near-even handling.
    try:
        team_comparison = build_team_comparison(
            away_metrics=away_metrics,
            home_metrics=home_metrics,
            away_rankings=away_rankings if ranking_context.get("available") else {},
            home_rankings=home_rankings if ranking_context.get("available") else {},
        )
    except TypeError:
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

    matchup_breakdown = build_matchup_breakdown(
        away_rankings=away_rankings if ranking_context.get("available") else {},
        home_rankings=home_rankings if ranking_context.get("available") else {},
        header=header,
    )

    matchup_breakdown = align_core_area_summaries_to_core_area_comparison(
        matchup_breakdown=matchup_breakdown,
        core_area_comparison=core_area_comparison,
        header=header,
    )

    claim_language_context = build_claim_language_context(
        core_area_comparison=core_area_comparison,
        team_comparison=team_comparison,
        away_rankings=away_rankings if ranking_context.get("available") else {},
        home_rankings=home_rankings if ranking_context.get("available") else {},
        matchup_breakdown=matchup_breakdown,
    )

    # Response-only annotation.
    # This does not feed back into matchup_lean/model_outcome/model_trust.
    annotated_response_sections = apply_claim_language_support_to_response_sections(
        team_comparison=team_comparison,
        matchup_breakdown=matchup_breakdown,
        claim_language_context=claim_language_context,
    )

    response_team_comparison = annotated_response_sections["team_comparison"]
    response_matchup_breakdown = annotated_response_sections["matchup_breakdown"]

    game_status = str(header.get("game_status") or "").lower()

    if not pregame_only and game_status in {"final", "final/ot"}:
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
        "team_comparison": response_team_comparison,
        "core_area_comparison": core_area_comparison,
        "ranking_context": ranking_context,
        "claim_language_context": claim_language_context,
        "matchup_breakdown": response_matchup_breakdown,
    }


def get_pregame_game_details(
    game_id: str,
    *,
    evidence: Optional[GameDetailsEvidence] = None,
) -> dict:
    """Build a fail-closed pregame response with no score query or write."""
    pregame_evidence = evidence or load_game_details_evidence(
        game_id,
        include_final_score=False,
    )
    payload = build_game_details_from_evidence(
        pregame_evidence,
        pregame_only=True,
    )
    require_pregame_payload(payload)
    return payload


def get_pregame_game_contract(
    game_id: str,
    *,
    learning_run_id: str,
    scheduled_kickoff: datetime,
    evidence: Optional[GameDetailsEvidence] = None,
) -> dict:
    """Construct and identify one pregame response without persistence."""
    payload = get_pregame_game_details(game_id, evidence=evidence)
    identity = identify_pregame_payload(
        payload=payload,
        learning_run_id=learning_run_id,
        game_id=game_id,
        scheduled_kickoff=scheduled_kickoff,
    )
    return {**identity, "payload": payload}


def get_game_details(game_id: str) -> dict:
    """Preserve the live route through the shared response builder."""
    evidence = load_game_details_evidence(game_id, include_final_score=True)
    return build_game_details_from_evidence(evidence, pregame_only=False)
