from datetime import datetime, timezone

from queries.admin_claim_health_queries import (
    get_baseline,
    get_category_matrix,
    get_confidence_core_area_matrix,
    get_core_area_matrix,
    get_coverage_summary,
    get_feature_scorecard,
    get_surface_matrix,
)
from typing import Optional

def _build_coverage_note(coverage: dict) -> Optional[str]:
    games_without_claims = coverage.get("games_without_claims") or 0

    if games_without_claims <= 0:
        return None

    return (
        "Some expected games did not produce claim rows. "
        "For the 2025 full-slate pilot, this matched Week 1 games where "
        "pregame ranking/window context was unavailable."
    )

def _build_section_metadata() -> dict:
    return {
        "core_area_matrix": {
            "title": "Core Area Health",
            "description": "Claim validation by broad football area.",
            "chart_type": "bar",
            "primary_metric": "validation_rate",
        },
        "category_matrix": {
            "title": "Category Health",
            "description": "Claim validation by football category within each core area.",
            "chart_type": "grouped_table",
            "primary_metric": "validation_rate",
        },
        "confidence_core_area_matrix": {
            "title": "Confidence by Core Area",
            "description": "Claim validation by confidence label and football area.",
            "chart_type": "grouped_bar",
            "primary_metric": "validation_rate",
        },
        "feature_scorecard": {
            "title": "Offensive Efficiency Feature Scorecard",
            "description": "Claim validation by offensive efficiency support bucket.",
            "chart_type": "bar",
            "primary_metric": "validation_rate",
        },
        "surface_matrix": {
            "title": "Claim Surface Health",
            "description": "Claim validation by claim type and claim layer.",
            "chart_type": "table",
            "primary_metric": "validation_rate",
        },
    }

def build_claim_health_response(run_id: str, season: str = "2025") -> dict:
    """
    Builds the aggregate admin claim-health response.

    This is intentionally aggregate-level only.
    It does not return game-level drilldown rows.
    """

    coverage = get_coverage_summary(run_id=run_id, season=season)
    baseline = get_baseline(run_id=run_id)

    return {
        "available": True,
        "scope": "aggregate_claim_health",
        "run_id": run_id,
        "season": season,
        "generated_at": datetime.now(timezone.utc).isoformat(),

        "coverage": {
            **coverage,
            "context_note": _build_coverage_note(coverage),
        },

        "baseline": baseline,
        
        "section_metadata": _build_section_metadata(),

        "sections": {
            "core_area_matrix": get_core_area_matrix(run_id=run_id),
            "category_matrix": get_category_matrix(run_id=run_id),
            "confidence_core_area_matrix": get_confidence_core_area_matrix(run_id=run_id),
            "feature_scorecard": get_feature_scorecard(run_id=run_id),
            "surface_matrix": get_surface_matrix(run_id=run_id),
        },
    }