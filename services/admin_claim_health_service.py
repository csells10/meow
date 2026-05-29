from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from queries.admin_claim_health_queries import (
    get_baseline,
    get_calibration_over_time,
    get_category_matrix,
    get_confidence_core_area_matrix,
    get_core_area_alignment_matrix,
    get_core_area_matrix,
    get_coverage_summary,
    get_feature_health_matrix,
    get_feature_scorecard,
    get_game_level_calibration,
    get_calibrated_game_level_calibration,
    get_calibrated_core_area_alignment_matrix,  
    get_pillar_health_matrix,
    get_pillar_weekly_health,
    get_surface_matrix,
)


def _build_coverage_note(coverage: dict) -> Optional[str]:
    games_without_claims = coverage.get("games_without_claims") or 0

    if games_without_claims <= 0:
        return None

    return (
        "Some expected games did not produce claim rows. "
        "For the 2025 full-slate pilot, this matched Week 1 games where "
        "pregame ranking/window context was unavailable."
    )


def _build_tabs() -> list[dict]:
    return [
        {
            "id": "overview",
            "label": "Overview",
            "description": (
                "Top-level read of claim coverage, baseline validation, and the "
                "main calibration trend needed before opening deeper tabs."
            ),
            "sections": [
                "coverage",
                "baseline",
                "calibration_over_time",
                "game_level_calibration",
            ],
        },
        {
            "id": "game_calibration",
            "label": "Game Calibration",
            "description": (
                "Game-level outcome calibration by profile strength and outcome "
                "confidence. This checks whether game reads were directionally aligned."
            ),
            "sections": [
                "game_level_calibration",
                "calibrated_game_level_calibration",
                "calibration_over_time",
            ],
        },
        {
            "id": "core_area_alignment",
            "label": "Core Area Alignment",
            "description": (
                "How matchup profile type and Core Area alignment relate to final "
                "game outcomes and claim validation."
            ),
            "sections": [
                "core_area_alignment_matrix",
                "calibrated_core_area_alignment_matrix",
                "core_area_matrix",
                "confidence_core_area_matrix",
            ],
        },
        {
            "id": "pillar_health",
            "label": "Pillar Health",
            "description": (
                "Claim health by football pillar, category, and week so strong and "
                "weak parts of the explanation layer are easier to spot."
            ),
            "sections": [
                "pillar_health_matrix",
                "pillar_weekly_health",
                "category_matrix",
            ],
        },
        {
            "id": "feature_health",
            "label": "Feature Health",
            "description": (
                "Calibration health for engineered features and metadata features. "
                "This is for language/support calibration, not winner logic."
            ),
            "sections": [
                "feature_health_matrix",
                "feature_scorecard",
            ],
        },
        {
            "id": "technical_debug",
            "label": "Technical Debug",
            "description": (
                "Lower-level claim surface and compatibility matrices retained for "
                "QA, regression checks, and future debugging."
            ),
            "sections": [
                "surface_matrix",
            ],
        },
    ]


def _build_formula_notes() -> dict:
    return {
        "claim_validation_rate": {
            "formula": "validated_claims / eligible_claim_rows",
            "eligible_claim_rows": (
                "Claim rows where validation_result is present and not unavailable. "
                "actual_neutral_or_mixed remains eligible because it is useful claim-quality feedback."
            ),
            "meaning": (
                "How often GameLens claim language was supported by postgame metric validation. "
                "This is claim-quality calibration, not direct winner accuracy."
            ),
        },
        "game_pick_correct_rate": {
            "formula": "correct_picks / (correct_picks + incorrect_picks)",
            "meaning": (
                "How often games with an actual directional pick aligned with the final result. "
                "No-pick games are tracked separately and excluded from this denominator."
            ),
        },
        "selected_segment_validation_rate": {
            "formula": "selected_segment_validated_claims / selected_segment_eligible_claim_rows",
            "default_segment": "all_claims_in_period",
            "meaning": (
                "The same claim-validation formula applied to whatever segment a tab or chart selects. "
                "For now the default segment is all claims in the returned period."
            ),
        },
    }


def _build_season_phase_groups() -> dict:
    return {
        "early_season": {
            "label": "Early Season",
            "description": "Weeks 1–5",
        },
        "mid_season": {
            "label": "Mid Season",
            "description": "Weeks 6–12",
        },
        "late_season": {
            "label": "Late Season",
            "description": "Weeks 13–18",
        },
        "postseason": {
            "label": "Postseason",
            "description": "Wild Card through Super Bowl",
        },
    }


def _build_section_metadata() -> dict:
    return {
        # New tab-ready sections
        "calibration_over_time": {
            "title": "Calibration Over Time",
            "description": (
                "Weekly aggregate view of claim validation and game-pick calibration. "
                "Default grain is week; the backend query also supports day and season_phase."
            ),
            "tab_id": "overview",
            "chart_type": "line",
            "primary_metric": "claim_validation_rate",
            "grain_default": "week",
            "supported_grains": ["day", "week", "season_phase"],
        },
        "game_level_calibration": {
            "title": "Game-Level Calibration",
            "description": (
                "Game outcome calibration by profile strength and outcome confidence. "
                "Useful for checking Low/Medium/High confidence behavior."
            ),
            "tab_id": "game_calibration",
            "chart_type": "matrix",
            "primary_metric": "correct_rate",
        },
        "core_area_alignment_matrix": {
            "title": "Matchup Lean × Core Area Alignment",
            "description": (
                "Game-level calibration grouped by profile type and outcome confidence, "
                "with average core/signal gaps for context."
            ),
            "tab_id": "core_area_alignment",
            "chart_type": "matrix",
            "primary_metric": "correct_rate",
        },
        "pillar_health_matrix": {
            "title": "Pillar Health Matrix",
            "description": "Claim validation by canonical Core Area and category.",
            "tab_id": "pillar_health",
            "chart_type": "grouped_table",
            "primary_metric": "validation_rate",
        },
        "pillar_weekly_health": {
            "title": "Pillar Weekly Health",
            "description": "Weekly claim and game calibration summary with strongest and weakest Core Areas.",
            "tab_id": "pillar_health",
            "chart_type": "table",
            "primary_metric": "claim_validation_rate",
        },
        "feature_health_matrix": {
            "title": "Feature Health Matrix",
            "description": (
                "Broad feature calibration matrix covering football calibration features "
                "and data quality / metadata features."
            ),
            "tab_id": "feature_health",
            "chart_type": "grouped_table",
            "primary_metric": "validation_rate",
        },

        # Preserved / backwards-compatible sections
        "core_area_matrix": {
            "title": "Core Area Health",
            "description": "Claim validation by broad football area.",
            "tab_id": "core_area_alignment",
            "chart_type": "bar",
            "primary_metric": "validation_rate",
            "compatibility": "preserved",
        },
        "category_matrix": {
            "title": "Category Health",
            "description": "Claim validation by football category within each core area.",
            "tab_id": "pillar_health",
            "chart_type": "grouped_table",
            "primary_metric": "validation_rate",
            "compatibility": "preserved",
        },
        "confidence_core_area_matrix": {
            "title": "Confidence by Core Area",
            "description": "Claim validation by confidence label and football area.",
            "tab_id": "core_area_alignment",
            "chart_type": "grouped_bar",
            "primary_metric": "validation_rate",
            "compatibility": "preserved",
        },
        "feature_scorecard": {
            "title": "Offensive Efficiency Feature Scorecard",
            "description": "Legacy claim validation scorecard by offensive efficiency support bucket.",
            "tab_id": "feature_health",
            "chart_type": "bar",
            "primary_metric": "validation_rate",
            "compatibility": "preserved",
        },
        "surface_matrix": {
            "title": "Claim Surface Health",
            "description": (
                "Claim validation by claim type and claim layer. Retained as a technical "
                "debug section rather than a primary product-facing dashboard section."
            ),
            "tab_id": "technical_debug",
            "chart_type": "table",
            "primary_metric": "validation_rate",
            "compatibility": "preserved",
            "section_role": "technical_debug",
        },
        "calibrated_game_level_calibration": {
            "title": "Calibrated Game-Level Calibration",
            "description": (
                "Admin-only preview of game outcome calibration after applying "
                "Core Area durability confidence softening. This does not change "
                "production /game confidence labels."
            ),
            "chart_type": "matrix",
            "primary_metric": "correct_rate",
            "tab_id": "game_calibration",
            "metadata_only": True,
            "production_use_allowed": False,
        },
        "calibrated_core_area_alignment_matrix": {
            "title": "Calibrated Matchup Lean × Core Area Alignment",
            "description": (
                "Admin-only preview of matchup profile type by calibrated confidence. "
                "High Confidence games with insufficient Core Area durability are "
                "shown as Medium for review."
            ),
            "chart_type": "matrix",
            "primary_metric": "correct_rate",
            "tab_id": "core_area_alignment",
            "metadata_only": True,
            "production_use_allowed": False,
        },
    }


def build_claim_health_response(
    run_id: str,
    season: str = "2025",
    calibration_grain: str = "week",
) -> dict:
    """
    Builds the aggregate admin calibration + claim-health response.

    This is intentionally aggregate-level only.
    It does not return game-level drilldown rows.
    It does not change model logic, matchup logic, or frontend behavior.
    """

    coverage = get_coverage_summary(run_id=run_id, season=season)
    baseline = get_baseline(run_id=run_id)

    return {
        "available": True,
        "scope": "admin_calibration_claim_health",
        "run_id": run_id,
        "season": season,
        "generated_at": datetime.now(timezone.utc).isoformat(),

        "default_tab": "overview",
        "tabs": _build_tabs(),
        "formula_notes": _build_formula_notes(),
        "season_phase_groups": _build_season_phase_groups(),

        "coverage": {
            **coverage,
            "context_note": _build_coverage_note(coverage),
        },

        "baseline": baseline,
        "section_metadata": _build_section_metadata(),

        "sections": {
            # New tab-ready sections
            "calibration_over_time": get_calibration_over_time(
                run_id=run_id,
                season=season,
                grain=calibration_grain,
            ),
            "game_level_calibration": get_game_level_calibration(run_id=run_id),
            "calibrated_game_level_calibration": get_calibrated_game_level_calibration(run_id=run_id),
            "core_area_alignment_matrix": get_core_area_alignment_matrix(run_id=run_id),
            "calibrated_core_area_alignment_matrix": get_calibrated_core_area_alignment_matrix(run_id=run_id),
            "pillar_health_matrix": get_pillar_health_matrix(run_id=run_id),
            "pillar_weekly_health": get_pillar_weekly_health(run_id=run_id, season=season),
            "feature_health_matrix": get_feature_health_matrix(run_id=run_id),

            # Preserved / backwards-compatible sections
            "core_area_matrix": get_core_area_matrix(run_id=run_id),
            "category_matrix": get_category_matrix(run_id=run_id),
            "confidence_core_area_matrix": get_confidence_core_area_matrix(run_id=run_id),
            "feature_scorecard": get_feature_scorecard(run_id=run_id),
            "surface_matrix": get_surface_matrix(run_id=run_id),
        },
    }
