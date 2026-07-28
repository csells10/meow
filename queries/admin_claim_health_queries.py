from __future__ import annotations

from google.cloud import bigquery


CLAIM_TABLE = "nfl-stream-406420.Analytics.gamelens_claim_training_examples"
SCHEDULE_TABLE = "nfl-stream-406420.League.schedule"

_client = None


def _get_client() -> bigquery.Client:
    """Create the BigQuery client lazily so app import stays lightweight."""
    global _client
    if _client is None:
        _client = bigquery.Client()
    return _client


def _run_query(query: str, params: list[bigquery.ScalarQueryParameter]) -> list[dict]:
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = _get_client().query(query, job_config=job_config).result()
    return [dict(row) for row in rows]


def _run_id_param(run_id: str) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter("run_id", "STRING", run_id)


def _season_param(season: str) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter("season", "STRING", season)


def _grain_param(grain: str) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter("grain", "STRING", grain)

def _float_param(name: str, value: float) -> bigquery.ScalarQueryParameter:
    return bigquery.ScalarQueryParameter(name, "FLOAT64", value)


# -----------------------------------------------------------------------------
# Existing / backwards-compatible sections
# -----------------------------------------------------------------------------

def get_coverage_summary(run_id: str, season: str) -> dict:
    query = f"""
        WITH expected_games AS (
            SELECT
                gameID AS game_id
            FROM `{SCHEDULE_TABLE}`
            WHERE CAST(season AS STRING) = @season
              AND gameStatus IN ('Final', 'Final/OT')
              AND LOWER(CAST(gameWeek AS STRING)) NOT LIKE 'preseason%'
        ),

        claim_games AS (
            SELECT DISTINCT
                game_id
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
        )

        SELECT
            COUNT(*) AS expected_games,
            COUNTIF(c.game_id IS NOT NULL) AS games_with_claims,
            COUNTIF(c.game_id IS NULL) AS games_without_claims
        FROM expected_games e
        LEFT JOIN claim_games c
            ON e.game_id = c.game_id
    """

    rows = _run_query(query, [_run_id_param(run_id), _season_param(season)])
    return rows[0] if rows else {}


def get_baseline(run_id: str) -> dict:
    query = f"""
        SELECT
            COUNT(*) AS claim_row_count,
            COUNT(DISTINCT game_id) AS game_count,

            COUNTIF(validation_result = 'validated') AS validated_count,
            COUNTIF(validation_result = 'not_validated') AS not_validated_count,
            COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,
            COUNTIF(validation_result = 'unavailable') AS unavailable_count,
            COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable') AS eligible_claim_row_count,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'validated'),
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
            ) AS validation_rate,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'actual_neutral_or_mixed'),
                COUNT(*)
            ) AS neutral_or_mixed_rate
        FROM `{CLAIM_TABLE}`
        WHERE run_id = @run_id
    """

    rows = _run_query(query, [_run_id_param(run_id)])
    return rows[0] if rows else {}


def get_core_area_matrix(run_id: str) -> list[dict]:
    query = f"""
        SELECT
            COALESCE(registry_core_area, core_area, 'missing_core_area') AS core_area,

            COUNT(*) AS claim_row_count,
            COUNT(DISTINCT game_id) AS game_count,

            COUNTIF(validation_result = 'validated') AS validated_count,
            COUNTIF(validation_result = 'not_validated') AS not_validated_count,
            COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'validated'),
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
            ) AS validation_rate,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'actual_neutral_or_mixed'),
                COUNT(*)
            ) AS neutral_or_mixed_rate
        FROM `{CLAIM_TABLE}`
        WHERE run_id = @run_id
        GROUP BY core_area
        ORDER BY validation_rate DESC, claim_row_count DESC
    """

    return _run_query(query, [_run_id_param(run_id)])


def get_category_matrix(run_id: str) -> list[dict]:
    query = f"""
        SELECT
            COALESCE(registry_core_area, core_area, 'missing_core_area') AS core_area,
            COALESCE(registry_category, category, 'missing_category') AS category,

            COUNT(*) AS claim_row_count,
            COUNT(DISTINCT game_id) AS game_count,

            COUNTIF(validation_result = 'validated') AS validated_count,
            COUNTIF(validation_result = 'not_validated') AS not_validated_count,
            COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'validated'),
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
            ) AS validation_rate,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'actual_neutral_or_mixed'),
                COUNT(*)
            ) AS neutral_or_mixed_rate
        FROM `{CLAIM_TABLE}`
        WHERE run_id = @run_id
        GROUP BY
            core_area,
            category
        HAVING claim_row_count >= 30
        ORDER BY
            core_area,
            validation_rate DESC,
            claim_row_count DESC
    """

    return _run_query(query, [_run_id_param(run_id)])


def get_confidence_core_area_matrix(run_id: str) -> list[dict]:
    query = f"""
        SELECT
            COALESCE(outcome_confidence_label, 'missing_confidence') AS confidence,
            COALESCE(registry_core_area, core_area, 'missing_core_area') AS core_area,

            COUNT(*) AS claim_row_count,
            COUNT(DISTINCT game_id) AS game_count,

            COUNTIF(validation_result = 'validated') AS validated_count,
            COUNTIF(validation_result = 'not_validated') AS not_validated_count,
            COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'validated'),
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
            ) AS validation_rate,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'actual_neutral_or_mixed'),
                COUNT(*)
            ) AS neutral_or_mixed_rate
        FROM `{CLAIM_TABLE}`
        WHERE run_id = @run_id
        GROUP BY
            confidence,
            core_area
        HAVING claim_row_count >= 10
        ORDER BY
            confidence,
            validation_rate DESC,
            claim_row_count DESC
    """

    return _run_query(query, [_run_id_param(run_id)])


def get_surface_matrix(run_id: str) -> list[dict]:
    query = f"""
        SELECT
            COALESCE(claim_type, 'missing_claim_type') AS claim_type,
            COALESCE(claim_layer, 'missing_claim_layer') AS claim_layer,

            COUNT(*) AS claim_row_count,
            COUNT(DISTINCT game_id) AS game_count,

            COUNTIF(validation_result = 'validated') AS validated_count,
            COUNTIF(validation_result = 'not_validated') AS not_validated_count,
            COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'validated'),
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
            ) AS validation_rate,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'actual_neutral_or_mixed'),
                COUNT(*)
            ) AS neutral_or_mixed_rate
        FROM `{CLAIM_TABLE}`
        WHERE run_id = @run_id
        GROUP BY
            claim_type,
            claim_layer
        HAVING claim_row_count >= 30
        ORDER BY
            validation_rate DESC,
            claim_row_count DESC
    """

    return _run_query(query, [_run_id_param(run_id)])


def get_feature_scorecard(run_id: str) -> list[dict]:
    query = f"""
        SELECT
            offensive_efficiency_support_bucket AS bucket,
            offensive_efficiency_support_strength AS strength,

            COUNT(*) AS claim_row_count,
            COUNT(DISTINCT game_id) AS game_count,

            COUNTIF(validation_result = 'validated') AS validated_count,
            COUNTIF(validation_result = 'not_validated') AS not_validated_count,
            COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'validated'),
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
            ) AS validation_rate,

            SAFE_DIVIDE(
                COUNTIF(validation_result = 'actual_neutral_or_mixed'),
                COUNT(*)
            ) AS neutral_or_mixed_rate
        FROM `{CLAIM_TABLE}`
        WHERE run_id = @run_id
        GROUP BY
            bucket,
            strength
        ORDER BY
            validation_rate DESC,
            claim_row_count DESC
    """

    return _run_query(query, [_run_id_param(run_id)])


# -----------------------------------------------------------------------------
# New tab-ready Admin Calibration + Claim Health sections
# -----------------------------------------------------------------------------

def get_calibration_over_time(
    run_id: str,
    season: str,
    grain: str = "week",
) -> list[dict]:
    """
    Aggregate claim validation and game-pick calibration over time.

    Default grain is week. The SQL also supports day and season_phase so the
    service/route can expose a grain parameter later without changing the
    response shape.
    """
    allowed_grains = {"day", "week", "season_phase"}
    safe_grain = grain if grain in allowed_grains else "week"

    query = f"""
        WITH schedule_games AS (
            SELECT
                s.gameID AS game_id,
                DATE(s.gameDate) AS game_date,
                CAST(s.gameWeek AS STRING) AS game_week,
                SAFE_CAST(
                    REGEXP_EXTRACT(LOWER(CAST(s.gameWeek AS STRING)), r'week\\s+(\\d+)')
                    AS INT64
                ) AS week_number,
                CASE
                    WHEN SAFE_CAST(REGEXP_EXTRACT(LOWER(CAST(s.gameWeek AS STRING)), r'week\\s+(\\d+)') AS INT64) BETWEEN 1 AND 5
                        THEN 'early_season'
                    WHEN SAFE_CAST(REGEXP_EXTRACT(LOWER(CAST(s.gameWeek AS STRING)), r'week\\s+(\\d+)') AS INT64) BETWEEN 6 AND 12
                        THEN 'mid_season'
                    WHEN SAFE_CAST(REGEXP_EXTRACT(LOWER(CAST(s.gameWeek AS STRING)), r'week\\s+(\\d+)') AS INT64) BETWEEN 13 AND 18
                        THEN 'late_season'
                    WHEN LOWER(CAST(s.gameWeek AS STRING)) IN ('wild card', 'wildcard', 'divisional round', 'conference championship', 'super bowl')
                        THEN 'postseason'
                    ELSE 'unknown_phase'
                END AS season_phase
            FROM `{SCHEDULE_TABLE}` s
            WHERE CAST(s.season AS STRING) = @season
              AND s.gameStatus IN ('Final', 'Final/OT')
              AND LOWER(CAST(s.gameWeek AS STRING)) NOT LIKE 'preseason%'
        ),

        game_claims AS (
            SELECT
                game_id,
                COUNT(*) AS claim_rows,
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable') AS eligible_claim_rows,
                COUNTIF(validation_result = 'validated') AS validated_claims,
                COUNTIF(validation_result = 'not_validated') AS not_validated_claims,
                COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_mixed_claims,
                COUNTIF(validation_result = 'unavailable') AS unavailable_claims,
                ANY_VALUE(model_result) AS model_result
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY game_id
        ),

        joined AS (
            SELECT
                CASE
                    WHEN @grain = 'day' THEN FORMAT_DATE('%Y-%m-%d', sg.game_date)
                    WHEN @grain = 'season_phase' THEN sg.season_phase
                    ELSE sg.game_week
                END AS period_label,
                @grain AS grain,
                sg.season_phase,
                sg.game_date,
                sg.game_id,
                gc.claim_rows,
                gc.eligible_claim_rows,
                gc.validated_claims,
                gc.not_validated_claims,
                gc.neutral_mixed_claims,
                gc.unavailable_claims,
                LOWER(TRIM(CAST(gc.model_result AS STRING))) AS model_result
            FROM schedule_games sg
            LEFT JOIN game_claims gc
                ON sg.game_id = gc.game_id
        ),

        baseline AS (
            SELECT
                SAFE_DIVIDE(
                    COUNTIF(validation_result = 'validated'),
                    COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
                ) AS claim_baseline_rate
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
        ),

        grouped AS (
            SELECT
                period_label,
                CAST(MIN(game_date) AS STRING) AS period_start,
                CAST(MAX(game_date) AS STRING) AS period_end,
                ANY_VALUE(grain) AS grain,
                CASE
                    WHEN ANY_VALUE(grain) = 'season_phase' THEN period_label
                    ELSE ANY_VALUE(season_phase)
                END AS season_phase,

                COALESCE(SUM(claim_rows), 0) AS claim_rows,
                COALESCE(SUM(eligible_claim_rows), 0) AS eligible_claim_rows,
                COALESCE(SUM(validated_claims), 0) AS validated_claims,
                COALESCE(SUM(not_validated_claims), 0) AS not_validated_claims,
                COALESCE(SUM(neutral_mixed_claims), 0) AS neutral_mixed_claims,
                COALESCE(SUM(unavailable_claims), 0) AS unavailable_claims,
                SAFE_DIVIDE(SUM(validated_claims), SUM(eligible_claim_rows)) AS claim_validation_rate,

                COUNT(DISTINCT game_id) AS games_total,
                COUNT(DISTINCT IF(claim_rows IS NOT NULL, game_id, NULL)) AS games_with_claims,
                COUNT(DISTINCT IF(model_result IN ('correct', 'incorrect'), game_id, NULL)) AS games_with_pick,
                COUNT(DISTINCT IF(model_result IN ('correct', 'incorrect'), game_id, NULL)) AS graded_games,
                COUNT(DISTINCT IF(model_result = 'correct', game_id, NULL)) AS correct_picks,
                COUNT(DISTINCT IF(model_result = 'incorrect', game_id, NULL)) AS incorrect_picks,
                COUNT(DISTINCT IF(model_result = 'no pick', game_id, NULL)) AS no_pick_games,
                COUNT(DISTINCT IF(model_result IN ('no decision', 'tie', 'push', 'no_decision'), game_id, NULL)) AS no_decision_games,
                SAFE_DIVIDE(COUNT(DISTINCT IF(model_result = 'correct', game_id, NULL)),COUNT(DISTINCT IF(model_result IN ('correct', 'incorrect'), game_id, NULL))) AS  game_pick_correct_rate,
                'all_claims_in_period' AS selected_segment_label,
                COALESCE(SUM(claim_rows), 0) AS selected_segment_claim_rows,
                SAFE_DIVIDE(SUM(validated_claims), SUM(eligible_claim_rows)) AS selected_segment_validation_rate,
                SAFE_DIVIDE(SUM(validated_claims), SUM(eligible_claim_rows))
                    - ANY_VALUE(b.claim_baseline_rate) AS selected_segment_lift_vs_claim_baseline
            FROM joined
            CROSS JOIN baseline b
            GROUP BY period_label
        )

        SELECT *
        FROM grouped
        ORDER BY period_start, period_label
    """

    return _run_query(
        query,
        [_run_id_param(run_id), _season_param(season), _grain_param(safe_grain)],
    )


def get_game_level_calibration(run_id: str) -> list[dict]:
    query = f"""
        WITH games AS (
            SELECT
                game_id,
                COALESCE(ANY_VALUE(profile_strength_label), 'missing_profile_strength') AS profile_strength_label,
                COALESCE(ANY_VALUE(outcome_confidence_label), 'missing_confidence') AS outcome_confidence_label,
                LOWER(TRIM(CAST(ANY_VALUE(model_result) AS STRING))) AS model_result,
                ANY_VALUE(final_margin_abs) AS final_margin_abs
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY game_id
        )

        SELECT
            profile_strength_label,
            outcome_confidence_label,
            COUNT(*) AS game_count,
            COUNTIF(model_result IN ('correct', 'incorrect')) AS graded_game_count,
            COUNTIF(model_result = 'correct') AS correct_count,
            COUNTIF(model_result = 'incorrect') AS incorrect_count,
            COUNTIF(model_result = 'no pick') AS no_pick_count,
            COUNTIF(model_result IN ('no decision', 'tie', 'push', 'no_decision')) AS no_decision_count,
            SAFE_DIVIDE(COUNTIF(model_result = 'correct'), COUNTIF(model_result IN ('correct', 'incorrect'))) AS correct_rate,
            SAFE_DIVIDE(COUNTIF(model_result = 'incorrect'), COUNTIF(model_result IN ('correct', 'incorrect'))) AS incorrect_rate,
            SAFE_DIVIDE(COUNTIF(model_result = 'no pick'), COUNT(*)) AS no_pick_rate,
            SAFE_DIVIDE(COUNTIF(model_result IN ('no decision', 'tie', 'push', 'no_decision')) ,COUNT(*)) AS no_decision_rate,
            AVG(SAFE_CAST(final_margin_abs AS FLOAT64)) AS avg_final_margin_abs,
            COUNTIF(model_result = 'incorrect' AND SAFE_CAST(final_margin_abs AS FLOAT64) <= 3) AS close_miss_count,
            COUNTIF(model_result = 'incorrect' AND SAFE_CAST(final_margin_abs AS FLOAT64) >= 17) AS severe_miss_count
        FROM games
        GROUP BY
            profile_strength_label,
            outcome_confidence_label
        ORDER BY
            profile_strength_label,
            outcome_confidence_label
    """

    return _run_query(query, [_run_id_param(run_id)])

def get_calibrated_game_level_calibration(
    run_id: str,
    core_gap_floor: float = 0.45,
) -> list[dict]:
    """
    Admin-only preview.

    Same shape as get_game_level_calibration(), but groups by calibrated confidence:
      High + core_gap < core_gap_floor -> Medium

    Does not change /game, BigQuery, or production labels.
    """
    query = f"""
        WITH games AS (
            SELECT
                game_id,
                COALESCE(ANY_VALUE(profile_strength_label), 'missing_profile_strength') AS profile_strength_label,

                CASE
                    WHEN COALESCE(ANY_VALUE(outcome_confidence_label), 'missing_confidence') = 'High'
                     AND SAFE_CAST(ANY_VALUE(core_gap) AS FLOAT64) < @core_gap_floor
                        THEN 'Medium'
                    ELSE COALESCE(ANY_VALUE(outcome_confidence_label), 'missing_confidence')
                END AS outcome_confidence_label,

                LOWER(TRIM(CAST(ANY_VALUE(model_result) AS STRING))) AS model_result,
                ANY_VALUE(final_margin_abs) AS final_margin_abs
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY game_id
        )

        SELECT
            profile_strength_label,
            outcome_confidence_label,
            COUNT(*) AS game_count,
            COUNTIF(model_result IN ('correct', 'incorrect')) AS graded_game_count,
            COUNTIF(model_result = 'correct') AS correct_count,
            COUNTIF(model_result = 'incorrect') AS incorrect_count,
            COUNTIF(model_result = 'no pick') AS no_pick_count,
            COUNTIF(model_result IN ('no decision', 'tie', 'push', 'no_decision')) AS no_decision_count,
            SAFE_DIVIDE(COUNTIF(model_result = 'correct'), COUNTIF(model_result IN ('correct', 'incorrect'))) AS correct_rate,
            SAFE_DIVIDE(COUNTIF(model_result = 'incorrect'), COUNTIF(model_result IN ('correct', 'incorrect'))) AS incorrect_rate,
            SAFE_DIVIDE(COUNTIF(model_result = 'no pick'), COUNT(*)) AS no_pick_rate,
            SAFE_DIVIDE(COUNTIF(model_result IN ('no decision', 'tie', 'push', 'no_decision')), COUNT(*)) AS no_decision_rate,
            AVG(SAFE_CAST(final_margin_abs AS FLOAT64)) AS avg_final_margin_abs,
            COUNTIF(model_result = 'incorrect' AND SAFE_CAST(final_margin_abs AS FLOAT64) <= 3) AS close_miss_count,
            COUNTIF(model_result = 'incorrect' AND SAFE_CAST(final_margin_abs AS FLOAT64) >= 17) AS severe_miss_count
        FROM games
        GROUP BY
            profile_strength_label,
            outcome_confidence_label
        ORDER BY
            profile_strength_label,
            outcome_confidence_label
    """

    return _run_query(
        query,
        [
            _run_id_param(run_id),
            _float_param("core_gap_floor", core_gap_floor),
        ],
    )

def get_core_area_alignment_matrix(run_id: str) -> list[dict]:
    query = f"""
        WITH games AS (
            SELECT
                game_id,
                COALESCE(ANY_VALUE(profile_type), 'missing_profile_type') AS profile_type,
                COALESCE(ANY_VALUE(outcome_confidence_label), 'missing_confidence') AS outcome_confidence_label,
                LOWER(TRIM(CAST(ANY_VALUE(model_result) AS STRING))) AS model_result,
                ANY_VALUE(core_gap) AS core_gap,
                ANY_VALUE(signal_gap) AS signal_gap,
                ANY_VALUE(final_margin_abs) AS final_margin_abs
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY game_id
        )

        SELECT
            profile_type,
            outcome_confidence_label,
            COUNT(*) AS game_count,
            COUNTIF(model_result IN ('correct', 'incorrect')) AS graded_game_count,
            COUNTIF(model_result = 'correct') AS correct_count,
            COUNTIF(model_result = 'incorrect') AS incorrect_count,
            COUNTIF(model_result = 'no pick') AS no_pick_count,
            COUNTIF(model_result IN ('no decision', 'tie', 'push', 'no_decision')) AS no_decision_count,
            SAFE_DIVIDE(COUNTIF(model_result = 'correct'), COUNTIF(model_result IN ('correct', 'incorrect'))) AS correct_rate,
            AVG(SAFE_CAST(core_gap AS FLOAT64)) AS avg_core_gap,
            AVG(SAFE_CAST(signal_gap AS FLOAT64)) AS avg_signal_gap,
            AVG(SAFE_CAST(final_margin_abs AS FLOAT64)) AS avg_final_margin_abs
        FROM games
        GROUP BY
            profile_type,
            outcome_confidence_label
        ORDER BY
            profile_type,
            outcome_confidence_label
    """

    return _run_query(query, [_run_id_param(run_id)])

def get_calibrated_core_area_alignment_matrix(
    run_id: str,
    core_gap_floor: float = 0.45,
) -> list[dict]:
    """
    Admin-only preview.

    Same shape as get_core_area_alignment_matrix(), but groups by calibrated confidence:
      High + core_gap < core_gap_floor -> Medium

    Does not change /game, BigQuery, or production labels.
    """
    query = f"""
        WITH games AS (
            SELECT
                game_id,
                COALESCE(ANY_VALUE(profile_type), 'missing_profile_type') AS profile_type,

                CASE
                    WHEN COALESCE(ANY_VALUE(outcome_confidence_label), 'missing_confidence') = 'High'
                     AND SAFE_CAST(ANY_VALUE(core_gap) AS FLOAT64) < @core_gap_floor
                        THEN 'Medium'
                    ELSE COALESCE(ANY_VALUE(outcome_confidence_label), 'missing_confidence')
                END AS outcome_confidence_label,

                LOWER(TRIM(CAST(ANY_VALUE(model_result) AS STRING))) AS model_result,
                ANY_VALUE(core_gap) AS core_gap,
                ANY_VALUE(signal_gap) AS signal_gap,
                ANY_VALUE(final_margin_abs) AS final_margin_abs
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY game_id
        )

        SELECT
            profile_type,
            outcome_confidence_label,
            COUNT(*) AS game_count,
            COUNTIF(model_result IN ('correct', 'incorrect')) AS graded_game_count,
            COUNTIF(model_result = 'correct') AS correct_count,
            COUNTIF(model_result = 'incorrect') AS incorrect_count,
            COUNTIF(model_result = 'no pick') AS no_pick_count,
            COUNTIF(model_result IN ('no decision', 'tie', 'push', 'no_decision')) AS no_decision_count,
            SAFE_DIVIDE(COUNTIF(model_result = 'correct'), COUNTIF(model_result IN ('correct', 'incorrect'))) AS correct_rate,
            AVG(SAFE_CAST(core_gap AS FLOAT64)) AS avg_core_gap,
            AVG(SAFE_CAST(signal_gap AS FLOAT64)) AS avg_signal_gap,
            AVG(SAFE_CAST(final_margin_abs AS FLOAT64)) AS avg_final_margin_abs
        FROM games
        GROUP BY
            profile_type,
            outcome_confidence_label
        ORDER BY
            profile_type,
            outcome_confidence_label
    """

    return _run_query(
        query,
        [
            _run_id_param(run_id),
            _float_param("core_gap_floor", core_gap_floor),
        ],
    )

def get_pillar_health_matrix(run_id: str) -> list[dict]:
    query = f"""
        WITH baseline AS (
            SELECT
                SAFE_DIVIDE(
                    COUNTIF(validation_result = 'validated'),
                    COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
                ) AS claim_baseline_rate
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
        ),

        grouped AS (
            SELECT
                COALESCE(registry_core_area, core_area, 'missing_core_area') AS core_area,
                COALESCE(registry_category, category, 'missing_category') AS category,
                COUNT(*) AS claim_rows,
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable') AS eligible_claim_rows,
                COUNTIF(validation_result = 'validated') AS validated_claims,
                COUNTIF(validation_result = 'not_validated') AS not_validated_claims,
                COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_mixed_claims,
                COUNT(DISTINCT game_id) AS games_represented
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY core_area, category
        )

        SELECT
            g.core_area,
            g.category,
            g.claim_rows,
            g.eligible_claim_rows,
            g.validated_claims,
            SAFE_DIVIDE(g.validated_claims, g.eligible_claim_rows) AS validation_rate,
            SAFE_DIVIDE(g.neutral_mixed_claims, g.claim_rows) AS neutral_mixed_rate,
            SAFE_DIVIDE(g.not_validated_claims, g.eligible_claim_rows) AS not_validated_rate,
            SAFE_DIVIDE(g.validated_claims, g.eligible_claim_rows)
                - b.claim_baseline_rate AS lift_vs_claim_baseline,
            g.games_represented
        FROM grouped g
        CROSS JOIN baseline b
        ORDER BY
            g.core_area,
            validation_rate DESC,
            g.claim_rows DESC
    """

    return _run_query(query, [_run_id_param(run_id)])


def get_pillar_weekly_health(run_id: str, season: str) -> list[dict]:
    query = f"""
        WITH schedule_games AS (
            SELECT
                gameID AS game_id,
                CAST(gameWeek AS STRING) AS game_week,
                DATE(gameDate) AS game_date
            FROM `{SCHEDULE_TABLE}`
            WHERE CAST(season AS STRING) = @season
              AND gameStatus IN ('Final', 'Final/OT')
              AND LOWER(CAST(gameWeek AS STRING)) NOT LIKE 'preseason%'
        ),

        game_rows AS (
            SELECT
                game_id,
                LOWER(TRIM(CAST(ANY_VALUE(model_result) AS STRING))) AS model_result,
                ANY_VALUE(outcome_confidence_label) AS outcome_confidence_label
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
            GROUP BY game_id
        ),

        game_week AS (
            SELECT
                sg.game_week,
                COUNT(DISTINCT sg.game_id) AS games_total,
                COUNT(DISTINCT IF(gr.game_id IS NOT NULL, sg.game_id, NULL)) AS games_with_claims,
                COUNT(DISTINCT IF(gr.model_result = 'correct', sg.game_id, NULL)) AS correct_picks,
                COUNT(DISTINCT IF(gr.model_result = 'incorrect', sg.game_id, NULL)) AS incorrect_picks,
                COUNT(DISTINCT IF(gr.model_result = 'no pick', sg.game_id, NULL)) AS no_pick_games,
                COUNT(DISTINCT IF(gr.model_result IN ('no decision', 'tie', 'push', 'no_decision'), sg.game_id, NULL)) AS no_decision_games,
                COUNT(DISTINCT IF(gr.model_result IN ('correct', 'incorrect'), sg.game_id, NULL)) AS graded_games,
                AVG(
                    CASE LOWER(COALESCE(gr.outcome_confidence_label, ''))
                        WHEN 'high' THEN 3.0
                        WHEN 'medium' THEN 2.0
                        WHEN 'low' THEN 1.0
                        ELSE NULL
                    END
                ) AS avg_confidence
            FROM schedule_games sg
            LEFT JOIN game_rows gr
                ON sg.game_id = gr.game_id
            GROUP BY sg.game_week
        ),

        claim_week AS (
            SELECT
                COALESCE(sg.game_week, c.game_week) AS game_week,
                COUNT(*) AS claim_rows,
                COUNTIF(c.validation_result IS NOT NULL AND c.validation_result != 'unavailable') AS eligible_claim_rows,
                COUNTIF(c.validation_result = 'validated') AS validated_claims
            FROM `{CLAIM_TABLE}` c
            LEFT JOIN schedule_games sg
                ON c.game_id = sg.game_id
            WHERE c.run_id = @run_id
            GROUP BY game_week
        ),

        core_area_week AS (
            SELECT
                COALESCE(sg.game_week, c.game_week) AS game_week,
                COALESCE(c.registry_core_area, c.core_area, 'missing_core_area') AS core_area,
                COUNT(*) AS claim_rows,
                COUNTIF(c.validation_result IS NOT NULL AND c.validation_result != 'unavailable') AS eligible_claim_rows,
                COUNTIF(c.validation_result = 'validated') AS validated_claims,
                SAFE_DIVIDE(
                    COUNTIF(c.validation_result = 'validated'),
                    COUNTIF(c.validation_result IS NOT NULL AND c.validation_result != 'unavailable')
                ) AS validation_rate
            FROM `{CLAIM_TABLE}` c
            LEFT JOIN schedule_games sg
                ON c.game_id = sg.game_id
            WHERE c.run_id = @run_id
            GROUP BY game_week, core_area
        ),

        core_rank AS (
            SELECT
                game_week,
                ARRAY_AGG(
                    STRUCT(core_area, validation_rate, claim_rows)
                    ORDER BY validation_rate DESC, claim_rows DESC
                    LIMIT 1
                )[SAFE_OFFSET(0)] AS top_area,
                ARRAY_AGG(
                    STRUCT(core_area, validation_rate, claim_rows)
                    ORDER BY validation_rate ASC, claim_rows DESC
                    LIMIT 1
                )[SAFE_OFFSET(0)] AS weakest_area
            FROM core_area_week
            WHERE eligible_claim_rows > 0
            GROUP BY game_week
        )

        SELECT
            gw.game_week,
            gw.games_total,
            gw.games_with_claims,
            gw.graded_games,
            gw.no_decision_games,
            COALESCE(cw.claim_rows, 0) AS claim_rows,
            SAFE_DIVIDE(cw.validated_claims, cw.eligible_claim_rows) AS claim_validation_rate,
            SAFE_DIVIDE(gw.correct_picks, gw.graded_games) AS game_pick_correct_rate,
            SAFE_DIVIDE(gw.no_pick_games, gw.games_with_claims) AS no_pick_rate,
            gw.avg_confidence,
            cr.top_area.core_area AS top_core_area,
            cr.weakest_area.core_area AS weakest_core_area
        FROM game_week gw
        LEFT JOIN claim_week cw
            ON gw.game_week = cw.game_week
        LEFT JOIN core_rank cr
            ON gw.game_week = cr.game_week
        ORDER BY COALESCE(SAFE_CAST(REGEXP_EXTRACT(LOWER(gw.game_week), r'week\\s+(\\d+)') AS INT64), 999), gw.game_week
    """

    return _run_query(query, [_run_id_param(run_id), _season_param(season)])

def get_feature_health_matrix(run_id: str) -> list[dict]:
    query = f"""
        WITH baseline AS (
            SELECT
                SAFE_DIVIDE(
                    COUNTIF(validation_result = 'validated'),
                    COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable')
                ) AS claim_baseline_rate
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
        ),

        feature_rows AS (
            SELECT
                'Football Calibration Features' AS feature_group,
                'offensive_efficiency_support_v1' AS feature_family,
                COALESCE(offensive_efficiency_support_bucket, 'missing_bucket') AS bucket_or_group,
                game_id,
                validation_result
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id

            UNION ALL

            SELECT
                'Football Calibration Features' AS feature_group,
                'offense_finish_score' AS feature_family,
                CASE
                    WHEN offense_finish_score IS NULL THEN 'missing'
                    WHEN offense_finish_score >= 0.40 THEN 'strong_positive'
                    WHEN offense_finish_score >= 0.15 THEN 'positive'
                    WHEN offense_finish_score > -0.15 THEN 'mixed_near_even'
                    WHEN offense_finish_score > -0.40 THEN 'negative'
                    ELSE 'strong_negative'
                END AS bucket_or_group,
                game_id,
                validation_result
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id

            UNION ALL

            SELECT
                'Football Calibration Features' AS feature_group,
                'defensive_suppression_score' AS feature_family,
                CASE
                    WHEN defensive_suppression_score IS NULL THEN 'missing'
                    WHEN defensive_suppression_score >= 0.40 THEN 'strong_positive'
                    WHEN defensive_suppression_score >= 0.15 THEN 'positive'
                    WHEN defensive_suppression_score > -0.15 THEN 'mixed_near_even'
                    WHEN defensive_suppression_score > -0.40 THEN 'negative'
                    ELSE 'strong_negative'
                END AS bucket_or_group,
                game_id,
                validation_result
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id

            UNION ALL

            SELECT
                'Football Calibration Features' AS feature_group,
                'two_way_context' AS feature_family,
                COALESCE(two_way_context, 'missing_context') AS bucket_or_group,
                game_id,
                validation_result
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id

            UNION ALL

            SELECT
                'Data Quality / Metadata Features' AS feature_group,
                'clean_hierarchy_context_v1' AS feature_family,
                COALESCE(clean_hierarchy_status, 'missing_status') AS bucket_or_group,
                game_id,
                validation_result
            FROM `{CLAIM_TABLE}`
            WHERE run_id = @run_id
        ),

        grouped AS (
            SELECT
                feature_group,
                feature_family,
                bucket_or_group,
                COUNT(*) AS claim_rows,
                COUNTIF(validation_result IS NOT NULL AND validation_result != 'unavailable') AS eligible_claim_rows,
                COUNT(DISTINCT game_id) AS games_represented,
                COUNTIF(validation_result = 'validated') AS validated_claims,
                COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_mixed_claims,
                COUNTIF(validation_result = 'not_validated') AS not_validated_claims
            FROM feature_rows
            GROUP BY
                feature_group,
                feature_family,
                bucket_or_group
        )

        SELECT
            g.feature_group,
            g.feature_family,
            g.bucket_or_group,
            g.claim_rows,
            g.eligible_claim_rows,
            g.games_represented,
            SAFE_DIVIDE(g.validated_claims, g.eligible_claim_rows) AS validation_rate,
            SAFE_DIVIDE(g.validated_claims, g.eligible_claim_rows)
                - b.claim_baseline_rate AS lift_vs_claim_baseline,
            SAFE_DIVIDE(g.neutral_mixed_claims, g.claim_rows) AS neutral_mixed_rate,
            SAFE_DIVIDE(g.not_validated_claims, g.eligible_claim_rows) AS not_validated_rate,
            CASE
                WHEN g.claim_rows < 10 THEN 'very_low_sample'
                WHEN g.claim_rows < 30 THEN 'low_sample'
                ELSE NULL
            END AS sample_warning
        FROM grouped g
        CROSS JOIN baseline b
        ORDER BY
            g.feature_group,
            g.feature_family,
            validation_rate DESC,
            g.claim_rows DESC
    """

    return _run_query(query, [_run_id_param(run_id)])
