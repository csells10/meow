from google.cloud import bigquery


client = bigquery.Client()

CLAIM_TABLE = "nfl-stream-406420.Analytics.gamelens_claim_training_examples"
SCHEDULE_TABLE = "nfl-stream-406420.League.schedule"


def _run_query(query: str, params: list[bigquery.ScalarQueryParameter]) -> list[dict]:
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(query, job_config=job_config).result()
    return [dict(row) for row in rows]


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

    rows = _run_query(
        query,
        [
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("season", "STRING", season),
        ],
    )

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

    rows = _run_query(
        query,
        [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
    )

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

    return _run_query(
        query,
        [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
    )


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

    return _run_query(
        query,
        [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
    )


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

    return _run_query(
        query,
        [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
    )

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

    return _run_query(
        query,
        [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
    )

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

    return _run_query(
        query,
        [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
    )