from google.cloud import bigquery


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"


def create_table(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
    description: str | None = None,
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
):
    """
    Creates a BigQuery table using the same style/pattern as the existing
    insert_bq.py helper.

    Notes:
    - BigQuery does not enforce uniqueness on claim_key. The builder job should
      still MERGE/upsert using claim_key + run_id or another deterministic key.
    - This script only creates the table shape. It does not populate rows.
    """

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    if description:
        table.description = description

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )

    if clustering_fields:
        table.clustering_fields = clustering_fields

    try:
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")
    except Exception as e:
        print(f"⚠️ Failed to create table {table_ref}: {e}")


def create_gamelens_claim_training_examples_table(client: bigquery.Client):
    """
    Creates Analytics.gamelens_claim_training_examples.

    Grain:
        One row per extracted GameLens claim per game.

    Why this table exists:
        This is the bridge between GameLens explanation QA and a future model layer.

        Instead of training first on "did the team win?", this table lets us train on:
        - Did the pregame claim validate?
        - Did the claim deserve stronger/elevated language?
        - Which pregame features made a claim trustworthy?
        - Which claim types are noisy unless paired with other football context?

    Fill strategy:
        Stage 1 should populate direct/extracted fields from the payload and QA outputs.
        Stage 2 should populate simple calculated fields, such as abs gaps and agreement rates.
        Stage 3 should populate combo/model features, such as offense_finish_score.
        Stage 4 should populate postgame validation labels after final metrics exist.

    Important:
        Combo FLOAT fields are intentionally nullable. Do not fake these values.
        Keep them NULL until the formula is intentionally implemented.
    """

    schema = [
        # ------------------------------------------------------------------
        # Run / record identity
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "claim_key",
            "STRING",
            mode="REQUIRED",
            description=(
                "Deterministic row key for this claim example. Suggested format: "
                "game_id|claim_type|claim_layer|group_name|metric|claimed_team. "
                "BigQuery will not enforce uniqueness; the builder job should MERGE on it."
            ),
        ),
        bigquery.SchemaField(
            "run_id",
            "STRING",
            mode="REQUIRED",
            description="Feature/validation build run identifier that created this row.",
        ),
        bigquery.SchemaField(
            "model_version",
            "STRING",
            mode="NULLABLE",
            description="Optional GameLens model/rules version used when this claim was generated.",
        ),
        bigquery.SchemaField(
            "source_payload_run",
            "STRING",
            mode="NULLABLE",
            description="Payload run folder/name used as source input, such as baseline_32_v2.",
        ),
        bigquery.SchemaField(
            "source_payload_path",
            "STRING",
            mode="NULLABLE",
            description="Path or URI to the source payload JSON for this game.",
        ),

        # ------------------------------------------------------------------
        # Build stage / row completeness
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "feature_build_stage",
            "STRING",
            mode="NULLABLE",
            description=(
                "How complete this row is: raw_extracted, basic_features, "
                "combo_features, validated, or model_scored."
            ),
        ),
        bigquery.SchemaField(
            "feature_status",
            "STRING",
            mode="NULLABLE",
            description=(
                "Row status: complete, partial, missing_metric_context, "
                "missing_postgame_actual, formula_not_implemented, or error."
            ),
        ),
        bigquery.SchemaField(
            "feature_notes",
            "STRING",
            mode="NULLABLE",
            description="Debug notes explaining missing/null fields or formula decisions.",
        ),

        # ------------------------------------------------------------------
        # Game identity
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "game_id",
            "STRING",
            mode="REQUIRED",
            description="Join key. Unique game identifier in format YYYYMMDD_AWAY@HOME.",
        ),
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year, such as 2025.",
        ),
        bigquery.SchemaField(
            "game_date",
            "DATE",
            mode="NULLABLE",
            description="Calendar date of the game.",
        ),
        bigquery.SchemaField(
            "game_week",
            "STRING",
            mode="NULLABLE",
            description="Week of the NFL season, such as 'Week 10', 'Wild Card', or 'Super Bowl'.",
        ),
        bigquery.SchemaField(
            "season_type",
            "STRING",
            mode="NULLABLE",
            description="Season type, such as Preseason, Regular Season, or Postseason.",
        ),
        bigquery.SchemaField(
            "bucket",
            "STRING",
            mode="NULLABLE",
            description="QA sample bucket, such as early_regular, mid_regular, late_regular, postseason, or manual.",
        ),
        bigquery.SchemaField(
            "game_status",
            "STRING",
            mode="NULLABLE",
            description="Game status from source schedule/API, such as Final or Final/OT.",
        ),

        # ------------------------------------------------------------------
        # Teams
        # ------------------------------------------------------------------
        bigquery.SchemaField("away_team", "STRING", mode="NULLABLE", description="Away team abbreviation."),
        bigquery.SchemaField("home_team", "STRING", mode="NULLABLE", description="Home team abbreviation."),
        bigquery.SchemaField(
            "claimed_team",
            "STRING",
            mode="REQUIRED",
            description="Team abbreviation that the pregame claim favored.",
        ),
        bigquery.SchemaField(
            "claimed_side",
            "STRING",
            mode="NULLABLE",
            description="Side favored by the pregame claim: away or home.",
        ),
        bigquery.SchemaField(
            "opponent_team",
            "STRING",
            mode="NULLABLE",
            description="Opponent team abbreviation for the claimed team.",
        ),
        bigquery.SchemaField(
            "opponent_side",
            "STRING",
            mode="NULLABLE",
            description="Opponent side for the claimed team: away or home.",
        ),

        # ------------------------------------------------------------------
        # Claim identity
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "claim_type",
            "STRING",
            mode="REQUIRED",
            description=(
                "Source type of the claim: game_profile, core_area_comparison, "
                "core_area_summary, category_summary, metric_highlight, or team_comparison_metric."
            ),
        ),
        bigquery.SchemaField(
            "claim_layer",
            "STRING",
            mode="REQUIRED",
            description="Scoring layer for this claim: headline or supporting.",
        ),
        bigquery.SchemaField(
            "claim_rank",
            "INTEGER",
            mode="NULLABLE",
            description="Optional display/order rank of this claim within its source section.",
        ),
        bigquery.SchemaField(
            "claim_name",
            "STRING",
            mode="NULLABLE",
            description="Display name of the claim, such as Pressure, Offensive Output, or Yards Per Pass.",
        ),
        bigquery.SchemaField(
            "claim_text",
            "STRING",
            mode="NULLABLE",
            description="Pregame user-facing claim text extracted from the GameLens payload.",
        ),
        bigquery.SchemaField(
            "source_section",
            "STRING",
            mode="NULLABLE",
            description="Payload section where this claim came from, such as matchup_breakdown.metric_highlights.",
        ),
        bigquery.SchemaField(
            "source_field_path",
            "STRING",
            mode="NULLABLE",
            description="Optional dotted/json path for debugging the exact source field.",
        ),
        bigquery.SchemaField(
            "group_name",
            "STRING",
            mode="NULLABLE",
            description="Football group name, such as Pressure, Scoring Efficiency, Defensive Control, or Rushing Game.",
        ),
        bigquery.SchemaField(
            "core_area",
            "STRING",
            mode="NULLABLE",
            description="Core Area associated with the claim, such as Offensive Output or Defensive Control.",
        ),
        bigquery.SchemaField(
            "category",
            "STRING",
            mode="NULLABLE",
            description="Metric category associated with the claim, such as Passing Game or Scoring Suppression.",
        ),
        bigquery.SchemaField(
            "metric",
            "STRING",
            mode="NULLABLE",
            description="Metric key associated with the claim, such as yards_per_pass or points_allowed_per_play.",
        ),
        bigquery.SchemaField(
            "metric_label",
            "STRING",
            mode="NULLABLE",
            description="Human-readable metric label.",
        ),

        # ------------------------------------------------------------------
        # Pregame claim language / metadata
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "claim_level",
            "STRING",
            mode="NULLABLE",
            description="Game Profile claim level when available, such as Elevated or Neutral.",
        ),
        bigquery.SchemaField(
            "claim_level_index",
            "INTEGER",
            mode="NULLABLE",
            description="Numeric Game Profile level index when available.",
        ),
        bigquery.SchemaField(
            "summary_label",
            "STRING",
            mode="NULLABLE",
            description="Pregame claim language label, such as slight_advantage, advantage, or clear_advantage.",
        ),
        bigquery.SchemaField(
            "signal_strength",
            "STRING",
            mode="NULLABLE",
            description="Registry/payload signal strength, such as strong, supporting, or context.",
        ),
        bigquery.SchemaField(
            "confidence_eligible",
            "BOOLEAN",
            mode="NULLABLE",
            description="Whether this metric/claim was allowed to affect confidence.",
        ),
        bigquery.SchemaField(
            "headline_eligible",
            "BOOLEAN",
            mode="NULLABLE",
            description="Whether this metric/claim was eligible to appear as a headline claim.",
        ),
        bigquery.SchemaField(
            "edge_language_allowed",
            "BOOLEAN",
            mode="NULLABLE",
            description="Whether edge/better-worse language is allowed for this metric.",
        ),
        bigquery.SchemaField(
            "data_quality_status",
            "STRING",
            mode="NULLABLE",
            description="Metric data quality status from registry/payload, such as good or watch.",
        ),
        bigquery.SchemaField(
            "ranking_kind",
            "STRING",
            mode="NULLABLE",
            description="Ranking kind, such as edge or context.",
        ),
        bigquery.SchemaField(
            "ranking_usage",
            "STRING",
            mode="NULLABLE",
            description="How the ranking should be used, such as edge or context_only.",
        ),

        # ------------------------------------------------------------------
        # Pregame numeric matchup features
        # These are either directly from payload/ranking context or simple gaps.
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "claimed_team_value",
            "FLOAT",
            mode="NULLABLE",
            description="Pregame metric value for the claimed team.",
        ),
        bigquery.SchemaField(
            "opponent_team_value",
            "FLOAT",
            mode="NULLABLE",
            description="Pregame metric value for the opponent team.",
        ),
        bigquery.SchemaField(
            "pregame_raw_gap",
            "FLOAT",
            mode="NULLABLE",
            description="Pregame raw metric gap between claimed team and opponent, direction-adjusted when possible.",
        ),
        bigquery.SchemaField(
            "pregame_abs_raw_gap",
            "FLOAT",
            mode="NULLABLE",
            description="Absolute value of pregame_raw_gap. Simple calc: abs(pregame_raw_gap).",
        ),
        bigquery.SchemaField(
            "pregame_rank_gap",
            "INTEGER",
            mode="NULLABLE",
            description="Pregame rank gap. Positive should favor claimed team when direction-adjusted.",
        ),
        bigquery.SchemaField(
            "pregame_percentile_gap",
            "FLOAT",
            mode="NULLABLE",
            description="Pregame league percentile gap. Positive should favor claimed team.",
        ),
        bigquery.SchemaField(
            "pregame_abs_percentile_gap",
            "FLOAT",
            mode="NULLABLE",
            description="Absolute value of pregame_percentile_gap. Simple calc: abs(pregame_percentile_gap).",
        ),
        bigquery.SchemaField(
            "claimed_team_league_rank",
            "INTEGER",
            mode="NULLABLE",
            description="League rank for the claimed team on this metric at the pregame as-of date.",
        ),
        bigquery.SchemaField(
            "opponent_team_league_rank",
            "INTEGER",
            mode="NULLABLE",
            description="League rank for the opponent team on this metric at the pregame as-of date.",
        ),
        bigquery.SchemaField(
            "claimed_team_league_percentile",
            "FLOAT",
            mode="NULLABLE",
            description="League percentile for the claimed team on this metric at the pregame as-of date.",
        ),
        bigquery.SchemaField(
            "opponent_team_league_percentile",
            "FLOAT",
            mode="NULLABLE",
            description="League percentile for the opponent team on this metric at the pregame as-of date.",
        ),
        bigquery.SchemaField(
            "claimed_team_tier",
            "STRING",
            mode="NULLABLE",
            description="League-relative tier for claimed team, such as elite, strong, average, weak, or poor.",
        ),
        bigquery.SchemaField(
            "opponent_team_tier",
            "STRING",
            mode="NULLABLE",
            description="League-relative tier for opponent team, such as elite, strong, average, weak, or poor.",
        ),

        # ------------------------------------------------------------------
        # Pregame game/matchup context
        # ------------------------------------------------------------------
        bigquery.SchemaField("window_type", "STRING", mode="NULLABLE", description="Metric window used for this claim."),
        bigquery.SchemaField("as_of_date", "DATE", mode="NULLABLE", description="Pregame as-of date for rankings/windowed metrics."),
        bigquery.SchemaField("source_data_date", "DATE", mode="NULLABLE", description="Most recent source data date in the metric window."),
        bigquery.SchemaField("data_lag_days", "INTEGER", mode="NULLABLE", description="Days between game date and source_data_date."),
        bigquery.SchemaField("profile_type", "STRING", mode="NULLABLE", description="GameLens profile type, such as confirmed_edge or no_clear_edge."),
        bigquery.SchemaField("profile_strength_code", "STRING", mode="NULLABLE", description="Normalized profile strength code."),
        bigquery.SchemaField("profile_strength_label", "STRING", mode="NULLABLE", description="Human-readable profile strength label."),
        bigquery.SchemaField("outcome_confidence_code", "STRING", mode="NULLABLE", description="Normalized outcome confidence code."),
        bigquery.SchemaField("outcome_confidence_label", "STRING", mode="NULLABLE", description="Human-readable outcome confidence label."),
        bigquery.SchemaField("matchup_label", "STRING", mode="NULLABLE", description="Combined profile/outcome label from GameLens."),
        bigquery.SchemaField("core_area_split", "STRING", mode="NULLABLE", description="Core Area split, such as 3-1 or 2-2."),
        bigquery.SchemaField("core_gap", "FLOAT", mode="NULLABLE", description="Pregame Core Area average gap from matchup_lean context."),
        bigquery.SchemaField("signal_gap", "INTEGER", mode="NULLABLE", description="Game Profile signal score gap from matchup_lean."),
        bigquery.SchemaField("team_comp_edge_score", "FLOAT", mode="NULLABLE", description="Team Comparison edge score from model trust/matchup lean."),
        bigquery.SchemaField("team_comp_away_count", "INTEGER", mode="NULLABLE", description="Visible Team Comparison metrics favoring away team."),
        bigquery.SchemaField("team_comp_home_count", "INTEGER", mode="NULLABLE", description="Visible Team Comparison metrics favoring home team."),
        bigquery.SchemaField("team_comp_neutral_count", "INTEGER", mode="NULLABLE", description="Visible Team Comparison metrics marked neutral."),
        bigquery.SchemaField("team_comp_total_visible", "INTEGER", mode="NULLABLE", description="Total visible Team Comparison metrics."),

        # ------------------------------------------------------------------
        # Simple engineered agreement features
        # These can be filled before combo formulas exist.
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "core_area_metric_count",
            "INTEGER",
            mode="NULLABLE",
            description="Number of metrics evaluated inside the claim's Core Area for this matchup.",
        ),
        bigquery.SchemaField(
            "same_direction_metric_count",
            "INTEGER",
            mode="NULLABLE",
            description="Number of related metrics in this group/Core Area that favored the claimed team.",
        ),
        bigquery.SchemaField(
            "opposing_signal_count",
            "INTEGER",
            mode="NULLABLE",
            description="Number of related metrics/signals that favored the opponent.",
        ),
        bigquery.SchemaField(
            "near_even_metric_count",
            "INTEGER",
            mode="NULLABLE",
            description="Number of related metrics considered near-even.",
        ),
        bigquery.SchemaField(
            "core_area_agreement_rate",
            "FLOAT",
            mode="NULLABLE",
            description="same_direction_metric_count / core_area_metric_count when denominator is available.",
        ),

        # ------------------------------------------------------------------
        # Combo/model feature scores
        # Keep NULL until formulas are implemented intentionally.
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "directional_edge_flag",
            "BOOLEAN",
            mode="NULLABLE",
            description="True when the claim indicates the claimed team is better than the opponent directionally.",
        ),
        bigquery.SchemaField(
            "elevated_candidate_flag",
            "BOOLEAN",
            mode="NULLABLE",
            description="True when pregame gap + league tier suggest elevated language might be justified.",
        ),
        bigquery.SchemaField(
            "strong_language_allowed_flag",
            "BOOLEAN",
            mode="NULLABLE",
            description="True when the claim has enough support to allow strong/elevated language.",
        ),
        bigquery.SchemaField(
            "offense_finish_score",
            "FLOAT",
            mode="NULLABLE",
            description="Future combo score for Offensive Output plus Scoring Efficiency. Leave NULL until formula exists.",
        ),
        bigquery.SchemaField(
            "defensive_suppression_score",
            "FLOAT",
            mode="NULLABLE",
            description="Future combo score for Defensive Control plus scoring suppression. Leave NULL until formula exists.",
        ),
        bigquery.SchemaField(
            "two_way_edge_score",
            "FLOAT",
            mode="NULLABLE",
            description="Future combo score for combined offensive and defensive paths. Leave NULL until formula exists.",
        ),
        bigquery.SchemaField(
            "disruption_upside_score",
            "FLOAT",
            mode="NULLABLE",
            description="Future combo score for pressure/turnover volatility or upside. Leave NULL until formula exists.",
        ),
        bigquery.SchemaField(
            "hidden_lean_score",
            "FLOAT",
            mode="NULLABLE",
            description="Future score for No Pick games where one side may still have a structural lean. Leave NULL until formula exists.",
        ),
        bigquery.SchemaField(
            "confidence_cap_reason",
            "STRING",
            mode="NULLABLE",
            description="Reason confidence/language should be capped, such as split_profile, weak_support, or volatility_only.",
        ),
        bigquery.SchemaField(
            "feature_formula_version",
            "STRING",
            mode="NULLABLE",
            description="Version label for combo feature formulas, once implemented.",
        ),

        # ------------------------------------------------------------------
        # Postgame actuals / validation labels
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "actual_team",
            "STRING",
            mode="NULLABLE",
            description="Team abbreviation that led the matching postgame metric/group validation.",
        ),
        bigquery.SchemaField(
            "actual_side",
            "STRING",
            mode="NULLABLE",
            description="Side that led the matching postgame metric/group validation: away, home, neutral, or unknown.",
        ),
        bigquery.SchemaField(
            "validation_result",
            "STRING",
            mode="NULLABLE",
            description="Claim validation result: validated, not_validated, actual_neutral_or_mixed, neutral_claim_but_actual_edge, or unavailable.",
        ),
        bigquery.SchemaField(
            "validated_flag",
            "BOOLEAN",
            mode="NULLABLE",
            description="True when validation_result is validated; false when not_validated; null for neutral/unavailable cases.",
        ),
        bigquery.SchemaField(
            "elevated_deserved_flag",
            "BOOLEAN",
            mode="NULLABLE",
            description="True when postgame result suggests elevated/strong language was deserved by actual gap strength.",
        ),
        bigquery.SchemaField("actual_gap", "FLOAT", mode="NULLABLE", description="Postgame actual raw gap when available."),
        bigquery.SchemaField("actual_rank_gap", "INTEGER", mode="NULLABLE", description="Postgame actual rank gap when available."),
        bigquery.SchemaField("actual_percentile_gap", "FLOAT", mode="NULLABLE", description="Postgame actual percentile gap when available."),
        bigquery.SchemaField("actual_gap_bucket", "STRING", mode="NULLABLE", description="Postgame gap bucket: near_even, small_edge, clear_edge, dominant_edge, or unknown."),

        # ------------------------------------------------------------------
        # Game result / QA labels
        # ------------------------------------------------------------------
        bigquery.SchemaField("predicted_team", "STRING", mode="NULLABLE", description="Team abbreviation selected by GameLens as matchup lean, if any."),
        bigquery.SchemaField("actual_winner", "STRING", mode="NULLABLE", description="Final-score winner, TIE for ties, or null if unavailable."),
        bigquery.SchemaField("model_result", "STRING", mode="NULLABLE", description="GameLens outcome result: Correct, Incorrect, No Pick, No Decision, or unavailable."),
        bigquery.SchemaField("is_tie", "BOOLEAN", mode="NULLABLE", description="True when final score was tied and model accuracy is not graded as win/loss."),
        bigquery.SchemaField("final_away_total", "INTEGER", mode="NULLABLE", description="Final away-team score."),
        bigquery.SchemaField("final_home_total", "INTEGER", mode="NULLABLE", description="Final home-team score."),
        bigquery.SchemaField("final_margin_abs", "INTEGER", mode="NULLABLE", description="Absolute final score margin."),
        bigquery.SchemaField("final_margin_bucket", "STRING", mode="NULLABLE", description="Final margin bucket, such as tie, close, one_score, material, severe, or unknown."),
        bigquery.SchemaField("qa_read_v2", "STRING", mode="NULLABLE", description="Level 2 QA game-level read, such as good_reasoning_correct_outcome or bad_reasoning_bad_outcome."),
        bigquery.SchemaField("headline_claim_validation_rate", "FLOAT", mode="NULLABLE", description="Game-level validation rate for headline claims."),
        bigquery.SchemaField("unique_claim_validation_rate", "FLOAT", mode="NULLABLE", description="Game-level validation rate for de-duplicated claims."),

        # ------------------------------------------------------------------
        # Audit
        # ------------------------------------------------------------------
        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this training example row was inserted.",
        ),
        bigquery.SchemaField(
            "updated_at",
            "TIMESTAMP",
            mode="NULLABLE",
            description="Timestamp when this training example row was last updated.",
        ),
    ]

    create_table(
        client=client,
        table_id="gamelens_claim_training_examples",
        schema=schema,
        description=(
            "Fine-grain GameLens claim training table. One row per extracted claim per game, "
            "combining pregame features, optional engineered combo scores, and postgame validation labels."
        ),
        partition_field="game_date",
        clustering_fields=["season", "game_id", "claim_type", "claim_layer"],
    )


def create_gamelens_feature_engineering_tables():
    client = bigquery.Client(project=PROJECT_ID)
    create_gamelens_claim_training_examples_table(client)


if __name__ == "__main__":
    create_gamelens_feature_engineering_tables()
