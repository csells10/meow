from google.cloud import bigquery


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"


def create_table(client: bigquery.Client, table_id: str, schema: list[bigquery.SchemaField]):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")
    except Exception as e:
        print(f"⚠️ Failed to create table {table_ref}: {e}")


def create_model_outcomes_table(client: bigquery.Client):
    schema = [
        bigquery.SchemaField(
            "game_id",
            "STRING",
            mode="REQUIRED",
            description="Join key. Unique game identifier in format YYYYMMDD_AWAY@HOME."
        ),
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year, such as 2025."
        ),
        bigquery.SchemaField(
            "game_week",
            "STRING",
            mode="REQUIRED",
            description="Week of the NFL season, such as 'Week 10', 'Wildcard', or 'Super Bowl'."
        ),

        # Core prediction outcome
        bigquery.SchemaField(
            "predicted_team",
            "STRING",
            mode="NULLABLE",
            description="Team abbreviation the model selected as having the matchup edge."
        ),
        bigquery.SchemaField(
            "predicted_side",
            "STRING",
            mode="NULLABLE",
            description="Predicted team side in this game: 'away', 'home', or null for No Pick."
        ),
        bigquery.SchemaField(
            "actual_winner",
            "STRING",
            mode="NULLABLE",
            description="Team abbreviation that actually won the game based on final score."
        ),
        bigquery.SchemaField(
            "result",
            "STRING",
            mode="REQUIRED",
            description="Human-readable prediction result: 'Correct', 'Incorrect', or 'No Pick'."
        ),
        bigquery.SchemaField(
            "result_code",
            "STRING",
            mode="REQUIRED",
            description="Normalized prediction result code: 'correct', 'incorrect', or 'no_pick'."
        ),

        # Confidence
        bigquery.SchemaField(
            "confidence",
            "STRING",
            mode="NULLABLE",
            description="Human-readable model confidence level: 'High', 'Medium', or 'Low'."
        ),
        bigquery.SchemaField(
            "confidence_tier",
            "STRING",
            mode="NULLABLE",
            description="Normalized confidence tier: 'high', 'medium', 'low', or 'unknown'."
        ),
        bigquery.SchemaField(
            "confidence_context",
            "STRING",
            mode="NULLABLE",
            description="Additional context affecting confidence, such as early-season sample size limits."
        ),

        # Model trust summary
        bigquery.SchemaField(
            "edge_strength",
            "STRING",
            mode="NULLABLE",
            description="Model-rated matchup edge strength: 'strong', 'moderate', or 'low'."
        ),
        bigquery.SchemaField(
            "edge_score",
            "FLOAT",
            mode="NULLABLE",
            description="Numeric model edge score used to support edge strength. Higher means stronger edge."
        ),
        bigquery.SchemaField(
            "signal_alignment_code",
            "STRING",
            mode="NULLABLE",
            description="Normalized signal alignment result: 'strong', 'mixed', 'weak', or 'unknown'."
        ),
        bigquery.SchemaField(
            "aligned_signal_count",
            "INTEGER",
            mode="NULLABLE",
            description="Number of evaluated signals that aligned with the model's predicted side."
        ),
        bigquery.SchemaField(
            "total_signal_count",
            "INTEGER",
            mode="NULLABLE",
            description="Total number of evaluated signals used in signal alignment."
        ),

        # Matchup advantage summary
        bigquery.SchemaField(
            "matchup_advantage_away",
            "INTEGER",
            mode="NULLABLE",
            description="Count of matchup comparison factors favoring the away team."
        ),
        bigquery.SchemaField(
            "matchup_advantage_home",
            "INTEGER",
            mode="NULLABLE",
            description="Count of matchup comparison factors favoring the home team."
        ),
        bigquery.SchemaField(
            "matchup_advantage_leader",
            "STRING",
            mode="NULLABLE",
            description="Side with the matchup advantage: 'away', 'home', or 'tie'."
        ),

        # Learning / review
        bigquery.SchemaField(
            "reason_tag",
            "STRING",
            mode="NULLABLE",
            description="Short model review tag explaining the outcome or miss pattern."
        ),

        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this model outcome record was inserted."
        ),
    ]

    create_table(client, "game_model_outcomes", schema)


def create_model_trust_details_table(client: bigquery.Client):
    schema = [
        bigquery.SchemaField(
            "game_id",
            "STRING",
            mode="REQUIRED",
            description="Join key. Unique game identifier in format YYYYMMDD_AWAY@HOME."
        ),
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year, such as 2025."
        ),
        bigquery.SchemaField(
            "game_week",
            "STRING",
            mode="REQUIRED",
            description="Week of the NFL season, such as 'Week 10', 'Wildcard', or 'Super Bowl'."
        ),

        bigquery.SchemaField(
            "section",
            "STRING",
            mode="REQUIRED",
            description="Model trust subsection this row belongs to, such as 'reasoning_driver', 'signal_alignment', or 'game_driver'."
        ),
        bigquery.SchemaField(
            "category",
            "STRING",
            mode="NULLABLE",
            description="Signal or metric category, such as 'Pressure', 'Turnover Margin', 'Scoring', or 'Red Zone'."
        ),

        bigquery.SchemaField(
            "team_side",
            "STRING",
            mode="NULLABLE",
            description="Team side associated with this detail row: 'away', 'home', or null if not team-specific."
        ),
        bigquery.SchemaField(
            "favored_side",
            "STRING",
            mode="NULLABLE",
            description="Side favored by this signal or detail: 'away', 'home', or null if neutral."
        ),
        bigquery.SchemaField(
            "aligns",
            "STRING",
            mode="NULLABLE",
            description="Whether this detail aligned with the model pick: 'yes', 'no', or 'neutral'."
        ),

        bigquery.SchemaField(
            "label",
            "STRING",
            mode="NULLABLE",
            description="Short display label for the detail row, such as 'Pressure' or 'Red Zone TD %'."
        ),
        bigquery.SchemaField(
            "sentence",
            "STRING",
            mode="NULLABLE",
            description="Backend-authored explanation sentence for frontend display."
        ),
        bigquery.SchemaField(
            "gap",
            "STRING",
            mode="NULLABLE",
            description="Formatted comparison gap, such as '42.1% vs 35.8%' or '+0.8 vs -0.2'."
        ),
        bigquery.SchemaField(
            "impact",
            "STRING",
            mode="NULLABLE",
            description="Optional impact level for this detail: 'high', 'medium', 'low', or null."
        ),

        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this model trust detail record was inserted."
        ),
    ]

    create_table(client, "game_model_trust_details", schema)


def create_model_feedback_tables():
    client = bigquery.Client(project=PROJECT_ID)

    create_model_outcomes_table(client)
    create_model_trust_details_table(client)


if __name__ == "__main__":
    create_model_feedback_tables()