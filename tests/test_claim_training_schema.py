from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from agg.gamelens_training import create_claim_training_examples_table as tables


def _signature(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def test_canonical_claim_schema_has_unique_field_names():
    schema = tables.claim_training_examples_schema()
    names = [field.name for field in schema]

    assert names
    assert len(names) == len(set(names))
    assert {
        "claim_key",
        "run_id",
        "game_id",
        "claim_type",
        "claim_layer",
    } <= set(names)
    assert {"validation_result", "validated_flag", "created_at"} <= set(names)


def test_historical_table_creator_reuses_canonical_schema(monkeypatch):
    recorded = {}

    def record_create_table(**kwargs):
        recorded.update(kwargs)

    monkeypatch.setattr(tables, "create_table", record_create_table)

    tables.create_gamelens_claim_training_examples_table(client=object())

    assert recorded["table_id"] == "gamelens_claim_training_examples"
    assert _signature(recorded["schema"]) == _signature(
        tables.claim_training_examples_schema()
    )
