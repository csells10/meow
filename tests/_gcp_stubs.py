"""Minimal import stubs for local tests when Google Cloud SDK is absent."""

import sys
import types


def install_bigquery_stub():
    try:
        from google.cloud import bigquery
    except ImportError:
        bigquery = types.ModuleType("google.cloud.bigquery")

    class Client:
        def __init__(self, *args, **kwargs):
            pass

    class QueryJobConfig:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class LoadJobConfig:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class QueryParameter:
        def __init__(self, *args):
            self.args = args

    class SchemaField:
        def __init__(self, name, field_type, mode="NULLABLE", **kwargs):
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class Dataset:
        def __init__(self, dataset_id):
            self.dataset_id = dataset_id
            self.location = None

    class Table:
        def __init__(self, table_id, schema=None):
            self.table_id = table_id
            self.schema = list(schema or [])
            self.time_partitioning = None
            self.clustering_fields = None

    class TimePartitioning:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class TimePartitioningType:
        DAY = "DAY"

    class WriteDisposition:
        WRITE_TRUNCATE = "WRITE_TRUNCATE"

    defaults = {
        "Client": Client,
        "QueryJobConfig": QueryJobConfig,
        "LoadJobConfig": LoadJobConfig,
        "ScalarQueryParameter": QueryParameter,
        "ArrayQueryParameter": QueryParameter,
        "SchemaField": SchemaField,
        "Dataset": Dataset,
        "Table": Table,
        "TimePartitioning": TimePartitioning,
        "TimePartitioningType": TimePartitioningType,
        "WriteDisposition": WriteDisposition,
    }
    for name, value in defaults.items():
        if not hasattr(bigquery, name):
            setattr(bigquery, name, value)

    google = sys.modules.setdefault("google", types.ModuleType("google"))
    cloud = sys.modules.setdefault(
        "google.cloud", types.ModuleType("google.cloud")
    )
    cloud.bigquery = bigquery
    google.cloud = cloud
    sys.modules["google.cloud.bigquery"] = bigquery
