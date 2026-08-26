"""Minimal BigQuery import stubs for focused local unit tests."""

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

    class QueryParameter:
        def __init__(self, *args):
            self.args = args

    defaults = {
        "Client": Client,
        "QueryJobConfig": QueryJobConfig,
        "ScalarQueryParameter": QueryParameter,
        "ArrayQueryParameter": QueryParameter,
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
