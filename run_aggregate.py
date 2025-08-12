from utils.logging_setup import setup_logging
from agg.aggregate_nfl_metrics_2025 import run_aggregate_for_season

setup_logging()
run_aggregate_for_season("2025")
