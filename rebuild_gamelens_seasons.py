"""
Rebuild GameLens metric pipeline for multiple seasons.

Runs, in order:
1. agg.build_metric_facts
2. agg.build_windowed_metrics
3. agg.build_metric_rankings

Example:
    python scripts/rebuild_gamelens_seasons.py
"""

from __future__ import annotations

import subprocess
import sys


SEASONS = ["2024", "2023"]

COMMANDS = [
    [sys.executable, "-m", "agg.build_metric_facts", "--season", "{season}", "--if-exists", "replace"],
    [sys.executable, "-m", "agg.build_windowed_metrics", "--season", "{season}", "--if-exists", "replace"],
    [sys.executable, "-m", "agg.build_metric_rankings", "--season", "{season}", "--if-exists", "replace"],
]


def run_command(command: list[str]) -> None:
    print("\n" + "=" * 80)
    print("Running:", " ".join(command))
    print("=" * 80)

    result = subprocess.run(command)

    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}"
        )


def main() -> None:
    for season in SEASONS:
        print("\n" + "#" * 80)
        print(f"Starting GameLens rebuild for season {season}")
        print("#" * 80)

        for command_template in COMMANDS:
            command = [part.format(season=season) for part in command_template]
            run_command(command)

        print(f"\n✅ Completed GameLens rebuild for season {season}")

    print("\n🎉 All requested seasons rebuilt successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n❌ Rebuild stopped: {exc}", file=sys.stderr)
        sys.exit(1)