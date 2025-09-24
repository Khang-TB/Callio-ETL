"""Command line interface for the Callio ETL pipeline."""
from __future__ import annotations

import argparse

from .config import PipelineConfig
from .runner import CallioETLRunner


def main() -> None:
    config = PipelineConfig.from_env()
    runner = CallioETLRunner(config)

    parser = argparse.ArgumentParser(description="Run Callio ETL jobs")
    parser.add_argument(
        "--mode",
        choices=["daemon", "once"],
        default="once",
        help="daemon = infinite loop; once = run a single tick",
    )
    parser.add_argument(
        "--job",
        choices=["all", "customer", "call", "staffgroup"],
        default="all",
        help="Job to run when --mode=once",
    )
    args = parser.parse_args()

    if args.mode == "daemon":
        runner.run_forever()
    else:
        runner.run_once(args.job)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
