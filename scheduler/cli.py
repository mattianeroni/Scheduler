from __future__ import annotations

import argparse
import sys
import textwrap
from pathlib import Path

from scheduler.error import SchedulerIOError, SchedulerValidationError, SchedulerModelError
from scheduler.scheduler import Scheduler


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(
            """
            Run the Scheduler using the input folder containing configuration and data files.

            Expected input directory structure:
              - config.toml
              - tasks.csv
              - resources.csv
              - groups.csv
              - resource_assignments.csv
              - group_assignments.csv

            The output folder will be created if it does not already exist.
            """
        )
    )
    parser.add_argument(
        "--input-path",
        default="./input",
        help="Path to the input folder containing Scheduler input files (default: ./input)",
    )
    parser.add_argument(
        "--output-path",
        default="./output",
        help="Path to the output folder where logs/results are written (default: ./output)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)

    scheduler = Scheduler(args.input_path, args.output_path)

    try:
        scheduler.run()
    except SchedulerIOError:
        return 3
    except SchedulerValidationError:
        return 4
    except SchedulerModelError:
        return 5
    except Exception:
        return 1
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
