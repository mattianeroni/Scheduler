from __future__ import annotations

from pathlib import Path

import polars as pl

from scheduler.plot import plot_solution


def main() -> None:
    output_dir = Path(__file__).parent / "test" / "output"
    assignments_file = output_dir / "assignments.csv"
    tasks_file = output_dir / "tasks.csv"
    html_file = output_dir / "schedule.html"

    if not assignments_file.exists():
        raise FileNotFoundError(f"Missing assignments file: {assignments_file}")
    if not tasks_file.exists():
        raise FileNotFoundError(f"Missing tasks file: {tasks_file}")

    assignments_df = pl.read_csv(assignments_file)
    tasks_df = pl.read_csv(tasks_file)

    plot_path = plot_solution(assignments_df=assignments_df, tasks_df=tasks_df, output_path=html_file)
    print(f"Schedule plot written to: {plot_path}")


if __name__ == "__main__":
    main()
