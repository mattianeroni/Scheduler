from __future__ import annotations

import pathlib
import polars as pl

import logging 
from typing import TYPE_CHECKING

from scheduler.plot.plotter import plot_solution

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem

logger = logging.Logger(__name__)


class SchedulingSolution:

    def __init__(self, 
        problem: SchedulingProblem, 
        tasks_df: pl.DataFrame, 
        assignments_df: pl.DataFrame
    ):
        self.problem = problem 
        self.tasks_df= tasks_df
        self.assignments_df = assignments_df

    def write(self, output_path: pathlib.Path):
        assignments = self.assignments_df.with_columns(pl.col.assignment_solution.round(0).cast(pl.Int64))
        tasks = self.tasks_df.with_columns(
            pl.col.start_solution.round(0).cast(pl.Int64),
            pl.col.end_solution.round(0).cast(pl.Int64),
        )
        assignments.write_csv(output_path / "assignments.csv")
        tasks.write_csv(output_path / "tasks.csv")
        plot_solution(assignments, tasks, output_path / "schedule.html")
