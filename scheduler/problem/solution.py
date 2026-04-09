from __future__ import annotations

import pathlib
import polars as pl

import logging 
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem

logger = logging.Logger(__name__)


class SchedulingSolution:

    def __init__(self, 
        problem: SchedulingProblem, 
        tasks: pl.DataFrame, 
        assignments: pl.DataFrame
    ):
        self.problem = problem 
        self.tasks = tasks 
        self.assignments = assignments 

    def write(self, output_path: pathlib.Path):
        pass