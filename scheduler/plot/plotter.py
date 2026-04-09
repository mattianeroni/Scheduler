from __future__ import annotations 

import polars as pl
import matplotlib.pyplot as plt

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem


def plot_solution(problem: SchedulingProblem):
    pass