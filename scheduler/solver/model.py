import cvxpy as cp
import numpy as np 
import polars as pl 

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem

from scheduler.problem.solution import SchedulingSolution


class SchedulingModel:

    def __init__(self, problem: SchedulingProblem):
        self.problem = problem
        self.model: cp.Problem | None = None
        self.solution: SchedulingSolution | None = None

    def build(self):
        pass 

    def solve(self):
        pass