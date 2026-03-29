import cvxpy as cp
import numpy as np 
import polars as pl 

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem


class SchedulingModel:

    def __init__(self, problem: SchedulingProblem):
        pass