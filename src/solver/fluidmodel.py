import cvxpy as cp
import numpy as np 
import polars as pl 

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.task.task import Task, FluidTask
    from src.problem.problem import SchedulingProblem 
    from src.resource.resource import Resource, ResourceGroup


class FluidSchedulingModel:

    def __init__(self, problem: SchedulingProblem):
        pass