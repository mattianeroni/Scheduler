from __future__ import annotations

import cvxpy as cp
import numpy as np 
import polars as pl 

import logging

from cvxpy.expressions.expression import Expression
from cvxpy.expressions.variable import Variable
from cvxpy.problems.objective import Maximize, Minimize
Objective = Maximize | Minimize

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.problem.problem import SchedulingProblem

from scheduler.problem.solution import SchedulingSolution
from scheduler.error import SchedulerModelError
from scheduler.solver.utils import get_variables

logger = logging.getLogger(__name__)


class SchedulingModel:

    def __init__(self, problem: SchedulingProblem):
        self.problem = problem
        self.solution: SchedulingSolution | None = None

        self._variables: dict[str, Variable] = {}
        self._constraints = []
        self._objective: Objective | None = None
        self._model: cp.Problem | None = None

    def build(self):
        logger.info("Model build started.")
        self._variables = {}
        self._constraints = []
        self._build_variables()
        self._build_constraints()
        self._build_objective()
        self._model = cp.Problem(self._objective, self._constraints)
        logger.info("Model build concluded.")

    def solve(self):
        logger.info("Model solve started.")
        try:
            # Build scip_params, only including non-None values
            scip_params = {}
            if self.problem.config.max_time is not None:
                scip_params["limits/time"] = self.problem.config.max_time
            if self.problem.config.optimization_gap is not None:
                scip_params["limits/gap"] = self.problem.config.optimization_gap
            
            self._model.solve(
                solver = cp.SCIP, 
                verbose = self.problem.config.verbose, 
                scip_params=scip_params
            )
        
            if self._model.status is None:
                raise SchedulerModelError("Solver failed to return a status (Internal Error).")
            
            if self._model.status != "optimal":
                raise SchedulerModelError(f"Solver did not find an optimal solution. Status: {self._model.status}")
            
        except Exception as e:
            raise SchedulerModelError(f"An unexpected error occurred: {str(e)}")
        
        self.solution = SchedulingSolution(
            problem = self.problem,
            tasks_df = self.problem.tasks.df.with_columns(
                start_solution = self._variables["task_start"].value[self.problem.tasks.df["id"].to_numpy()],
                end_solution = self._variables["task_end"].value[self.problem.tasks.df["id"].to_numpy()],
            ),
            assignments_df=self.problem.resource_assignments.df.with_columns(
                assignment_solution = self._variables["assignment"].value[self.problem.resource_assignments.df["id"].to_numpy()]
            )
        )
        logger.info("Model solve concluded.")
        return self.solution
        
    def _build_variables(self):
        self._variables["assignment"] = cp.Variable(self.problem.resource_assignments.df.height, boolean=True)
        self._variables["task_start"] = cp.Variable(self.problem.tasks.df.height, integer=True, nonneg=True)
        self._variables["task_end"] = cp.Variable(self.problem.tasks.df.height, integer=True, nonneg=True)

    def _build_objective(self):
        self._objective = cp.Minimize(cp.sum(self._variables["assignment"]) + cp.sum(self._variables["task_start"] + self._variables["task_end"]))

    def _build_constraints(self):
        self._rigid_tasks_constraints()
        self._tasks_duration_constraints()
        self._tasks_lead_time_constaints()

    def _rigid_tasks_constraints(self):
        """Rigid tasks must start when user says."""
        rigid_tasks = self.problem.tasks.df.filter(pl.col.type == "rigid").select("id", "start")
        if rigid_tasks.is_empty():
            return 
        
        start_vars = get_variables(self._variables["task_start"], rigid_tasks["id"].to_numpy())
        self._constraints.append(start_vars == rigid_tasks["start"].to_numpy())

    def _tasks_duration_constraints(self):
        """Tasks end must be equal to start + duration."""
        tasks = self.problem.tasks.df.select("id", "duration")
        start_vars = get_variables(self._variables["task_start"], tasks["id"].to_numpy())
        end_vars = get_variables(self._variables["task_end"], tasks["id"].to_numpy())
        self._constraints.append(start_vars + tasks["duration"].to_numpy() <= end_vars)  # Eventually set equal

    def _tasks_lead_time_constaints(self):
        """Tasks must be concluded before end of time horizon."""
        self._constraints.append(self._variables["task_end"] <= self.problem.config.timehorizon)


        


