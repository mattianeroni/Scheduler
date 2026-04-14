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

            if self._model.status not in ("optimal", "optimal_inaccurate"):
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
        self._variables["resource_overlap"] = cp.Variable(self.problem.resource_assignment_combinations.df.height, boolean=True)
        self._variables["time_overlap"] = cp.Variable(self.problem.resource_assignment_combinations.df.height, boolean=True)
        self._variables["overlap"] = cp.Variable(self.problem.resource_assignment_combinations.df.height, boolean=True)
        # Auxiliary variable for the disjunction in the no-overlap case:
        # order = 1 means task_left finishes before task_right starts
        # order = 0 means task_right finishes before task_left starts
        self._variables["order"] = cp.Variable(self.problem.resource_assignment_combinations.df.height, boolean=True)

    def _build_objective(self):
        assignments =  cp.sum(self._variables["assignment"])
        overlaps = cp.sum(self._variables["overlap"]) * self.problem.config.overlap_penalization
        self._objective = cp.Minimize(assignments + overlaps)

    def _build_constraints(self):
        self._time_overlap_constraints()
        self._resource_overlap_constraints()
        self._overlap_constraints()
        self._rigid_tasks_constraints()
        self._tasks_duration_constraints()
        self._tasks_lead_time_constaints()
        self._all_tasks_assigned_constraints() 
        self._forced_assignments_constraints()

    def _time_overlap_constraints(self):
        M = self.problem.config.timehorizon
        comb = (
            self.problem.resource_assignment_combinations.df.join(
                self.problem.tasks.df.select(
                    pl.col("id").alias("task_left_id"), 
                    pl.col("task_name").alias("task_name_left"),
                    pl.col("type").alias("type_left"),
                    pl.col("start").alias("start_left"),
                    pl.col("end").alias("end_left"),
                ),
                on="task_name_left",
                how="inner",
            )
            .join(
                self.problem.tasks.df.select(
                    pl.col("id").alias("task_right_id"), 
                    pl.col("task_name").alias("task_name_right"),
                    pl.col("type").alias("type_right"),
                    pl.col("start").alias("start_right"),
                    pl.col("end").alias("end_right"),
                ),
                on="task_name_right",
                how="inner",
            )
        )
        overlap_vars = get_variables(self._variables["time_overlap"], comb["id"].to_numpy())
        order_vars = get_variables(self._variables["order"], comb["id"].to_numpy())
        s1 = get_variables(self._variables["task_start"], comb["task_left_id"].to_numpy())
        s2 = get_variables(self._variables["task_start"], comb["task_right_id"].to_numpy())
        e1 = get_variables(self._variables["task_end"], comb["task_left_id"].to_numpy())
        e2 = get_variables(self._variables["task_end"], comb["task_right_id"].to_numpy())
        # When overlap_vars = 1 (tasks overlap in time): s1 <= e2 and s2 <= e1
        self._constraints.append(s1 <= e2 + M * (1 - overlap_vars))
        self._constraints.append(s2 <= e1 + M * (1 - overlap_vars))
        # When overlap_vars = 0 (tasks don't overlap): EITHER e1 <= s2 OR e2 <= s1
        # We use order_vars to select which side of the disjunction holds:
        #   order = 1 → e1 <= s2  (left finishes before right starts)
        #   order = 0 → e2 <= s1  (right finishes before left starts)
        self._constraints.append(e1 <= s2 + M * (overlap_vars + (1 - order_vars)))
        self._constraints.append(e2 <= s1 + M * (overlap_vars + order_vars))
        
    def _resource_overlap_constraints(self):
        """Ensure the resource overlap variables are 1 when both tasks use same resources and 0 otherwise."""
        combinations_ids = self.problem.resource_assignment_combinations.df["id"].to_numpy()
        first_assignment_ids = self.problem.resource_assignment_combinations.df["assignment_left_id"].to_numpy()
        second_assignment_ids = self.problem.resource_assignment_combinations.df["assignment_right_id"].to_numpy()
        first_assignment_vars = get_variables(self._variables["assignment"], first_assignment_ids)
        second_assignment_vars = get_variables(self._variables["assignment"], second_assignment_ids)
        resource_overlap_vars = get_variables(self._variables["resource_overlap"], combinations_ids)
        self._constraints.append(resource_overlap_vars + 1 >= first_assignment_vars + second_assignment_vars)

    def _overlap_constraints(self):
        """Drive the overlap variable to be 1 when resource_overlap AND time_overlap are both 1."""
        self._constraints.append(self._variables["overlap"] + 1 >= self._variables["resource_overlap"] + self._variables["time_overlap"])

    def _rigid_tasks_constraints(self):
        """Rigid tasks must start when user says."""
        rigid_tasks = self.problem.tasks.df.filter(pl.col.type == "rigid").select("id", "start")
        if rigid_tasks.is_empty():
            return 
        start_vars = get_variables(self._variables["task_start"], rigid_tasks["id"].to_numpy())
        self._constraints.append(start_vars == rigid_tasks["start"].to_numpy())

    def _tasks_duration_constraints(self):
        """Tasks end must equal start + duration."""
        tasks = self.problem.tasks.df.select("id", "duration")
        start_vars = get_variables(self._variables["task_start"], tasks["id"].to_numpy())
        end_vars = get_variables(self._variables["task_end"], tasks["id"].to_numpy())
        self._constraints.append(start_vars + tasks["duration"].to_numpy() == end_vars)  

    def _tasks_lead_time_constaints(self):
        """Tasks must be concluded before end of time horizon."""
        self._constraints.append(self._variables["task_end"] <= self.problem.config.timehorizon)

    def _all_tasks_assigned_constraints(self):
        """Each task is assigned to at least a resource."""
        task_to_assignment = self.problem.resource_assignments.df.group_by("task_name").agg(pl.col.id)
        self._constraints.append(get_variables(self._variables["assignment"], task_to_assignment["id"].to_numpy()) >= 1)

    def _forced_assignments_constraints(self):
        """Forced assignments are respected."""
        forced_assignments_id = self.problem.resource_assignments.df.filter(pl.col.type == "forced")["id"].to_numpy()
        if len(forced_assignments_id) == 0:
            return
        assignemnts_vars = get_variables(self._variables["assignment"], forced_assignments_id)
        self._constraints.append(assignemnts_vars == 1)


