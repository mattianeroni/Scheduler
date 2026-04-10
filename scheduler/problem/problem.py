from __future__ import annotations

import polars as pl

import logging 
from typing import TYPE_CHECKING

from scheduler.error import SchedulerValidationError

if TYPE_CHECKING:
    from scheduler.config import Config
    from scheduler.io.resource import Resources
    from scheduler.io.task import Tasks
    from scheduler.io.group import ResourceGroups
    from scheduler.io.assignment import ResourceAssignments, GroupAssignments

logger = logging.getLogger(__name__)


class SchedulingProblem:

    def __init__(self, 
            config: Config,
            tasks: Tasks, 
            resources: Resources, 
            resource_groups: ResourceGroups, 
            resource_assignments: ResourceAssignments,
            group_assignments: GroupAssignments,
        ):
        self.config = config
        self.tasks = tasks
        self.resources = resources
        self.resource_groups = resource_groups
        self.resource_assignments = resource_assignments
        self.group_assignments = group_assignments

    def build(self):
        logger.info("Problem build started.")
        self._build_resources()
        self._build_tasks()
        self._build_resource_groups()
        self._build_resource_assignments()
        self._build_group_assignments()
        logger.info("Problem build concluded.")

    def validate(self):
        logger.info("Problem validation started.")
        self._validate_tasks_ending_time()
        self._validate_tasks_have_enough_resources()
        self._validate_group_tasks_missing_resource()
        self._validate_individual_tasks_forced_resources()
        self._validate_multiple_all_group_assignments()
        logger.info("Problem validation concluded.")

    def _build_resources(self):
        self.resources.df = self.resources.df.with_row_index("id")

    def _build_tasks(self):
        self.tasks.df = self.tasks.df.with_row_index("id")

    def _build_resource_groups(self):
        # Filter resource groups made by unexisting resources
        resource_groups_df = (
            self.resource_groups.df.explode("resource_name")
            .filter(pl.col.resource_name.is_in(self.resources.df["resource_name"]))
            .group_by("group_name").agg("resource_name")
        )
        if (delta := self.resource_groups.df.height - resource_groups_df.height) > 0:
            logger.warning(f"Resource groups eliminated because made by unexisting resources: {delta}")
        self.resource_groups.df = resource_groups_df.with_row_index("id")

    def _build_resource_assignments(self):
        columns = self.resource_assignments.df.columns
        # Filter resource assignments made by unexisting tasks or resources
        df = (
            self.resource_assignments.df.filter(
                (pl.col.task_name.is_in(self.tasks.df["task_name"]))
                & (pl.col.resource_name.is_in(self.resources.df["resource_name"]))
            )
        )
        if (delta := self.resource_assignments.df.height - df.height) > 0:
            logger.warning(f"Resource assignments eliminated because using unexisting tasks or resources: {delta}")

        # Filter resource assignmetns for which the capacity of the resource is not even enough to cover the entire task
        # NOTE: We don't allow assignment of multiple resources to complete a task
        resource_assignments_df = (
            df
            .join(self.tasks.df.select("task_name", "duration"), on="task_name", how="inner")
            .join(self.resources.df.select("resource_name", "capacity"), on="resource_name", how="inner")
            .filter(pl.col.capacity > pl.col.duration)
        )
        if (delta := df.height - resource_assignments_df.height) > 0:
            logger.warning(f"Resource assignments eliminated because using unexisting tasks or resources: {delta}")

        self.resource_assignments.df = resource_assignments_df.select(columns).with_row_index("id")

    def _build_group_assignments(self):
        # Filter groups assignments made by unexisting (or already filtered) groups or tasks
        group_assignments_df = (
            self.group_assignments.df.filter(
                (pl.col.task_name.is_in(self.tasks.df["task_name"]))
                & (pl.col.group_name.is_in(self.resource_groups.df["group_name"]))
            )
        )
        if group_assignments_df.height < self.group_assignments.df.height:
            delta = self.group_assignments.df.height - group_assignments_df.height
            logger.warning(f"Group assignments eliminated because using unexisting tasks or groups: {delta}")
        self.group_assignments.df = group_assignments_df.with_row_index("id")

    def _validate_tasks_ending_time(self):
        # Check no task is ending after time horizon
        overtime_tasks = self.tasks.df.filter((pl.col.end.is_not_null()) & (pl.col.end > self.config.timehorizon))
        if not overtime_tasks.is_empty():
            tasks = overtime_tasks["task_name"].unique().to_list()
            raise SchedulerValidationError(f"Detected tasks ending after time horizon ({self.config.timehorizon}): {tasks}")

    def _validate_tasks_have_enough_resources(self):
        # Check presence of tasks with no resources associated
        unsolvable_tasks = (
            self.tasks.df.join(self.resource_assignments.df, on="task_name", how="left")
            .group_by("task_name")
            .agg(n_resources = pl.col.resource_name.len())
            .filter(pl.col.n_resources == 0)
        )
        if not unsolvable_tasks.is_empty():
            tasks = unsolvable_tasks["task_name"].unique().to_list()
            raise SchedulerValidationError(f"Detected tasks with no resources associated: {tasks}")
        
    def _validate_group_tasks_missing_resource(self):
        # Check presence of tasks requiring an entire group for which one of resources association is missing
        group_tasks = (
            self.tasks.df.select("task_name")
            .join(
                (
                    self.group_assignments.df
                    .select("task_name", "group_name", "require_all_group")
                    .filter(pl.col.require_all_group)
                ),
                on="task_name",
                how="inner"
            )
            .join(self.resource_groups.df.select("group_name", "resource_name"), on="group_name", how="inner")
            .explode("resource_name")
            .join(
                self.resource_assignments.df.select("task_name", "resource_name"),
                on=["task_name", "resource_name"],
                how="anti"
            )
        )
        if not group_tasks.is_empty():
            tasks = group_tasks["task_name"].unique().to_list()
            raise SchedulerValidationError(f"Detected taks requiring an entire group for which we miss resources associations: {tasks}")
        
    def _validate_individual_tasks_forced_resources(self):
        # Check presence of individual tasks where the forced association require more than one resource 
        individual_tasks = (
            self.tasks.df.select("task_name")
            .join(
                self.resource_assignments.df.select("resource_name", "task_name", "type"), 
                on="task_name", 
                how="inner"
            )
            .filter(pl.col.type == "forced")
            .join(
                self.group_assignments.df.select("task_name"),
                on="task_name",
                how="anti"
            )
            .group_by("task_name")
            .agg(n_resources = pl.col.resource_name.len())
            .filter(pl.col.n_resources > 1)
        )
        if not individual_tasks.is_empty():
            tasks = individual_tasks["task_name"].unique().to_list()
            logger.warning(f"Detected individual tasks with more than 1 force-assigned resource: {tasks}")

    def _validate_multiple_all_group_assignments(self):
        # Check tasks requiring multiple entire groups (correct approach would be to create a new group)
        group_tasks = (
            self.tasks.df.select("task_name")
            .join(
                self.group_assignments.df.filter(pl.col.require_all_group).select("task_name", "group_name"),
                on="task_name",
                how="inner"
            )
            .group_by("task_name").agg(n_groups = pl.col.group_name.len())
            .filter(pl.col.n_groups > 1)
        )
        if not group_tasks.is_empty():
            tasks = group_tasks["task_name"].unique().to_list()
            logger.warning(f"Detected tasks requiring multiple entire groups: {tasks}. Please, consider to create a new group as union of the previous ones.")