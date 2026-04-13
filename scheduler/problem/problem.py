from __future__ import annotations

import polars as pl

import logging 
from typing import TYPE_CHECKING

from scheduler.error import SchedulerValidationError
from scheduler.builder.task import TasksBuilder
from scheduler.builder.resource import ResourcesBuilder
from scheduler.builder.group import ResourceGroupsBuilder
from scheduler.builder.assignment import ResourceAssignmentsBuilder, GroupAssignmentsBuilder, ResourceAssignmentCombinationsBuilder

if TYPE_CHECKING:
    from scheduler.config import Config
    from scheduler.reader.resource import ResourcesReader
    from scheduler.reader.task import TasksReader
    from scheduler.reader.group import ResourceGroupsReader
    from scheduler.reader.assignment import ResourceAssignmentsReader, GroupAssignmentsReader

logger = logging.getLogger(__name__)


class SchedulingProblem:

    def __init__(self, 
            config: Config,
            tasks_reader: TasksReader, 
            resources_reader: ResourcesReader, 
            resource_groups_reader: ResourceGroupsReader, 
            resource_assignments_reader: ResourceAssignmentsReader,
            group_assignments_reader: GroupAssignmentsReader,
        ):
        self.config = config
        self.tasks_reader = tasks_reader
        self.resources_reader = resources_reader
        self.resource_groups_reader = resource_groups_reader
        self.resource_assignments_reader = resource_assignments_reader
        self.group_assignments_reader = group_assignments_reader

        self.tasks_df: pl.DataFrame | None = None 
        self.resources_df: pl.DataFrame | None = None 
        self.resource_groups_df: pl.DataFrame | None = None
        self.resource_assignments_df: pl.DataFrame | None = None
        self.group_assignments_df: pl.DataFrame | None = None 
        self.resource_assignment_combinations_df: pl.DataFrame | None = None

    def build(self):
        logger.info("Problem build started.")
        self.resources_df = ResourcesBuilder.build(resources_reader=self.resources_reader)
        self.tasks_df = TasksBuilder.build(tasks_reader=self.tasks_reader)
        self.resource_groups_df = ResourceGroupsBuilder.build(
            resource_groups_reader=self.resource_groups_reader, 
            resources_df=self.resources_df,
        )
        self.resource_assignments_df = ResourceAssignmentsBuilder.build(
            resource_assignments_reader=self.resource_assignments_reader, 
            tasks_df=self.tasks_df, 
            resources_df=self.resources_df,
        )
        self.group_assignments_df = GroupAssignmentsBuilder.build(
            group_assignments_reader=self.group_assignments_reader,
            tasks_df=self.tasks_df,
            resource_groups_df=self.resource_groups_df,
        )
        self.resource_assignment_combinations_df = ResourceAssignmentCombinationsBuilder.build(
            resource_assignments_df=self.resource_assignments_df
        )
        logger.info("Problem build concluded.")

    def validate(self):
        logger.info("Problem validation started.")
        self._validate_tasks_ending_time()
        self._validate_tasks_have_enough_resources()
        self._validate_group_tasks_missing_resource()
        self._validate_individual_tasks_forced_resources()
        self._validate_multiple_all_group_assignments()
        logger.info("Problem validation concluded.")

    def _validate_tasks_ending_time(self):
        # Check no task is ending after time horizon
        overtime_tasks = self.tasks_df.filter((pl.col.end.is_not_null()) & (pl.col.end > self.config.timehorizon))
        if not overtime_tasks.is_empty():
            tasks = overtime_tasks["task_name"].unique().to_list()
            raise SchedulerValidationError(f"Detected tasks ending after time horizon ({self.config.timehorizon}): {tasks}")

    def _validate_tasks_have_enough_resources(self):
        # Check presence of tasks with no resources associated
        unsolvable_tasks = (
            self.tasks_df.join(self.resource_assignments_df, on="task_name", how="left")
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
            self.tasks_df.select("task_name")
            .join(
                (
                    self.group_assignments_df
                    .select("task_name", "group_name", "require_all_group")
                    .filter(pl.col.require_all_group)
                ),
                on="task_name",
                how="inner"
            )
            .join(self.resource_groups_df.select("group_name", "resource_name"), on="group_name", how="inner")
            .explode("resource_name")
            .join(
                self.resource_assignments_df.select("task_name", "resource_name"),
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
            self.tasks_df.select("task_name")
            .join(
                self.resource_assignments_df.select("resource_name", "task_name", "type"), 
                on="task_name", 
                how="inner"
            )
            .filter(pl.col.type == "forced")
            .join(
                self.group_assignments_df.select("task_name"),
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
            self.tasks_df.select("task_name")
            .join(
                self.group_assignments_df.filter(pl.col.require_all_group).select("task_name", "group_name"),
                on="task_name",
                how="inner"
            )
            .group_by("task_name").agg(n_groups = pl.col.group_name.len())
            .filter(pl.col.n_groups > 1)
        )
        if not group_tasks.is_empty():
            tasks = group_tasks["task_name"].unique().to_list()
            logger.warning(f"Detected tasks requiring multiple entire groups: {tasks}. Please, consider to create a new group as union of the previous ones.")