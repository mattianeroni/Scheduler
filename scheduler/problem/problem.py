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

        self.tasks: TasksBuilder | None = None 
        self.resources: ResourcesBuilder | None = None 
        self.resource_groups: ResourceGroupsBuilder | None = None
        self.resource_assignments: ResourceAssignmentsBuilder | None = None
        self.group_assignments: GroupAssignmentsBuilder | None = None 
        self.resource_assignment_combinations: ResourceAssignmentCombinationsBuilder | None = None

    def build(self):
        logger.info("Problem build started.")
        self.resources = ResourcesBuilder(resources_reader=self.resources_reader)
        self.tasks = TasksBuilder(tasks_reader=self.tasks_reader)
        self.resource_groups = ResourceGroupsBuilder(
            resource_groups_reader=self.resource_groups_reader, 
            resources=self.resources,
        )
        self.resource_assignments = ResourceAssignmentsBuilder(
            resource_assignments_reader=self.resource_assignments_reader, 
            tasks=self.tasks, 
            resources=self.resources,
        )
        self.group_assignments = GroupAssignmentsBuilder(
            group_assignments_reader=self.group_assignments_reader,
            tasks=self.tasks,
            resource_groups=self.resource_groups,
        )
        self.resource_assignment_combinations = ResourceAssignmentCombinationsBuilder(
            resource_assignments=self.resource_assignments
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