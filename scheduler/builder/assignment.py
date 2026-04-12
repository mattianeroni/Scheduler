from __future__ import annotations

import logging
import polars as pl

from scheduler.reader.assignment import ResourceAssignmentsReader, GroupAssignmentsReader
from scheduler.builder.task import TasksBuilder
from scheduler.builder.resource import ResourcesBuilder
from scheduler.builder.group import ResourceGroupsBuilder

logger = logging.getLogger(__name__)
    

class ResourceAssignmentsBuilder:

    def __init__(self, resource_assignments_reader: ResourceAssignmentsReader, tasks: TasksBuilder, resources: ResourcesBuilder):
        columns = resource_assignments_reader.df.columns
        # Filter resource assignments made by unexisting tasks or resources
        _df = (
            resource_assignments_reader.df.filter(
                (pl.col.task_name.is_in(tasks.df["task_name"]))
                & (pl.col.resource_name.is_in(resources.df["resource_name"]))
            )
        )
        if (delta := resource_assignments_reader.df.height - _df.height) > 0:
            logger.warning(f"Resource assignments eliminated because using unexisting tasks or resources: {delta}")

        # Filter resource assignmetns for which the capacity of the resource is not even enough to cover the entire task
        # NOTE: We don't allow assignment of multiple resources to complete a task
        df = (
            _df
            .join(tasks.df.select("task_name", "duration"), on="task_name", how="inner")
            .join(resources.df.select("resource_name", "capacity"), on="resource_name", how="inner")
            .filter(pl.col.capacity > pl.col.duration)
        )
        if (delta := _df.height - df.height) > 0:
            logger.warning(f"Resource assignments eliminated because using unexisting tasks or resources: {delta}")

        self.df = df.select(columns).with_row_index("id")
    

class GroupAssignmentsBuilder:

    def __init__(self, group_assignments_reader: GroupAssignmentsReader, tasks: TasksBuilder, resource_groups: ResourceGroupsBuilder):
        # Filter groups assignments made by unexisting (or already filtered) groups or tasks
        df = (
            group_assignments_reader.df.filter(
                (pl.col.task_name.is_in(tasks.df["task_name"]))
                & (pl.col.group_name.is_in(resource_groups.df["group_name"]))
            )
        )
        if (delta := group_assignments_reader.df.height - df.height) > 0:
            logger.warning(f"Group assignments eliminated because using unexisting tasks or groups: {delta}")
        self.df = df.with_row_index("id")


class ResourceAssignmentCombinationsBuilder:

    def __init__(self, resource_assignments: ResourceAssignmentsBuilder):
        self.df = (
            resource_assignments.df.join(resource_assignments.df, how="cross")
            .rename({
                "id": "assignment_left_id", 
                "id_right": "assignment_right_id",
                "resource_name": "resource_name_left",
                "task_name": "task_name_left",
                "type": "type_left"
            })
            .filter(
                (pl.col.resource_name_left == pl.col.resource_name_right) 
                & (pl.col.task_name_left != pl.col.task_name_right)
            )
        ).with_row_index("id")