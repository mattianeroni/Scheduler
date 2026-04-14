from __future__ import annotations

import logging

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.builder.base import BaseBuilder
from scheduler.reader.assignment import ResourceAssignmentsReader, GroupAssignmentsReader
from scheduler.builder.task import TasksBuilder
from scheduler.builder.resource import ResourcesBuilder
from scheduler.builder.group import ResourceGroupsBuilder

logger = logging.getLogger(__name__)


class ResourceAssignmentBuilderSchema(pa.DataFrameModel):
    id: Series[int] = pa.Field(coerce=True, unique=True)
    task_name: Series[str] = pa.Field(coerce=True)
    resource_name: Series[str] = pa.Field(coerce=True)
    type: Series[str] = pa.Field(coerce=True, isin=["forced", "relaxed"])

    @pa.dataframe_check(error="Assignments Task-Resource id must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("id").collect().height == data.lazyframe.collect().height
    
class GroupAssignmentBuilderSchema(pa.DataFrameModel):
    id: Series[int] = pa.Field(coerce=True, unique=True)
    task_name: Series[str] = pa.Field(coerce=True)
    group_name: Series[str] = pa.Field(coerce=True)
    require_all_group: Series[bool] = pa.Field(coerce=True)
    
    @pa.dataframe_check(error="Assignments Task-Group id must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("id").collect().height == data.lazyframe.collect().height

class ResourceAssignmentCombinationBuilderSchema(pa.DataFrameModel):
    id: Series[int] = pa.Field(coerce=True, unique=True)
    resource_name: Series[str] = pa.Field(coerce=True)
    assignment_left_id: Series[int] = pa.Field(coerce=True)
    assignment_right_id: Series[int] = pa.Field(coerce=True)
    task_name_left: Series[str] = pa.Field(coerce=True)
    task_name_right: Series[str] = pa.Field(coerce=True)
    type_left: Series[str] = pa.Field(coerce=True, isin=["rigid", "fluid"])
    type_right: Series[str] = pa.Field(coerce=True, isin=["rigid", "fluid"])

    @pa.dataframe_check(error="Assignments combinations id must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("id").collect().height == data.lazyframe.collect().height
    



class ResourceAssignmentsBuilder(BaseBuilder):

    def __init__(self, resource_assignments_reader: ResourceAssignmentsReader, tasks: TasksBuilder, resources: ResourcesBuilder):
        self.tasks = tasks 
        self.resources = resources 
        super().__init__(df=resource_assignments_reader.df, schema=ResourceAssignmentBuilderSchema)
    
    def _build(self, df: pl.DataFrame) -> pl.DataFrame:
        columns = df.columns
        # Filter resource assignments made by unexisting tasks or resources
        _df = (
            df.filter(
                (pl.col.task_name.is_in(self.tasks.df["task_name"]))
                & (pl.col.resource_name.is_in(self.resources.df["resource_name"]))
            )
        )
        if (delta := df.height - _df.height) > 0:
            logger.warning(f"Resource assignments eliminated because using unexisting tasks or resources: {delta}")
        # Filter resource assignmetns for which the capacity of the resource is not even enough to cover the entire task
        # NOTE: We don't allow assignment of multiple resources to complete a task
        df = (
            _df
            .join(self.tasks.df.select("task_name", "duration"), on="task_name", how="inner")
            .join(self.resources.df.select("resource_name", "capacity"), on="resource_name", how="inner")
            .filter(pl.col.capacity > pl.col.duration)
        )
        if (delta := _df.height - df.height) > 0:
            logger.warning(f"Resource assignments eliminated because using unexisting tasks or resources: {delta}")
        return df.select(columns).with_row_index("id")
    

class GroupAssignmentsBuilder(BaseBuilder):
    
    def __init__(self, group_assignments_reader: GroupAssignmentsReader, tasks: TasksBuilder, resource_groups: ResourceGroupsBuilder) -> pl.DataFrame:
        self.tasks = tasks 
        self.resource_groups = resource_groups 
        super().__init__(df=group_assignments_reader.df, schema=GroupAssignmentBuilderSchema)
        
    def _build(self, df: pl.DataFrame) -> pl.DataFrame:
        # Filter groups assignments made by unexisting (or already filtered) groups or tasks
        df = (
            df.filter(
                (pl.col.task_name.is_in(self.tasks.df["task_name"]))
                & (pl.col.group_name.is_in(self.resource_groups.df["group_name"]))
            )
        )
        if (delta := df.height - df.height) > 0:
            logger.warning(f"Group assignments eliminated because using unexisting tasks or groups: {delta}")
        return df.with_row_index("id")


class ResourceAssignmentCombinationsBuilder(BaseBuilder):
    
    def __init__(self, resource_assignments: ResourceAssignmentsReader, tasks: TasksBuilder):
        self.tasks = tasks
        super().__init__(df=resource_assignments.df, schema=ResourceAssignmentCombinationBuilderSchema)

    def _build(self, df: pl.DataFrame) -> pl.DataFrame:
        assignments = df.select("id", "resource_name", "task_name")
        return (
            assignments.join(assignments, how="cross")
            .rename({
                "id": "assignment_left_id", 
                "id_right": "assignment_right_id",
                "resource_name": "resource_name_left",
                "task_name": "task_name_left",
            })
            .filter(
                (pl.col.resource_name_left == pl.col.resource_name_right) 
                & (pl.col.task_name_left != pl.col.task_name_right)
            )
            .rename({"resource_name_left": "resource_name"})
            .drop("resource_name_right")
            .join(
                self.tasks.df.select(pl.col("task_name").alias("task_name_left"), pl.col("type").alias("type_left")),
                on="task_name_left",
                how="inner"
            )
            .join(
                self.tasks.df.select(pl.col("task_name").alias("task_name_right"), pl.col("type").alias("type_right")),
                on="task_name_right",
                how="inner"
            )
            .with_row_index("id")
        )