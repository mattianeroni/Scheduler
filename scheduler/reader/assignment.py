from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.reader.base import BaseReader


class ResourceAssignmentReaderSchema(pa.DataFrameModel):
    task_name: Series[str] = pa.Field(coerce=True)
    resource_name: Series[str] = pa.Field(coerce=True)
    type: Series[str] = pa.Field(coerce=True, nullable=True, isin=["forced", "relaxed"])

    @pa.dataframe_check(error="Assignments Task-Resource name must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique(["task_name", "resource_name"]).collect().height == data.lazyframe.collect().height

    @pa.dataframe_check(error="No assignments task-resource provided.")
    def empty_check(cls, data: pa.PolarsData):
        return data.lazyframe.collect().height > 0
    

class ResourceAssignmentsReader(BaseReader):

    def __init__(self, filepath: str):
        super().__init__(filepath, schema=ResourceAssignmentReaderSchema)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.col.type.fill_null(pl.lit("relaxed")))
    

class GroupAssignmentReaderSchema(pa.DataFrameModel):
    task_name: Series[str] = pa.Field(coerce=True)
    group_name: Series[str] = pa.Field(coerce=True)
    require_all_group: Series[bool] = pa.Field(coerce=True, nullable=True, default=False)
    
    @pa.dataframe_check(error="Assignments Task-Group name must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique(["task_name", "group_name"]).collect().height == data.lazyframe.collect().height
    

class GroupAssignmentsReader(BaseReader):

    def __init__(self, filepath: str):
        super().__init__(filepath, schema=GroupAssignmentReaderSchema)
    
    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.col.require_all_group.fill_null(False))