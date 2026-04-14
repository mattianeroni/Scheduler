from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.builder.base import BaseBuilder
from scheduler.reader.task import TasksReader

class TaskBuilderSchema(pa.DataFrameModel):
    id: Series[int] = pa.Field(coerce=True, unique=True)
    task_name: Series[str] = pa.Field(coerce=True, unique=True)
    duration: Series[int] = pa.Field(coerce=True, gt=0)
    start: Series[int] = pa.Field(coerce=True, ge=0, nullable=True)
    end: Series[int] = pa.Field(coerce=True, gt=0, nullable=True)
    type: Series[str] = pa.Field(coerce=True, isin=["rigid", "fluid"])

    @pa.dataframe_check(error="Tasks id must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("id").collect().height == data.lazyframe.collect().height
    

class TasksBuilder(BaseBuilder):

    def __init__(self, tasks_reader: TasksReader):
        super().__init__(df=tasks_reader.df, schema=TaskBuilderSchema)

    def _build(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_row_index("id")