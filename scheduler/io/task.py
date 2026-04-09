from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.io.base import BaseInput


class TaskSchema(pa.DataFrameModel):
    task_name: Series[str] = pa.Field(coerce=True, unique=True)
    duration: Series[int] = pa.Field(coerce=True, gt=0)
    start: Series[int] = pa.Field(coerce=True, ge=0, nullable=True)
    end: Series[int] = pa.Field(coerce=True, gt=0, nullable=True)
    type: Series[str] = pa.Field(coerce=True, isin=["rigid", "fluid"])

    @pa.dataframe_check(error="Tasks name must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("task_name").collect().height == data.lazyframe.collect().height
    
    @pa.dataframe_check(error="No tasks provided.")
    def empty_check(cls, data: pa.PolarsData):
        return data.lazyframe.collect().height > 0
    
    @pa.dataframe_check(error="Durations cannot be negative.")
    def duration_check(cls, data: pa.PolarsData):
        return data.lazyframe.select((pl.col.duration > 0).all()).collect().item()
    
    @pa.dataframe_check(error="Start time cannot be negative.")
    def start_check(cls, data: pa.PolarsData):
        return data.lazyframe.select((pl.col.start.is_null() | pl.col.start >= 0).all()).collect().item()
    
    @pa.dataframe_check(error="End time cannot be negative or lower than duration.")
    def end_check(cls, data: pa.PolarsData):
        return data.lazyframe.select((pl.col.end.is_null() | pl.col.end >= pl.col.duration).all()).collect().item()
    
    @pa.dataframe_check(error="Unconsisten (end - start) and durations.")
    def consistency_check(cls, data: pa.PolarsData):
        return (
            data.lazyframe.filter(pl.col.start.is_not_null() & pl.col.end.is_not_null())
            .select((pl.col.duration == (pl.col.end - pl.col.start)).all())
            .collect()
            .item()
        )

class Tasks(BaseInput):

    def __init__(self, filepath: str):
        super().__init__(filepath, schema=TaskSchema)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(
                start = pl.when(pl.col.start.is_null() & pl.col.end.is_not_null())
                .then(pl.col.end - pl.col.duration)
                .otherwise("start"),
                end = pl.when(pl.col.end.is_null() & pl.col.start.is_not_null())
                .then(pl.col.start + pl.col.duration)
                .otherwise("end"),
            )
        )