from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing import Series

from scheduler.io.base import BaseReader

class AssignmentSchema(pa.DataFrameModel):
    name: Series[str] = pa.Field(coerce=True, unique=True)
    capacity: Series[float] = pa.Field(coerce=True, ge=0)

    @pa.dataframe_check(error="Resources name must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("name").collect().height == data.lazyframe.collect().height
    
    @pa.dataframe_check(error="Capacities cannot be negative.")
    def capacity_check(cls, data: pa.PolarsData):
        return data.lazyframe.select((pl.col.capacity > 0).all()).collect().item()


class AssignmentReader(BaseReader):

    def __init__(self, filepath: str):
        super().__init__(filepath, schema=AssignmentSchema)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_row_index("id")