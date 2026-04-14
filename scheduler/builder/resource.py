from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.builder.base import BaseBuilder
from scheduler.reader.resource import ResourcesReader

class ResourceBuilderSchema(pa.DataFrameModel):
    id: Series[int] = pa.Field(coerce=True, unique=True)
    resource_name: Series[str] = pa.Field(coerce=True, unique=True)
    capacity: Series[float] = pa.Field(coerce=True, ge=0)

    @pa.dataframe_check(error="Resources id must be unique.")
    def primary_key_check(cls, data: pa.PolarsData):
        return data.lazyframe.unique("id").collect().height == data.lazyframe.collect().height
    

class ResourcesBuilder(BaseBuilder):

    def __init__(self, resources_reader: ResourcesReader):
        super().__init__(df=resources_reader.df, schema=ResourceBuilderSchema)

    def _build(self, df: pl.DataFrame)  -> pl.DataFrame:
        return df.with_row_index("id")