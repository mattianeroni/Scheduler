from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.reader.base import BaseReader


class ResourceGroupReaderSchema(pa.DataFrameModel):
    resource_name: Series[str] = pa.Field(coerce=True)
    group_name: Series[str] = pa.Field(coerce=True)


class ResourceGroupsReader(BaseReader):
    
    def __init__(self, filepath: str):
        super().__init__(filepath, schema=ResourceGroupReaderSchema)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unique(["group_name", "resource_name"]).group_by("group_name").agg("resource_name")