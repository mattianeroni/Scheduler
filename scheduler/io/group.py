from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.io.base import BaseInput


class ResourceGroupSchema(pa.DataFrameModel):
    resource_name: Series[str] = pa.Field(coerce=True)
    group_name: Series[str] = pa.Field(coerce=True)


class ResourceGroups(BaseInput):
    
    def __init__(self, filepath: str):
        super().__init__(filepath, schema=ResourceGroupSchema)

    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unique(["group_name", "resource_name"]).group_by("group_name").agg("resource_name")