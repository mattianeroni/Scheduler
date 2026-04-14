from __future__ import annotations

import logging 
import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

from scheduler.builder.base import BaseBuilder
from scheduler.builder.resource import ResourcesBuilder
from scheduler.reader.group import ResourceGroupsReader

logger = logging.getLogger(__name__)

class ResourceGroupBuilderSchema(pa.DataFrameModel):
    id: Series[int] = pa.Field(coerce=True, unique=True)
    group_name: Series[str] = pa.Field(coerce=True)
    resource_name: Series[list[str]] = pa.Field(coerce=True)


class ResourceGroupsBuilder(BaseBuilder):
    def __init__(self, resource_groups_reader: ResourceGroupsReader, resources: ResourcesBuilder):
        self.resources = resources 
        super().__init__(df=resource_groups_reader.df, schema=ResourceGroupBuilderSchema)

    def _build(self, df: pl.DataFrame) -> pl.DataFrame:
        # Filter resource groups made by unexisting resources
        df = (
            df.explode("resource_name")
            .filter(pl.col.resource_name.is_in(self.resources.df["resource_name"]))
            .group_by("group_name").agg("resource_name")
        )
        if (delta := df.height - df.height) > 0:
            logger.warning(f"Resource groups eliminated because made by unexisting resources: {delta}")
        return df.with_row_index("id")