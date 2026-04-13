from __future__ import annotations

import logging 
import polars as pl

from scheduler.reader.group import ResourceGroupsReader

logger = logging.getLogger(__name__)


class ResourceGroupsBuilder:
    @staticmethod
    def build(resource_groups_reader: ResourceGroupsReader, resources_df: pl.DataFrame) -> pl.DataFrame:
        # Filter resource groups made by unexisting resources
        df = (
            resource_groups_reader.df.explode("resource_name")
            .filter(pl.col.resource_name.is_in(resources_df["resource_name"]))
            .group_by("group_name").agg("resource_name")
        )
        if (delta := resource_groups_reader.df.height - df.height) > 0:
            logger.warning(f"Resource groups eliminated because made by unexisting resources: {delta}")
        return df.with_row_index("id")