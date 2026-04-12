from __future__ import annotations

import logging 
import polars as pl

from scheduler.reader.group import ResourceGroupsReader
from scheduler.builder.resource import ResourcesBuilder

logger = logging.getLogger(__name__)


class ResourceGroupsBuilder:
    
    def __init__(self, resource_groups_reader: ResourceGroupsReader, resources: ResourcesBuilder):
        # Filter resource groups made by unexisting resources
        df = (
            resource_groups_reader.df.explode("resource_name")
            .filter(pl.col.resource_name.is_in(resources.df["resource_name"]))
            .group_by("group_name").agg("resource_name")
        )
        if (delta := resource_groups_reader.df.height - df.height) > 0:
            logger.warning(f"Resource groups eliminated because made by unexisting resources: {delta}")
        self.df = df.with_row_index("id")