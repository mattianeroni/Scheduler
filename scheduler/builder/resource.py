from __future__ import annotations

import polars as pl
from scheduler.reader.resource import ResourcesReader


class ResourcesBuilder:
    def build(resources_reader: ResourcesReader)  -> pl.DataFrame:
        return resources_reader.df.with_row_index("id")