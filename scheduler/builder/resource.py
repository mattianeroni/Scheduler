from __future__ import annotations

from scheduler.reader.resource import ResourcesReader


class ResourcesBuilder:
    def __init__(self, resources_reader: ResourcesReader):
        self.df = resources_reader.df.with_row_index("id")