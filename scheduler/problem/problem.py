from __future__ import annotations

import polars as pl

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.io.resource import ResourceReader
    from scheduler.io.task import TaskReader
    from scheduler.io.group import GroupReader

class SchedulingProblem:

    def __init__(self, task: TaskReader, resource: ResourceReader, group: GroupReader):
        self.task = task
        self.resource = resource
        self.group = group 

