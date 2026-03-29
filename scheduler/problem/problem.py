from __future__ import annotations

import polars as pl

import logging 
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler.config import Config
    from scheduler.io.resource import ResourceReader
    from scheduler.io.task import TaskReader
    from scheduler.io.group import GroupReader
    from scheduler.io.assignment import ResourceAssignmentReader, GroupAssignmentReader

logger = logging.Logger(__name__)


class SchedulingProblem:

    def __init__(self, 
            config: Config,
            task: TaskReader, 
            resource: ResourceReader, 
            group: GroupReader, 
            resource_assignment: ResourceAssignmentReader,
            group_assignment: GroupAssignmentReader,
        ):
        self.config = config
        self.task = task
        self.resource = resource
        self.group = group 
        self.resource_assignment = resource_assignment
        self.group_assignment = group_assignment

    def validate(self):
        pass

