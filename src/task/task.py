from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.resource.resource import Resource, ResourceGroup   

class FluidTask:
    def __init__(self, id: int, duration: int = 1, name: None | str = None):
        self.id = id 
        self.duration = duration
        self.name = name
        self.resources: dict[int, Resource] = {}
        self.groups : dict[int, tuple[ResourceGroup, bool]] = {}

    def add_executor(self, *resources: list[Resource]):
        for resource in resources:
            if not isinstance(resource, Resource):
                raise ValueError(f"Expected Resource, got {type(resource)}")
            self.resources[resource.id] = resource
    
    def add_executor_group(self, group: ResourceGroup, all_involved: bool = False):
        if not isinstance(group, ResourceGroup):
            raise ValueError(f"Expected ResourceGroup, got {type(group)}")
        self.groups[group.id] = (group, all_involved)


class Task(FluidTask):
    def __init__(self, id: int, start: int, end: int, name: None | str = None):
        super().__init__(id, end - start, name)
        self.start = start 
        self.end = end
