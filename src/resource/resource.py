from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.task.task import Task, FluidTask

class Resource:
    def __init__(self, id: int, capacity: float = float("inf"), name: None | str = None):
        self.id = id
        self.capacity = capacity
        self.name = name
        self.group: ResourceGroup | None = None

    def assign_tasks(self, tasks: list[Task | FluidTask]):
        for task in tasks:
            if not isinstance(task, (Task, FluidTask)):
                raise ValueError(f"Expected Task or FluidTask, got {type(task)}")
            task.resources[self.id] = self  


class ResourceGroup:
    def __init__(self, id: int, resources: list[Resource] | None = None, name: None | str = None):
        self.id = id
        self.resources = resources or []
        self.name = name    
        for resource in resources:
            resource.group = self 

    def assign_tasks(self, tasks: list[Task | FluidTask], all_involved: bool = False):
        for task in tasks:
            if not isinstance(task, (Task, FluidTask)):
                raise ValueError(f"Expected Task or FluidTask, got {type(task)}")
            task.groups[self.id] = (self, all_involved)
    
    def add_resources(self, *resources: list[Resource]):
        for resource in resources:
            if not isinstance(resource, Resource):
                raise ValueError(f"Expected Resource, got {type(resource)}")
            self.resources.append(resource)
            resource.group = self
