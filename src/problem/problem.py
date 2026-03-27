from __future__ import annotations

from src.task.task import Task, FluidTask
from src.resource.resource import Resource, ResourceGroup 

class SchedulingProblem:
    def __init__(self):
        self.tasks: dict[int, Task | FluidTask] = {}
        self.resources: dict[int, Resource] = {}
        self.groups: dict[int, ResourceGroup] = {}

    def add_resources(self, *resources : list[Resource]):
        for resource in resources:
            if not isinstance(resource, Resource):
                raise ValueError(f"Expected Resource, got {type(resource)}")
            self.resources[resource.id] = resource 
    
    def add_tasks(self, *tasks: list[Task | FluidTask]):
        for task in tasks:
            if not isinstance(task, Task) or not isinstance(task, FluidTask):
                raise ValueError(f"Expected Task, got {type(task)}")
            self.tasks[task.id] = task

    def add_groups(self, *groups: list[ResourceGroup]):
        for group in groups:
            if not isinstance(group, ResourceGroup):
                raise ValueError(f"Expected ResourceGroup, got {type(group)}")
            self.groups[group.id] = group
            for resource in group.resources:
                if resource.id not in self.resources:
                    self.resources[resource.id] = resource 
    

     
