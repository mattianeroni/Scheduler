from __future__ import annotations

import logging
import pathlib

import tomllib

from scheduler.files_properties import InputFiles
from scheduler.config import Config
from scheduler.error import SchedulerIOError
from scheduler.reader.task import TasksReader
from scheduler.reader.resource import ResourcesReader
from scheduler.reader.group import ResourceGroupsReader
from scheduler.reader.assignment import ResourceAssignmentsReader, GroupAssignmentsReader
from scheduler.problem.problem import SchedulingProblem
from scheduler.solver.model import SchedulingModel
from scheduler.utils import setup_logging

logger = logging.getLogger(__name__)



class Scheduler:

    def __init__(self, input_path: str, output_path: str):
        self.input_path = pathlib.Path(input_path)
        self.output_path = pathlib.Path(output_path)

    def _validate_input(self):
        config_path = self.input_path / InputFiles.CONFIG
        if not config_path.exists():
            raise SchedulerIOError(f"Configuration file 'config.toml' not found in {self.input_path.as_posix()}.")
        
        missing_inputs = []
        for filename in (
            InputFiles.TASKS, 
            InputFiles.RESOURCES, 
            InputFiles.RESOURCE_GROUPS, 
            InputFiles.RESOURCE_ASSIGNMENTS, 
            InputFiles.GROUP_ASSIGNMENTS
            ):
            if not (self.input_path / filename).exists():
                missing_inputs.append(filename)
        
        if len(missing_inputs) > 0:
            raise SchedulerIOError(f"Input files {missing_inputs} not found in {self.input_path.as_posix()}")

    def _load_config(self) -> Config:
        config_path = self.input_path / InputFiles.CONFIG
        with open(config_path, "rb") as f:
            config_data = tomllib.load(f)
        return Config(**config_data)

    def _load_problem(self, config: Config) -> SchedulingProblem:
        tasks_reader = TasksReader(self.input_path / InputFiles.TASKS)
        resources_reader = ResourcesReader(self.input_path / InputFiles.RESOURCES)
        resource_groups_reader = ResourceGroupsReader(self.input_path / InputFiles.RESOURCE_GROUPS)
        resource_assignments_reader = ResourceAssignmentsReader(self.input_path / InputFiles.RESOURCE_ASSIGNMENTS)
        group_assignments_reader = GroupAssignmentsReader(self.input_path / InputFiles.GROUP_ASSIGNMENTS)
        return SchedulingProblem(
            config=config,
            tasks_reader=tasks_reader, 
            resources_reader=resources_reader, 
            resource_groups_reader=resource_groups_reader, 
            resource_assignments_reader=resource_assignments_reader,
            group_assignments_reader=group_assignments_reader,
        )

    def run(self):
        self.output_path.mkdir(parents=True, exist_ok=True)
        setup_logging(self.output_path)

        logger.info("Scheduler execution.")
        logger.info(f"Input folder: {self.input_path.as_posix()}.")
        logger.info(f"Output folder: {self.output_path.as_posix()}.")

        self._validate_input()
        logger.info("All input files detected.")

        config = self._load_config()
        logger.info("Configuration loaded.")

        problem = self._load_problem(config)
        logger.info("Problem loaded.")

        problem.build()
        problem.validate()

        #model = SchedulingModel(problem)
        #model.build()
        #solution = model.solve()

        logger.info("Writing solution.")
        #solution.write(self.output_path)
        logger.info(f"Solution output written to {self.output_path.as_posix()}.")

        logger.info("Schedule execution concluded successfully.")
    