from __future__ import annotations

from scheduler.reader.task import TasksReader


class TasksBuilder:
    def __init__(self, tasks_reader: TasksReader):
        self.df = tasks_reader.df.with_row_index("id")