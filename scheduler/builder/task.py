from __future__ import annotations

import polars as pl
from scheduler.reader.task import TasksReader


class TasksBuilder:
    def build(tasks_reader: TasksReader) -> pl.DataFrame:
        return tasks_reader.df.with_row_index("id")