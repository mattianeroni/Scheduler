from __future__ import annotations

import polars as pl 
import pandera.polars as pa
from abc import ABC, abstractmethod
from pandera.errors import SchemaError

from scheduler.error import SchedulerIOError


class BaseReader(ABC):
    def __init__(self, filepath: str, schema: pa.DataFrameModel):
        self.filepath = filepath
        self.schema = schema

        df = pl.read_csv(self.filepath)
        try:
            self.schema.validate(df)
        except SchemaError as e:
            raise SchedulerIOError(f"IO schema validation failed: {str(e)}")
        
        self.df = self._transform(df)

    @abstractmethod 
    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process the dataframe - to be implemented by subclasses."""
        pass