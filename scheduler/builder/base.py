from __future__ import annotations

import polars as pl 
import pandera.polars as pa
from abc import ABC, abstractmethod
from pandera.errors import SchemaError

from scheduler.error import SchedulerBuildError


class BaseBuilder(ABC):
    def __init__(self, df: pl.DataFrame, schema: pa.DataFrameModel):
        self.schema = schema
        self.df = self._build(df)
        try:
            self.schema.validate(self.df)
        except SchemaError as e:
            raise SchedulerBuildError(f"Build schema validation failed: {str(e)}")
        
    @abstractmethod 
    def _build(self, df: pl.DataFrame) -> pl.DataFrame:
        pass