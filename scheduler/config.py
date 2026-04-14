from __future__ import annotations

from pydantic import BaseModel


class Config(BaseModel):
    timehorizon: int = 160
    overlap_penalization: float = 100.0
    optimization_gap: float | None = None 
    max_time: float | None = None
    verbose: bool = True