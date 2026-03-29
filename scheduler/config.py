from __future__ import annotations

import tomllib
from pydantic import BaseModel

from dataclasses import dataclass

@dataclass
class Config(BaseModel):
    timehorizon: int
    optimization_gap: float | None = None 
    max_time: float | None = None