from __future__ import annotations

from pydantic import BaseModel, Field
from datetime import datetime

class MetricResult(BaseModel):
    dataset: str
    run_id: str
    run_ts: datetime
    metric_name: str
    column: str
    dimension: str
    severity_level: str 
    threshold_result: float = Field(default=0.0)
    threshold_range: float = Field(default=0.0)
    threshold: float = Field(default=0.0)
    value_double: float = Field(default=0.0)
    value_string: str = Field(default="")
    ingest_datetime: datetime = Field(default_factory=datetime.now)
 