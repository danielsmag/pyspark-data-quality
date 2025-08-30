from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dq_platform.core.models import MetricResult
    from pyspark.sql import DataFrame

__all__: list[str] = ["ResultObj"]


class ResultObj:
    def __init__(self) -> None:
        pass