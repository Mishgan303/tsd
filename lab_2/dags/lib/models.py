from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SourceWindow:
    """Хранит нижнюю и верхнюю границы инкремента для одного ODS-источника."""

    source_name: str
    lower_bound: datetime | None
    upper_bound: datetime | None


@dataclass(frozen=True, slots=True)
class QualityResult:
    """Хранит результат одной DQ-проверки и признак её критичности."""

    check_name: str
    status: bool
    actual_value: str
    expected_rule: str
    details: str | None = None
    blocking: bool = True
