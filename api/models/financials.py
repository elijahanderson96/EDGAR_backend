from datetime import datetime
from typing import Any, List

from pydantic import BaseModel


class FinancialRecord(BaseModel):
    symbol: str
    report_date: Any
    filing_date: Any
    data: Any
