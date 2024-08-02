from typing import Any, Dict, List

from pydantic import BaseModel


class FinancialRecord(BaseModel):
    symbol: str
    report_date: str
    filing_date: str
    data: List[Dict[str, Any]]
