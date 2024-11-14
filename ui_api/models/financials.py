from datetime import date
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
from pydantic.types import Decimal


class FinancialRecord(BaseModel):
    symbol: str
    start_date: Optional[date] = None  # Corresponds to report period start date
    end_date: Optional[date] = None    # Corresponds to report period end date
    value: Decimal
