from datetime import date
from typing import Optional
from pydantic import BaseModel
from pydantic.types import Decimal


class FinancialRecord(BaseModel):
    symbol: str
    fact_name: Optional[str] = None  # Name of the financial fact (e.g., "Revenue", "Net Income")
    start_date: Optional[date] = None  # Report period start date
    end_date: Optional[date] = None  # Report period end date
    filed_date: Optional[date] = None  # Date when the data was filed with the SEC
    fiscal_year: Optional[int] = None  # Fiscal year of the report
    fiscal_period: Optional[str] = None  # Fiscal period (e.g., "Q1", "Q2", "FY")
    form: Optional[str] = None  # SEC filing form (e.g., "10-Q", "10-K")
    value: Decimal  # The reported value for the financial fact
