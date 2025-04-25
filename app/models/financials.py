from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date

class CompanyFactBase(BaseModel):
    fact_name: str
    unit: str
    start_date: Optional[date] = None # Make start_date optional
    end_date: date
    filed_date: date
    fiscal_year: int
    fiscal_period: str
    form: str
    value: float # Use float for NUMERIC, adjust if needed (e.g., Decimal)
    accn: str

class CompanyFact(CompanyFactBase):
    symbol: str # Add symbol back for response clarity

    class Config:
        orm_mode = True # For potential ORM usage later, adapt for Pydantic v2 if needed (from_attributes=True)

class FactQueryResponse(BaseModel):
    symbol: str
    fact_name: str
    data: List[CompanyFactBase] # Return list of facts without repeating symbol/fact_name

class CommonFinancialsParams(BaseModel):
    """Holds common parameters and their resolved IDs for financial routes."""
    symbol: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    symbol_id: int
    start_date_id: Optional[int] = None
    end_date_id: Optional[int] = None

# Example for a potential future route needing specific fields
class RevenueFact(BaseModel):
    symbol: str
    end_date: date
    filed_date: date
    value: float
    unit: str
