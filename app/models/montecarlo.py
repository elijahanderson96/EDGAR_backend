from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum

class Period(str, Enum):
    one_year = "1y"
    two_years = "2y"
    three_years = "3y"
    five_years = "5y"
    ten_years = "10y"

class MonteCarloRequest(BaseModel):
    symbol: str = Field(..., example="SPY")
    period: Period = Field(..., example="1y")
    num_simulations: int = Field(1000, ge=100, le=10000, example=1000)
    forecast_days: int = Field(252, ge=1, le=2520, example=252)  # Default to 1 trading year

class ConfidenceIntervals(BaseModel):
    one_std: List[float]
    two_std: List[float]
    three_std: List[float]

class MonteCarloResponse(BaseModel):
    current_price: float
    simulations: List[List[float]]  # We'll limit the number of simulations returned for performance
    confidence_intervals: ConfidenceIntervals
    forecast_days: int
    period_used: str
