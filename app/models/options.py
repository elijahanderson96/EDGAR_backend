from pydantic import BaseModel


class TickerRequest(BaseModel):
    symbol: str


class ExpirationRequest(BaseModel):
    symbol: str
    expiration: str


class CollarAnalysisRequest(BaseModel):
    symbol: str
    expiration: str
    put_strike: float
    call_strike: float
