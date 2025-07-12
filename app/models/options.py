from pydantic import BaseModel


class TickerRequest(BaseModel):
    symbol: str


class ExpirationRequest(BaseModel):
    symbol: str
    expiration: str


class CollarAnalysisRequest(BaseModel):
    symbol: str
    put_expiration: str
    call_expiration: str
    put_strike: float
    call_strike: float


class LongOptionAnalysisRequest(BaseModel):
    symbol: str
    expiration: str
    strike: float
