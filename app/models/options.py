from pydantic import BaseModel


class ExpirationRequest(BaseModel):
    symbol: str
    expiration: str
