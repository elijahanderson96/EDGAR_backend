from pydantic import BaseModel

class AccountInfo(BaseModel):
    username: str
    email: str
    fundamentals_route_count: int
    balance_sheet_route_count: int
    income_statement_route_count: int
    stock_prices_route_count: int
    metadata_route_count: int
