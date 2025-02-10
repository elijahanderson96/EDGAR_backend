from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional, List
import logging

from ui_api.helpers import get_user_id_and_request_type, update_api_usage
from ui_api.models.financials import FinancialRecord
from database.async_database import db_connector

logging.basicConfig(level=logging.INFO)

financials_router = APIRouter()


async def get_user_id_by_username(username: str) -> int:
    query = f"SELECT id FROM users.users WHERE username = '{username}'"
    result = await db_connector.run_query(query, return_df=True)
    if result.empty:
        raise HTTPException(status_code=404, detail="User not found")
    return result.iloc[0]['id']


@financials_router.get("/financials", tags=["Financials_API"], response_model=List[FinancialRecord])
async def get_financial_data(
        request: Request,
        symbol: str = Query(..., description="Stock symbol (mandatory)"),
        fact_name: Optional[str] = Query(None, description="Fact name to filter"),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
        latest_n_records: Optional[int] = Query(None, description="Fetch the latest N records")
):
    """
    Endpoint to fetch financial data from the company_facts table.
    """
    user_id, is_api_key = await get_user_id_and_request_type(request)

    # Base SQL query
    query = """
    SELECT cf.value, cf.start_date_id, cf.end_date_id, cf.filed_date_id, cf.fiscal_year, cf.fiscal_period, cf.form,
           cf.fact_name, s.symbol, d_start.date AS start_date, d_end.date AS end_date, d_filed.date AS filed_date
    FROM financials.company_facts cf
    JOIN metadata.symbols s ON cf.symbol_id = s.symbol_id
    LEFT JOIN metadata.dates d_start ON cf.start_date_id = d_start.date_id
    LEFT JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
    LEFT JOIN metadata.dates d_filed ON cf.filed_date_id = d_filed.date_id
    WHERE s.symbol = $1
    """
    params = [symbol]

    # Add optional filtering by fact_name
    if fact_name:
        query += " AND cf.fact_name = $2"
        params.append(fact_name)

    # Add ordering and limit for the query
    query += " ORDER BY cf.id DESC"
    if latest_n_records:
        query += f" LIMIT {latest_n_records}"
    else:
        query += " LIMIT 1000"

    # Fetch records from the database
    records = await db_connector.run_query(query, params=params)
    if records.empty:
        raise HTTPException(status_code=404, detail="No records found.")

    # Convert date IDs to actual dates in the DataFrame
    records['start_date'] = records['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else None)
    records['end_date'] = records['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else None)
    records['filed_date'] = records['filed_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else None)

    # Filter by start_date and end_date in Python
    if start_date:
        records = records[records['end_date'] >= start_date]
    if end_date:
        records = records[records['start_date'] <= end_date]

    # Convert to dictionary for FastAPI response
    result = records.to_dict(orient="records")

    # Update API usage count
    if is_api_key:
        await update_api_usage(user_id, f'{symbol}_company_facts', len(result))

    return result


@financials_router.get("/financials/market_caps", tags=["Financials_API"])
async def get_market_cap_data(
        request: Request,
        symbol: Optional[str] = Query(None, description="Stock symbol"),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
        latest_n_records: Optional[int] = Query(1000, description="Limit number of records")
):
    """
    Fetch market capitalization data from the materialized view `market_caps`.
    """
    user_id, is_api_key = await get_user_id_and_request_type(request)
    # Base Query
    query = """
    SELECT date, symbol, market_cap, shares, price
    FROM financials.market_caps
    WHERE 1=1
    """
    params = []

    if symbol:
        query += " AND symbol = $1"
        params.append(symbol)

    if start_date:
        query += " AND date >= $2"
        params.append(start_date)

    if end_date:
        query += " AND date <= $3"
        params.append(end_date)

        # Add ordering and limit for the query
    query += " ORDER BY date DESC"

    if latest_n_records:
        query += f" LIMIT {latest_n_records}"
    else:
        query += " LIMIT 1000"
    print(query, params)
    market_caps = await db_connector.run_query(query, params=params)

    if market_caps.empty:
        raise HTTPException(status_code=404, detail="No market cap data found.")

    result = market_caps.to_dict(orient="records")

    # Update API usage count
    if is_api_key:
       await update_api_usage(user_id, f'{symbol}_market_cap', len(result))

    return result
