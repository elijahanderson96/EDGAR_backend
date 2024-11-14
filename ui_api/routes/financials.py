import json
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional, List
import logging

from ui_api.helpers import get_user_id_and_request_type, update_api_usage_count
from ui_api.models.financials import FinancialRecord
from database.async_database import db_connector

logging.basicConfig(level=logging.INFO)

financials_router = APIRouter()

# Lists of tables
ALLOWED_TABLES = [
    "assets", "cash_financing_activities", "cash_investing_activities", "cash_operating_activities",
    "common_stock", "comprehensive_income", "cost_of_revenue", "current_assets", "current_liabilities",
    "depreciation_and_amortization", "eps_basic", "eps_diluted", "goodwill", "gross_profit",
    "intangible_assets", "interest_expense", "inventory", "liabilities", "net_income_loss",
    "operating_expenses", "operating_income", "operating_income_loss", "preferred_stock",
    "property_plant_and_equipment", "research_and_development_expense", "retained_earnings",
    "revenue", "shares", "total_stockholders_equity"
]

# Tables that should only consider `end_date`
INSTANTANEOUS_TABLES = [
    "assets", "current_assets", "current_liabilities", "inventory", "liabilities", "property_plant_and_equipment",
    "retained_earnings", "preferred_stock", "intangible_assets", "goodwill", "common_stock", "shares",
    "total_stockholders_equity"
]


async def get_user_id_by_username(username: str) -> int:
    query = f"SELECT id FROM users.users WHERE username = '{username}'"
    result = await db_connector.run_query(query, return_df=True)
    if result.empty:
        raise HTTPException(status_code=404, detail="User not found")
    return result.iloc[0]['id']


async def fetch_financial_data(table: str, symbol: Optional[str], start_date: Optional[str],
                               end_date: Optional[str], latest_n_records: Optional[int]):
    # Base query structure
    query = f"""
        SELECT f.value, s.symbol,
    """

    # Determine select and join conditions based on the table type
    if table in INSTANTANEOUS_TABLES:
        # Instantaneous tables only have `end_date_id`
        query += "rd_end.date AS end_date "
        query += f"""
            FROM financials.{table} f
            JOIN metadata.symbols s ON f.symbol_id = s.symbol_id
            JOIN metadata.dates rd_end ON f.end_date_id = rd_end.date_id
        """
    else:
        # Period tables have both `start_date_id` and `end_date_id`
        query += "rd_start.date AS start_date, rd_end.date AS end_date "
        query += f"""
            FROM financials.{table} f
            JOIN metadata.symbols s ON f.symbol_id = s.symbol_id
            JOIN metadata.dates rd_start ON f.start_date_id = rd_start.date_id
            JOIN metadata.dates rd_end ON f.end_date_id = rd_end.date_id
        """

    # Add a WHERE clause base
    query += "WHERE 1=1 "

    # Prepare parameters and add filtering conditions
    params = []
    param_index = 1

    if symbol:
        query += f" AND s.symbol = ${param_index}"
        params.append(symbol)
        param_index += 1

    # Date filtering logic based on table type
    if table in INSTANTANEOUS_TABLES:
        # Only filter by `end_date`
        if end_date:
            query += f" AND rd_end.date <= ${param_index}"
            params.append(datetime.strptime(end_date, '%Y-%m-%d'))
            param_index += 1
    else:
        # Use both `start_date` and `end_date` for range filtering
        if start_date:
            query += f" AND rd_end.date >= ${param_index}"
            params.append(datetime.strptime(start_date, '%Y-%m-%d'))
            param_index += 1
        if end_date:
            query += f" AND rd_start.date <= ${param_index}"
            params.append(datetime.strptime(end_date, '%Y-%m-%d'))
            param_index += 1

    # Add limit directly if provided, else default to 500 without adding to params
    if latest_n_records:
        query += f" ORDER BY f.id DESC LIMIT {latest_n_records}"
    else:
        query += " ORDER BY f.id DESC LIMIT 500"

    records = await db_connector.run_query(query, params=params)

    if records.empty:
        raise HTTPException(status_code=404, detail="No records found")

    print(records)
    # Format date columns as strings
    if 'start_date' in records.columns.tolist():
        records['start_date'] = records['start_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else None)

    if 'end_date' in records.columns.tolist():
        records['end_date'] = records['end_date'].apply(lambda x: x.strftime('%Y-%m-%d') if x else None)

    print(records)
    return records.to_dict(orient='records')


@financials_router.get("/{table}", response_model=List[FinancialRecord])
async def get_financial_data(
        request: Request,
        table: str,
        symbol: Optional[str] = Query(None, description="Stock symbol"),
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
        latest_n_records: Optional[int] = Query(None, description="Fetch the latest N records")
):
    # Ensure table is in the list of allowed tables
    if table not in ALLOWED_TABLES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid table name.",
                "valid_endpoints": ALLOWED_TABLES
            }
        )

    user_id, is_api_key = await get_user_id_and_request_type(request)
    data = await fetch_financial_data(table, symbol, start_date, end_date, latest_n_records)
    record_count = len(data)

    # Update API usage count
    if is_api_key:
        await update_api_usage_count(user_id, table, record_count)

    return data
