import json
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional, List
import logging

from ui_api.helpers import get_user_id_and_request_type, update_api_usage_count
from ui_api.models.financials import FinancialRecord
from database.async_database import db_connector

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Define the FastAPI app
financials_router = APIRouter()


async def get_user_id_by_username(username: str) -> int:
    query = f"SELECT id FROM users.users WHERE username = '{username}'"
    result = await db_connector.run_query(query, return_df=True)
    if result.empty:
        raise HTTPException(status_code=404, detail="User not found")
    return result.iloc[0]['id']


async def fetch_financial_data(table: str, symbol: Optional[str], report_date: Optional[str],
                               filing_date: Optional[str], latest_n_records: Optional[int]):
    query = f"""
        SELECT f.data, s.symbol, rd.date AS report_date, fd.date AS filing_date
        FROM financials.{table} f
        JOIN metadata.symbols s ON f.symbol_id = s.symbol_id
        JOIN metadata.dates rd ON f.report_date_id = rd.date_id
        JOIN metadata.dates fd ON f.filing_date_id = fd.date_id
        WHERE 1=1
    """
    params = []
    param_index = 1

    if symbol:
        query += f" AND s.symbol = ${param_index}"
        params.append(symbol)
        param_index += 1
    if report_date:
        query += f" AND rd.date = ${param_index}"
        params.append(datetime.strptime(report_date, '%Y-%m-%d'))
        param_index += 1
    if filing_date:
        query += f" AND fd.date = ${param_index}"
        params.append(datetime.strptime(filing_date, '%Y-%m-%d'))
        param_index += 1
    if latest_n_records:
        query += f" ORDER BY f.id DESC LIMIT ${param_index}"
        params.append(latest_n_records)

    records = await db_connector.run_query(query, params=params)

    if records.empty:
        raise HTTPException(status_code=404, detail="No records found")

    records['data'] = records['data'].apply(json.loads)
    records['report_date'] = records['report_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    records['filing_date'] = records['filing_date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    return records.to_dict(orient='records')


@financials_router.get("/cash_flow", response_model=List[FinancialRecord])
async def get_cash_flow(
        request: Request,
        symbol: Optional[str] = Query(None, description="Stock symbol"),
        report_date: Optional[str] = Query(None, description="Report date in YYYY-MM-DD format"),
        filing_date: Optional[str] = Query(None, description="Filing date in YYYY-MM-DD format"),
        latest_n_records: Optional[int] = Query(None, description="Fetch the latest N records")
):
    user_id, is_api_key = await get_user_id_and_request_type(request)
    data = await fetch_financial_data("cash_flow", symbol, report_date, filing_date, latest_n_records)
    record_count = len(data)
    if is_api_key:
        await update_api_usage_count(user_id, "cash_flow", record_count)
    return data


@financials_router.get("/balance_sheet", response_model=List[FinancialRecord])
async def get_balance_sheet(
        request: Request,
        symbol: Optional[str] = Query(None, description="Stock symbol"),
        report_date: Optional[str] = Query(None, description="Report date in YYYY-MM-DD format"),
        filing_date: Optional[str] = Query(None, description="Filing date in YYYY-MM-DD format"),
        latest_n_records: Optional[int] = Query(None, description="Fetch the latest N records")
):
    user_id, is_api_key = await get_user_id_and_request_type(request)
    data = await fetch_financial_data("balance_sheet", symbol, report_date, filing_date, latest_n_records)
    record_count = len(data)
    if is_api_key:
        await update_api_usage_count(user_id, "balance_sheet", record_count)
    return data


@financials_router.get("/income", response_model=List[FinancialRecord])
async def get_income(
        request: Request,
        symbol: Optional[str] = Query(None, description="Stock symbol"),
        report_date: Optional[str] = Query(None, description="Report date in YYYY-MM-DD format"),
        filing_date: Optional[str] = Query(None, description="Filing date in YYYY-MM-DD format"),
        latest_n_records: Optional[int] = Query(None, description="Fetch the latest N records")
):
    user_id, is_api_key = await get_user_id_and_request_type(request)
    data = await fetch_financial_data("income", symbol, report_date, filing_date, latest_n_records)
    record_count = len(data)
    if is_api_key:
        await update_api_usage_count(user_id, "income", record_count)
    return data
