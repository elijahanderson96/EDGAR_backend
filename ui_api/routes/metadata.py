from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Query, Request
from database.async_database import db_connector
from ui_api.helpers import get_user_id_and_request_type, update_api_usage_count

metadata_router = APIRouter()

# Define tables that are instantaneous
INSTANTANEOUS_TABLES = [
    "assets", "current_assets", "current_liabilities", "inventory", "liabilities", "property_plant_and_equipment",
    "shares", "total_stockholders_equity"
]


# Helper function to build and execute queries
async def fetch_metadata(table: str, symbol: str, start_date: Optional[str], end_date: Optional[str]):
    query_conditions = ["symbol = $1"]
    query_params = [symbol]

    # Check if the table is in the instantaneous list to adjust date filtering
    if table in INSTANTANEOUS_TABLES:
        # For instantaneous tables, only use `end_date`
        if end_date:
            query_conditions.append("report_date <= $2")
            query_params.append(datetime.strptime(end_date, '%Y-%m-%d').date())
    else:
        # For period tables, use `start_date` and `end_date` as a range
        if start_date and end_date:
            query_conditions.append("report_date BETWEEN $2 AND $3")
            query_params.extend([
                datetime.strptime(start_date, '%Y-%m-%d').date(),
                datetime.strptime(end_date, '%Y-%m-%d').date()
            ])
        elif start_date:
            query_conditions.append("report_date >= $2")
            query_params.append(datetime.strptime(start_date, '%Y-%m-%d').date())
        elif end_date:
            query_conditions.append("report_date <= $2")
            query_params.append(datetime.strptime(end_date, '%Y-%m-%d').date())

    conditions = " AND ".join(query_conditions)

    query_count = f"SELECT COUNT(*) as count FROM {table} WHERE {conditions}"
    query_date_range = f"SELECT MIN(report_date) as start_date, MAX(report_date) as end_date FROM {table} WHERE {conditions}"
    query_latest_date = f"SELECT MAX(report_date) as latest_date FROM {table} WHERE {conditions}"

    result_count = await db_connector.run_query(query_count, tuple(query_params))
    result_date_range = await db_connector.run_query(query_date_range, tuple(query_params))
    result_latest_date = await db_connector.run_query(query_latest_date, tuple(query_params))

    return {
        "symbol": symbol,
        "count": int(result_count.iloc[0]['count']) if not result_count.empty else 0,
        "date_range": {
            "start_date": result_date_range.iloc[0]['start_date'] if not result_date_range.empty else None,
            "end_date": result_date_range.iloc[0]['end_date'] if not result_date_range.empty else None
        },
        "latest_report_date": result_latest_date.iloc[0]['latest_date'] if not result_latest_date.empty else None
    }


@metadata_router.get("/metadata/{table}", response_model=Dict[str, Any])
async def get_metadata(
        request: Request,
        table: str,
        symbol: str,
        start_date: Optional[str] = Query(None,
                                          description="Start date for the report date range in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date for the report date range in YYYY-MM-DD format"),
):
    # Ensure the table is a valid financials metadata view
    if table not in ["cash_flow_mv", "balance_sheet_mv", "income_mv"]:
        raise HTTPException(status_code=400, detail="Invalid metadata table specified")

    try:
        metadata = await fetch_metadata(f"financials.{table}", symbol, start_date, end_date)

        # Update API usage count
        user_id, is_api_key = await get_user_id_and_request_type(request)
        if is_api_key:
            await update_api_usage_count(user_id, "metadata", metadata['count'])

        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
