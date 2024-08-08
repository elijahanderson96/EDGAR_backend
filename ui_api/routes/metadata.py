from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Query, Request


from database.async_database import db_connector
from ui_api.helpers import get_user_id_and_request_type, update_api_usage_count

metadata_router = APIRouter()


@metadata_router.get("/metadata/details", response_model=Dict[str, Any])
async def get_metadata_details(
    request: Request,
    symbol: Optional[str] = Query(None, description="Stock symbol"),
    report_date: Optional[str] = Query(None, description="Report date in YYYY-MM-DD format"),
    start_date: Optional[str] = Query(None, description="Start date for the report date range in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date for the report date range in YYYY-MM-DD format"),
):
    try:
        query_conditions = []
        query_params = []

        param_counter = 1

        if symbol:
            query_conditions.append(f"metadata.symbols.symbol = ${param_counter}")
            query_params.append(symbol)
            param_counter += 1

        if report_date:
            query_conditions.append(f"metadata.dates.date = ${param_counter}::date")
            query_params.append(datetime.strptime(report_date, '%Y-%m-%d').date())
            param_counter += 1

        if start_date and end_date:
            query_conditions.append(f"metadata.dates.date BETWEEN ${param_counter}::date AND ${param_counter + 1}::date")
            query_params.extend([
                datetime.strptime(start_date, '%Y-%m-%d').date(),
                datetime.strptime(end_date, '%Y-%m-%d').date()
            ])
            param_counter += 2
        elif start_date:
            query_conditions.append(f"metadata.dates.date >= ${param_counter}::date")
            query_params.append(datetime.strptime(start_date, '%Y-%m-%d').date())
            param_counter += 1
        elif end_date:
            query_conditions.append(f"metadata.dates.date <= ${param_counter}::date")
            query_params.append(datetime.strptime(end_date, '%Y-%m-%d').date())
            param_counter += 1

        conditions = " AND ".join(query_conditions) if query_conditions else "1=1"

        tables = ["financials.cash_flow", "financials.balance_sheet", "financials.income"]
        response = {}
        total_record_count = 0

        for table in tables:
            table_name = table.split(".")[1]
            query_count = f"""
            SELECT COUNT(*) as count 
            FROM {table} 
            JOIN metadata.symbols ON {table}.symbol_id = metadata.symbols.symbol_id
            JOIN metadata.dates ON {table}.report_date_id = metadata.dates.date_id
            WHERE {conditions}
            """
            query_date_range = f"""
            SELECT MIN(metadata.dates.date) as start_date, MAX(metadata.dates.date) as end_date 
            FROM {table}
            JOIN metadata.symbols ON {table}.symbol_id = metadata.symbols.symbol_id
            JOIN metadata.dates ON {table}.report_date_id = metadata.dates.date_id
            WHERE {conditions}
            """
            query_latest_date = f"""
            SELECT MAX(metadata.dates.date) as latest_date 
            FROM {table}
            JOIN metadata.symbols ON {table}.symbol_id = metadata.symbols.symbol_id
            JOIN metadata.dates ON {table}.report_date_id = metadata.dates.date_id
            WHERE {conditions}
            """

            result_count = await db_connector.run_query(query_count, tuple(query_params))
            result_date_range = await db_connector.run_query(query_date_range, tuple(query_params))
            result_latest_date = await db_connector.run_query(query_latest_date, tuple(query_params))

            count = result_count.iloc[0]['count'] if not result_count.empty else 0
            total_record_count += count

            response[table_name] = {
                "count": int(count),
                "date_range": {
                    "start_date": result_date_range.iloc[0]['start_date'] if not result_date_range.empty else None,
                    "end_date": result_date_range.iloc[0]['end_date'] if not result_date_range.empty else None
                },
                "latest_report_date": result_latest_date.iloc[0]['latest_date'] if not result_latest_date.empty else None
            }

        # Update API usage count
        user_id, is_api_key = await get_user_id_and_request_type(request)
        if is_api_key:
            await update_api_usage_count(user_id, "metadata", total_record_count)

        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
