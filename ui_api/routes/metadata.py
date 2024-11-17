from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Query, Request
from database.async_database import db_connector
from ui_api.helpers import get_user_id_and_request_type, update_api_usage

metadata_router = APIRouter()


# Helper function to build and execute metadata queries
async def fetch_metadata(
    symbol: str, fact_name: Optional[str], start_date: Optional[str], end_date: Optional[str]
) -> Dict[str, Any]:
    """
    Fetch metadata for the specified symbol, optional fact_name, and optional date range.
    """
    query_conditions = ["s.symbol = $1"]
    query_params = [symbol]
    param_index = 2

    # Add optional fact_name filter
    if fact_name:
        query_conditions.append(f"cf.fact_name = ${param_index}")
        query_params.append(fact_name)
        param_index += 1

    # Date range filtering
    if start_date:
        query_conditions.append(f"d_end.date >= ${param_index}")
        query_params.append(datetime.strptime(start_date, "%Y-%m-%d").date())
        param_index += 1
    if end_date:
        query_conditions.append(f"d_start.date <= ${param_index}")
        query_params.append(datetime.strptime(end_date, "%Y-%m-%d").date())
        param_index += 1

    # Combine conditions into the WHERE clause
    where_clause = " AND ".join(query_conditions)
    # Metadata queries
    query_total_count = f"""
    SELECT COUNT(*) AS record_count
    FROM financials.company_facts cf
    JOIN metadata.symbols s ON cf.symbol_id = s.symbol_id
    LEFT JOIN metadata.dates d_start ON cf.start_date_id = d_start.date_id
    LEFT JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
    WHERE {where_clause}
    """
    query_date_range = f"""
    SELECT MIN(d_start.date) AS start_date, MAX(d_end.date) AS end_date
    FROM financials.company_facts cf
    JOIN metadata.symbols s ON cf.symbol_id = s.symbol_id
    LEFT JOIN metadata.dates d_start ON cf.start_date_id = d_start.date_id
    LEFT JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
    WHERE {where_clause}
    """
    query_fact_catalog = f"""
    SELECT cf.fact_name, COUNT(*) AS record_count
    FROM financials.company_facts cf
    JOIN metadata.symbols s ON cf.symbol_id = s.symbol_id
    LEFT JOIN metadata.dates d_start ON cf.start_date_id = d_start.date_id
    LEFT JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
    WHERE {where_clause}
    GROUP BY cf.fact_name
    ORDER BY cf.fact_name
    """

    # Execute queries
    total_count_result = await db_connector.run_query(query_total_count, params=query_params)
    date_range_result = await db_connector.run_query(query_date_range, params=query_params)
    fact_catalog_result = await db_connector.run_query(query_fact_catalog, params=query_params)

    # Convert fact catalog result into a dictionary
    fact_catalog = (
        fact_catalog_result.to_dict(orient="records") if not fact_catalog_result.empty else []
    )

    return {
        "symbol": symbol,
        "fact_name": fact_name,
        "total_records": int(total_count_result.iloc[0]["record_count"])
        if not total_count_result.empty
        else 0,
        "date_range": {
            "start_date": date_range_result.iloc[0]["start_date"]
            if not date_range_result.empty
            else None,
            "end_date": date_range_result.iloc[0]["end_date"]
            if not date_range_result.empty
            else None,
        },
        "fact_catalog": fact_catalog,  # List of fact names and their record counts
    }


@metadata_router.get("/metadata", response_model=Dict[str, Any])
async def get_metadata(
    request: Request,
    symbol: str = Query(..., description="Stock symbol (mandatory)"),
    fact_name: Optional[str] = Query(None, description="Fact name to filter (optional)"),
    start_date: Optional[str] = Query(
        None, description="Start date for filtering (optional, format: YYYY-MM-DD)"
    ),
    end_date: Optional[str] = Query(
        None, description="End date for filtering (optional, format: YYYY-MM-DD)"
    ),
):
    """
    Endpoint to fetch metadata information from the company_facts table.
    """
    try:
        # Fetch metadata
        metadata = await fetch_metadata(symbol, fact_name, start_date, end_date)

        # Update API usage
        user_id, is_api_key = await get_user_id_and_request_type(request)
        if is_api_key:
            await update_api_usage(user_id, "metadata", metadata["total_records"])

        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
