import logging
from datetime import date
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from starlette import status

from app.cache import get_symbol_id, get_date_id, get_date_from_id, get_symbol_from_id
from app.helpers.security import verify_api_key
# Import the new model and date type
from app.models.financials import FactQueryResponse, CompanyFactBase, CommonFinancialsParams
from app.models.user import User
from database.async_database import db_connector

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/financials",
    tags=["Financial Data"],
    dependencies=[Depends(verify_api_key)] # Apply API key verification to all routes in this router
)

# --- Dependency for Common Financial Parameters ---
async def get_common_financial_params(
    symbol: str = Path(..., title="Stock Symbol", description="The ticker symbol (e.g., AAPL)", min_length=1, max_length=10),
    start_date_str: Optional[str] = Query(None, alias="startDate", description="Start date filter (YYYY-MM-DD)", regex=r"^\d{4}-\d{2}-\d{2}$"),
    end_date_str: Optional[str] = Query(None, alias="endDate", description="End date filter (YYYY-MM-DD)", regex=r"^\d{4}-\d{2}-\d{2}$"),
) -> CommonFinancialsParams:
    """
    Dependency to handle common symbol and date parameters, including validation and cache lookups.
    """
    symbol_upper = symbol.upper()
    symbol_id = get_symbol_id(symbol_upper)
    if symbol_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Symbol '{symbol_upper}' not found.")

    start_date_obj: Optional[date] = None
    start_date_id: Optional[int] = None
    if start_date_str:
        start_date_id = get_date_id(start_date_str)
        if start_date_id is None:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Start date '{start_date_str}' not found or invalid format (YYYY-MM-DD).")
        start_date_obj = get_date_from_id(start_date_id) # Get date object for the model

    end_date_obj: Optional[date] = None
    end_date_id: Optional[int] = None
    if end_date_str:
        end_date_id = get_date_id(end_date_str)
        if end_date_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"End date '{end_date_str}' not found or invalid format (YYYY-MM-DD).")
        end_date_obj = get_date_from_id(end_date_id) # Get date object for the model

    # Optional: Add validation for start_date <= end_date if both are provided
    if start_date_obj and end_date_obj and start_date_obj > end_date_obj:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Start date cannot be after end date.")

    return CommonFinancialsParams(
        symbol=symbol_upper,
        start_date=start_date_obj,
        end_date=end_date_obj,
        symbol_id=symbol_id,
        start_date_id=start_date_id,
        end_date_id=end_date_id
    )


@router.get("/facts/{symbol}/{fact_name}", response_model=FactQueryResponse)
async def get_company_facts(
    fact_name: str = Path(..., title="Fact Name", description="The specific financial fact name (e.g., Revenues)", min_length=1),
    common_params: CommonFinancialsParams = Depends(get_common_financial_params), # Inject common params
    current_user: User = Depends(verify_api_key) # Get user info if needed, already verified
):
    """
    Retrieves historical values for a specific company fact (e.g., Revenues, Assets)
    for a given stock symbol, optionally filtered by date range (based on `end_date_id`).
    Requires a valid API key via the 'X-API-Key' header.
    """
    # Use values from the dependency
    symbol = common_params.symbol
    symbol_id = common_params.symbol_id
    start_date_id = common_params.start_date_id
    end_date_id = common_params.end_date_id

    logger.info(f"User {current_user.username} requested facts for {symbol}, fact: {fact_name}")

    # Build the query dynamically based on filters using resolved IDs
    query_params = [symbol_id, fact_name]
    query = """
        SELECT fact_name, unit, start_date_id, end_date_id, filed_date_id,
               fiscal_year, fiscal_period, form, value, accn
        FROM financials.company_facts
        WHERE symbol_id = $1 AND fact_name = $2
    """
    param_index = 3 # Start parameter index after symbol_id and fact_name

    # Add date filters (filtering on end_date_id)
    if start_date_id is not None:
        query += f" AND end_date_id >= ${param_index}"
        query_params.append(start_date_id)
        param_index += 1
    if end_date_id is not None:
        query += f" AND end_date_id <= ${param_index}"
        query_params.append(end_date_id)
        param_index += 1

    query += " ORDER BY end_date_id DESC;" # Order by date descending

    try:
        results = await db_connector.run_query(query, params=query_params, return_df=False, fetch_one=False) # fetch_one=False returns list of records

        if not results:
            logger.warning(f"No facts found for symbol_id {symbol_id} and fact_name {fact_name} with given filters.")
            # Return empty list instead of 404 if symbol/fact exists but no data for filters
            # raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No facts found for the given criteria.")
            # Use symbol from common_params
            return FactQueryResponse(symbol=common_params.symbol, fact_name=fact_name, data=[])


        # Process results, converting date_ids back to dates using cache
        processed_data = []
        for record_dict in (dict(record) for record in results): # Convert records to dicts
            start_dt = get_date_from_id(record_dict.get('start_date_id'))
            end_dt = get_date_from_id(record_dict.get('end_date_id'))
            filed_dt = get_date_from_id(record_dict.get('filed_date_id'))

            # Handle cases where date_id might not be in cache (should be rare if cache is complete)
            if start_dt is None or end_dt is None or filed_dt is None:
                 logger.warning(f"Could not resolve date IDs for record: {record_dict}. Skipping.")
                 continue

            # Create the Pydantic model instance for the response item
            fact_data = CompanyFactBase(
                fact_name=record_dict['fact_name'],
                unit=record_dict['unit'],
                start_date=start_dt,
                end_date=end_dt,
                filed_date=filed_dt,
                fiscal_year=record_dict['fiscal_year'],
                fiscal_period=record_dict['fiscal_period'],
                form=record_dict['form'],
                value=record_dict['value'], # Ensure type matches model (float)
                accn=record_dict['accn']
            )
            processed_data.append(fact_data)

        # Use symbol from common_params
        return FactQueryResponse(symbol=common_params.symbol, fact_name=fact_name, data=processed_data)

    except Exception as e:
        logger.error(f"Error fetching facts for {symbol}/{fact_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error retrieving financial data.")

# Add more routes here later, e.g., for specific facts like /revenue, /assets, etc.
# Or a more generic search endpoint.
