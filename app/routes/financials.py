import logging
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from fastapi.security import APIKeyHeader
from starlette import status

from app.cache import get_symbol_id, get_date_id, get_date_from_id
from app.helpers import users as user_helpers
# Import the new model and date type
from app.models.financials import FactQueryResponse, CompanyFactBase, CommonFinancialsParams, SymbolMetadataResponse, FactMetadata # Add FactMetadata
from app.models.user import User
from database.async_database import db_connector

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/financials",
    tags=["Financial Data"],
    # dependencies=[Depends(verify_api_key)] # Dependency will be defined below
)

# --- API Key Verification Dependency (Moved Here) ---
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)  # Set auto_error=False for custom handling


async def verify_api_key(api_key: str = Depends(api_key_header)) -> User:
    """
    Dependency to verify the API key provided in the 'X-API-Key' header.
    Returns the authenticated user or raises HTTPException 401/403.
    Moved here to break circular import.
    """
    if not api_key:
        logger.warning("API Key missing from request header 'X-API-Key'")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key missing",
        )

    user = await user_helpers.get_user_by_api_key(api_key)
    if user is None:
        logger.warning(f"Invalid API Key received: {api_key[:4]}...{api_key[-4:]}")  # Log partial key for debugging
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )

    # Optionally, check if the user associated with the API key is active/verified
    if not user.is_authenticated:
        logger.warning(f"API Key belongs to unverified user: {user.username} (ID: {user.id})")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account associated with this API key is not verified.",
        )

    logger.info(f"API Key verified for user: {user.username} (ID: {user.id})")
    # Return the user object (Pydantic model)
    return User.model_validate(user)


# Apply the dependency to the router *after* defining it
router.dependencies.append(Depends(verify_api_key))


# --- Dependency for Common Financial Parameters ---
async def get_common_financial_params(
        symbol: str = Path(..., title="Stock Symbol", description="The ticker symbol (e.g., AAPL)", min_length=1,
                           max_length=10),
        start_date_str: Optional[str] = Query(None, alias="startDate", description="Start date filter (YYYY-MM-DD)",
                                              regex=r"^\d{4}-\d{2}-\d{2}$"),
        end_date_str: Optional[str] = Query(None, alias="endDate", description="End date filter (YYYY-MM-DD)",
                                            regex=r"^\d{4}-\d{2}-\d{2}$"),
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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Start date '{start_date_str}' not found or invalid format (YYYY-MM-DD).")
        start_date_obj = get_date_from_id(start_date_id)  # Get date object for the model

    end_date_obj: Optional[date] = None
    end_date_id: Optional[int] = None
    if end_date_str:
        end_date_id = get_date_id(end_date_str)
        if end_date_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"End date '{end_date_str}' not found or invalid format (YYYY-MM-DD).")
        end_date_obj = get_date_from_id(end_date_id)  # Get date object for the model

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
        fact_name: str = Path(..., title="Fact Name", description="The specific financial fact name (e.g., Revenues)",
                              min_length=1),
        common_params: CommonFinancialsParams = Depends(get_common_financial_params),  # Inject common params
        current_user: User = Depends(verify_api_key)  # Get user info if needed, already verified
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
    param_index = 3  # Start parameter index after symbol_id and fact_name

    # Add date filters (filtering on end_date_id)
    if start_date_id is not None:
        query += f" AND end_date_id >= ${param_index}"
        query_params.append(start_date_id)
        param_index += 1
    if end_date_id is not None:
        query += f" AND end_date_id <= ${param_index}"
        query_params.append(end_date_id)
        param_index += 1

    query += " ORDER BY end_date_id DESC;"  # Order by date descending

    try:
        # Explicitly state return_df=True (default) for clarity
        results_df = await db_connector.run_query(query, params=query_params, return_df=True)

        # Check if the DataFrame is empty
        if results_df is None or results_df.empty:
            logger.warning(f"No facts found for symbol_id {symbol_id} and fact_name {fact_name} with given filters.")
            # Return empty list instead of 404 if symbol/fact exists but no data for filters
            # raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No facts found for the given criteria.")
            # Use symbol from common_params
            return FactQueryResponse(symbol=common_params.symbol, fact_name=fact_name, data=[])

        # Process results from the DataFrame, converting date_ids back to dates using cache
        processed_data = []
        # Iterate through DataFrame rows
        for index, row in results_df.iterrows():
            # Handle potentially missing start_date_id for point-in-time facts
            start_date_id = row.get('start_date_id')
            start_dt = get_date_from_id(start_date_id) if pd.notna(start_date_id) else None

            # Resolve required dates (end_date, filed_date)
            end_date_id = row.get('end_date_id')
            filed_date_id = row.get('filed_date_id')

            # Check if required date IDs exist and can be resolved
            if pd.isna(end_date_id) or pd.isna(filed_date_id):
                 logger.warning(f"Missing required end_date_id or filed_date_id for row: {row.to_dict()}. Skipping.")
                 continue

            end_dt = get_date_from_id(end_date_id)
            filed_dt = get_date_from_id(filed_date_id)

            # Skip row only if required dates (end_dt, filed_dt) couldn't be resolved
            if end_dt is None or filed_dt is None:
                logger.warning(f"Could not resolve required end_date_id ({end_date_id}) or filed_date_id ({filed_date_id}) for row: {row.to_dict()}. Skipping.")
                continue

            # Create the Pydantic model instance for the response item
            try:
                fact_data = CompanyFactBase(
                    fact_name=row['fact_name'],
                    unit=row['unit'],
                    start_date=start_dt,
                    end_date=end_dt,
                    filed_date=filed_dt,
                    fiscal_year=int(row['fiscal_year']), # Ensure correct type
                    fiscal_period=row['fiscal_period'],
                    form=row['form'],
                    value=float(row['value']), # Ensure correct type (float)
                    accn=row['accn']
                )
                processed_data.append(fact_data)
            except Exception as model_exc:
                logger.error(f"Error creating CompanyFactBase model for row {row.to_dict()}: {model_exc}", exc_info=True)
                # Decide whether to skip the row or raise an error

        # Use symbol from common_params
        return FactQueryResponse(symbol=common_params.symbol, fact_name=fact_name, data=processed_data)

    except Exception as e:
        logger.error(f"Error fetching facts for {symbol}/{fact_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Error retrieving financial data.")


@router.get("/metadata/{symbol}", response_model=SymbolMetadataResponse)
async def get_symbol_metadata(
    symbol: str = Path(..., title="Stock Symbol", description="The ticker symbol (e.g., AAPL)", min_length=1, max_length=10),
    current_user: User = Depends(verify_api_key) # Ensure user is authenticated via API key
):
    """
    Retrieves metadata for a given stock symbol, including available fact names
    and the overall date range (min/max end_date) of available data in the company_facts table.
    Requires a valid API key via the 'X-API-Key' header.
    """
    symbol_upper = symbol.upper()
    logger.info(f"User {current_user.username} requested metadata for symbol: {symbol_upper}")

    # Resolve symbol using cache
    symbol_id = get_symbol_id(symbol_upper)
    if symbol_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Symbol '{symbol_upper}' not found.")

    # Query to get count, min/max date IDs for each fact_name associated with the symbol
    query = """
        SELECT
            fact_name,
            COUNT(*) AS fact_count,
            MIN(end_date_id) AS min_date_id,
            MAX(end_date_id) AS max_date_id
        FROM financials.company_facts
        WHERE symbol_id = $1
        GROUP BY fact_name
        ORDER BY fact_name;
    """
    query_params = [symbol_id]

    try:
        # Fetch multiple rows (one per fact_name) as a DataFrame
        results_df = await db_connector.run_query(query, params=query_params, return_df=True, fetch_one=False)

        # Check if the DataFrame is None or empty
        if results_df is None or results_df.empty:
            # Handle case where symbol exists but has no facts in the table
            logger.warning(f"No facts found in company_facts for symbol_id {symbol_id} ({symbol_upper}).")
            # Return empty list for available_facts
            return SymbolMetadataResponse(symbol=symbol_upper, available_facts=[], overall_min_date=None, overall_max_date=None)

        detailed_facts = []
        overall_min_dt: Optional[date] = None
        overall_max_dt: Optional[date] = None

        # Iterate over the DataFrame rows
        for index, row in results_df.iterrows():
            fact_name = row['fact_name']
            fact_count = row['fact_count']
            min_date_id = row['min_date_id']
            max_date_id = row['max_date_id']

            # Convert date IDs to dates using cache
            # Handle potential pd.NA from database/dataframe
            min_dt = get_date_from_id(min_date_id) if pd.notna(min_date_id) else None
            max_dt = get_date_from_id(max_date_id) if pd.notna(max_date_id) else None

            detailed_facts.append(FactMetadata(
                fact_name=fact_name,
                count=int(fact_count), # Ensure count is int
                min_date=min_dt,
                max_date=max_dt
            ))

            # Update overall min/max dates
            if min_dt:
                overall_min_dt = min(overall_min_dt, min_dt) if overall_min_dt else min_dt
            if max_dt:
                overall_max_dt = max(overall_max_dt, max_dt) if overall_max_dt else max_dt


        return SymbolMetadataResponse(
            symbol=symbol_upper,
            available_facts=detailed_facts,
            overall_min_date=overall_min_dt,
            overall_max_date=overall_max_dt
        )

    except Exception as e:
        logger.error(f"Error fetching metadata for {symbol_upper}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error retrieving symbol metadata.")


# Add more routes here later, e.g., for specific facts like /revenue, /assets, etc.
# Or a more generic search endpoint.
