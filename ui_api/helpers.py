from datetime import datetime

from fastapi import Request, HTTPException, status

from database.async_database import db_connector


async def get_user_id_and_request_type(request: Request) -> (int, bool):
    user_id = getattr(request.state, "user_id", None)
    is_api_key = getattr(request.state, "is_api_key", None)
    if user_id is None or is_api_key is None:
        raise HTTPException(status_code=401, detail="User ID or request type not found in request")
    return user_id, is_api_key


def get_refresh_token_from_cookie(request: Request):
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        print('Refresh token not found.')
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired or missing")
    return refresh_token


async def update_api_usage_count(user_id: int, route: str, record_count: int):
    current_date = datetime.utcnow()
    billing_period = current_date.strftime('%m-%Y')

    # Expanded column mapping to include all the various endpoints
    column_mapping = {
        "cash_flow": "cash_flow_route_count",
        "balance_sheet": "balance_sheet_route_count",
        "income": "income_statement_route_count",
        "metadata": "metadata_route_count",
        "assets": "assets_route_count",
        "cash_financing_activities": "cash_financing_activities_route_count",
        "cash_investing_activities": "cash_investing_activities_route_count",
        "cash_operating_activities": "cash_operating_activities_route_count",
        "common_stock": "common_stock_route_count",
        "comprehensive_income": "comprehensive_income_route_count",
        "cost_of_revenue": "cost_of_revenue_route_count",
        "current_assets": "current_assets_route_count",
        "current_liabilities": "current_liabilities_route_count",
        "depreciation_and_amortization": "depreciation_and_amortization_route_count",
        "eps_basic": "eps_basic_route_count",
        "eps_diluted": "eps_diluted_route_count",
        "goodwill": "goodwill_route_count",
        "gross_profit": "gross_profit_route_count",
        "historical_data": "historical_data_route_count",
        "intangible_assets": "intangible_assets_route_count",
        "interest_expense": "interest_expense_route_count",
        "inventory": "inventory_route_count",
        "liabilities": "liabilities_route_count",
        "net_income_loss": "net_income_loss_route_count",
        "operating_expenses": "operating_expenses_route_count",
        "operating_income": "operating_income_route_count",
        "operating_income_loss": "operating_income_loss_route_count",
        "preferred_stock": "preferred_stock_route_count",
        "property_plant_and_equipment": "property_plant_and_equipment_route_count",
        "research_and_development_expense": "research_and_development_expense_route_count",
        "retained_earnings": "retained_earnings_route_count",
        "revenue": "revenue_route_count",
        "shares": "shares_route_count",
        "total_stockholders_equity": "total_stockholders_equity_route_count"
    }

    if route not in column_mapping:
        raise ValueError("Invalid route for updating API usage count")

    route_column = column_mapping[route]

    # Check if the record for the user and billing period already exists
    query_check = f"""
    SELECT id FROM metadata.api_usage
    WHERE user_id = {user_id} AND billing_period = '{billing_period}'
    """
    result = await db_connector.run_query(query_check, return_df=True)

    if not result.empty:
        # Record exists, update the count
        record_id = result.iloc[0]['id']
        query_update = f"""
        UPDATE metadata.api_usage
        SET {route_column} = {route_column} + {record_count}
        WHERE id = {record_id}
        """
        await db_connector.run_query(query_update, return_df=False)
    else:
        # Record does not exist, insert a new record
        query_insert = f"""
        INSERT INTO metadata.api_usage (user_id, billing_period, {route_column})
        VALUES ({user_id}, '{billing_period}', {record_count})
        """
        await db_connector.run_query(query_insert, return_df=False)
