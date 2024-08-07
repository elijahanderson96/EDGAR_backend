from fastapi import APIRouter, Request, HTTPException

from database.async_database import db_connector
from datetime import datetime

from ui_api.helpers import get_user_id_and_request_type
from ui_api.models.account import AccountInfo

account_router = APIRouter()


@account_router.get("/account", response_model=AccountInfo)
async def get_account_info(request: Request):
    user_id, is_api_key = await get_user_id_and_request_type(request)

    query = """
    SELECT u.username, u.email, COALESCE(au.cash_flow_route_count, 0) AS cash_flow_route_count, 
           COALESCE(au.balance_sheet_route_count, 0) AS balance_sheet_route_count, 
           COALESCE(au.income_statement_route_count, 0) AS income_statement_route_count, 
           COALESCE(au.stock_prices_route_count, 0) AS stock_prices_route_count, 
           COALESCE(au.metadata_route_count, 0) AS metadata_route_count
    FROM users.users u
    LEFT JOIN metadata.api_usage au ON u.id = au.user_id AND au.billing_period = $1
    WHERE u.id = $2
    """

    billing_period = datetime.now().strftime("%m-%Y")

    result = await db_connector.run_query(query, (billing_period, int(user_id)))

    if result.empty:
        raise HTTPException(status_code=404, detail="User not found or no API usage for the current period.")

    user_info = result.iloc[0].to_dict()
    return user_info
