from fastapi import APIRouter, Request, HTTPException
from database.async_database import db_connector
from datetime import datetime
from ui_api.helpers import get_user_id_and_request_type
from ui_api.models.account import AccountInfo

account_router = APIRouter()

@account_router.get("/account", response_model=AccountInfo)
async def get_account_info(request: Request):
    user_id, is_api_key = await get_user_id_and_request_type(request)

    query_user = """
    SELECT u.username, u.email, u.api_key
    FROM users.users u
    WHERE u.id = $1
    """

    query_usage = """
    SELECT COALESCE(endpoint_name, 'Unknown') AS endpoint_name, COALESCE(SUM(route_count), 0) AS route_count
    FROM metadata.api_usage
    WHERE user_id = $1 AND billing_period = $2
    GROUP BY endpoint_name
    """

    billing_period = datetime.now().strftime("%m-%Y")

    # Fetch user info
    user_result = await db_connector.run_query(query_user, (int(user_id),))
    if user_result.empty:
        raise HTTPException(status_code=404, detail="User not found.")

    # Fetch usage data
    usage_result = await db_connector.run_query(query_usage, (int(user_id), billing_period))

    # Handle case where `endpoint_name` might not exist
    if 'endpoint_name' not in usage_result.columns or 'route_count' not in usage_result.columns:
        usage_dict = {}
    else:
        usage_dict = usage_result.set_index('endpoint_name')['route_count'].to_dict()
    print('HELLO WORLD')
    # Prepare response
    user_info = user_result.iloc[0].to_dict()
    user_info['usage'] = usage_dict  # Add usage details under 'usage'

    return user_info
