from datetime import datetime

from fastapi import Request, HTTPException, status
from starlette.responses import JSONResponse

from database.async_database import db_connector


async def get_user_id_and_request_type(request: Request) -> (int, bool):
    user_id = getattr(request.state, "user_id", None)
    is_api_key = getattr(request.state, "is_api_key", None)
    if user_id is None or is_api_key is None:
        raise HTTPException(status_code=401, detail="User ID or request type not found in request")
    return user_id, is_api_key


def get_refresh_token_from_cookie(request: Request):
    refresh_token = request.cookies.get("refresh_token")
    print(refresh_token)
    print(request.cookies)
    if not refresh_token:
        print('Refresh token not found.')
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired or missing")
    return refresh_token


async def update_api_usage_count(user_id: int, route: str, record_count: int):
    current_date = datetime.utcnow()
    billing_period = current_date.strftime('%m-%Y')

    column_mapping = {
        "cash_flow": "cash_flow_route_count",
        "balance_sheet": "balance_sheet_route_count",
        "income": "income_statement_route_count",
        "metadata": "metadata_route_count"
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
