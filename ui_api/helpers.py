from fastapi import Request, HTTPException, status
from datetime import datetime
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


async def update_api_usage(user_id: int, endpoint: str, count: int = 1):
    """
    Updates the API usage for a user in the `api_usage` table.

    Parameters:
        user_id (int): The ID of the user.
        endpoint (str): The API endpoint being accessed.
        count (int): The number of records to increment (default: 1).
    """
    billing_period = datetime.now().strftime("%m-%Y")

    # Prepare the query to upsert (insert or update) the API usage
    upsert_query = """
    INSERT INTO metadata.api_usage (user_id, billing_period, endpoint_name, route_count)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (user_id, billing_period, endpoint_name)
    DO UPDATE SET route_count = metadata.api_usage.route_count + $4;
    """

    try:
        # Run the query with the provided parameters
        await db_connector.run_query(
            upsert_query,
            params=(user_id, billing_period, endpoint, count),
            return_df=False
        )
        print(f"Updated API usage: user_id={user_id}, endpoint={endpoint}, count={count}, from deployment test")
    except Exception as e:
        print(f"Error updating API usage: {e}")
