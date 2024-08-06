from fastapi import Request, HTTPException

async def get_user_id_and_request_type(request: Request) -> (int, bool):
    user_id = getattr(request.state, "user_id", None)
    is_api_key = getattr(request.state, "is_api_key", None)
    if user_id is None or is_api_key is None:
        raise HTTPException(status_code=401, detail="User ID or request type not found in request")
    return user_id, is_api_key
