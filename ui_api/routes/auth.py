import uuid
import time
import os
import bcrypt
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from fastapi.responses import JSONResponse, HTMLResponse
from jose import jwt, JWTError

from database.database import db_connector
from ui_api.helpers import get_refresh_token_from_cookie
from ui_api.helpers.auth import (
    add_delay,
    authenticate_user,
    create_access_token,
    create_refresh_token,
    get_current_user,
    SECRET_KEY,  # Import SECRET_KEY for refresh token decoding
    ALGORITHM    # Import ALGORITHM for refresh token decoding
)
from ui_api.models.auth import Token, UserLogin, UserRegistration
from helpers.email_utils import send_authentication_email, is_valid_email


auth_router = APIRouter()


@auth_router.post("/login", response_model=Token)
async def login(user_credentials: UserLogin, response: Response):
    """Handles user login, authenticates credentials, updates last login time,
    and returns access/refresh tokens."""
    username = user_credentials.username
    password = user_credentials.password
    start_time = time.monotonic() # For timing attack mitigation

    # authenticate_user now returns {'id': ..., 'email': ...} or None
    user = authenticate_user(username, password)

    if not user:
        add_delay(start_time) # Ensure consistent response time regardless of outcome
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, # Use 401 for authentication failures
            detail="Incorrect username or password"
        )

    # Update last logged in time using user ID
    query = "UPDATE users.users SET last_logged_in = %s WHERE id = %s"
    db_connector.run_query(query, (datetime.now(), user["id"]), return_df=False)

    # Create tokens using user ID
    access_token = create_access_token(user["id"])
    refresh_token = create_refresh_token(user["id"])

    # Set refresh token in secure, http-only cookie
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=True, # Set Secure=True for HTTPS only
        samesite="strict" # Use 'strict' or 'lax'
    )

    add_delay(start_time) # Ensure consistent response time

    # Return email directly from the authenticated user dict
    return {"access_token": access_token, "token_type": "bearer", "email": user["email"]}


@auth_router.post("/register")
async def register(user_registration: UserRegistration):
    """Handles new user registration, including validation, hashing password,
    storing user data, and sending an authentication email."""
    username = user_registration.username
    password = user_registration.password
    email = user_registration.email

    # Validate email format
    if not is_valid_email(email):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email format")

    # Check if the username or email already exists
    query = "SELECT * FROM users.users WHERE username = %s OR email = %s"
    result = db_connector.run_query(query, (username, email))

    if not result.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username or email already exists")

    # Hash the password
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    # Generate a unique authentication token
    auth_token = str(uuid.uuid4())

    # Generate a unique API key
    api_key = str(uuid.uuid4())

    # Insert the new user into the database
    query = """
        INSERT INTO users.users (username, password_hash, email, auth_token, api_key)
        VALUES (%s, %s, %s, %s, %s)
    """
    db_connector.run_query(query, (username, password_hash, email, auth_token, api_key), return_df=False)

    # Construct authentication link using environment variables for flexibility
    host = os.getenv("HOST", "http://localhost:8000") # Default for local dev
    authentication_link = f"{host}/authenticate/{auth_token}"
    send_authentication_email(email, authentication_link)

    return JSONResponse(
        content={"message": "Registration successful. Please check your email for the authentication link."})


@auth_router.get("/authenticate/{auth_token}", response_class=HTMLResponse)
async def authenticate(auth_token: str):
    """Authenticates a user via a token sent by email."""
    # Check if the authentication token exists and is not NULL
    query = "SELECT id FROM users.users WHERE auth_token = %s"
    result = db_connector.run_query(query, (auth_token,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired authentication token")

    # Atomically update the user's authentication status (set auth_token to NULL)
    # This prevents replay attacks if the link is clicked multiple times.
    update_query = "UPDATE users.users SET auth_token = NULL WHERE auth_token = %s RETURNING id"
    updated_user = db_connector.run_query(update_query, (auth_token,), return_df=False, fetch_one=True)

    if not updated_user:
         # Should not happen if the first check passed, but good for robustness
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Failed to validate token.")

    # Fetch the frontend domain from the environment variable
    # Use a more specific name like FRONTEND_DOMAIN
    domain = os.getenv("FRONTEND_DOMAIN", "http://localhost:3000")  # Default to localhost for development

    # Simple HTML content with meta refresh for redirection
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="3;url={domain}/login">
        <title>Authentication Successful</title>
    </head>
    <body>
        <h1>Email authentication successful.</h1>
        <p>You will be redirected to the login page shortly. If not, <a href="{domain}/login">click here</a>.</p>
    </body>
    </html>
    """.format(domain=domain) # Use .format() for clarity

    return HTMLResponse(content=html_content)


@auth_router.post("/refresh", response_model=Token)
async def refresh_token(refresh_token: str = Depends(get_refresh_token_from_cookie)):
    """Refreshes the access token using a valid refresh token from the cookie."""
    if refresh_token is None:
         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token missing")
    try:
        payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: str = payload.get("sub")
        if user_id_str is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload: User ID missing")
        user_id = int(user_id_str) # Convert sub to int
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")
    except (ValueError, TypeError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload: User ID format incorrect")

    # Optional: Check if user still exists or is active in DB
    # user_check_query = "SELECT id FROM users.users WHERE id = %s" # Add AND is_active = TRUE if needed
    # user = db_connector.run_query(user_check_query, (user_id,), fetch_one=True)
    # if not user:
    #     raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")

    new_access_token = create_access_token(user_id)
    # Consider logging instead of printing for production
    # logger.info(f"New access token created for user_id: {user_id}")
    print(f"New access token created for user_id: {user_id}.") # Keep print for now if desired
    return {"access_token": new_access_token, "token_type": "bearer"}


# Note: get_current_user dependency is now imported from ui_api.helpers.auth
# It should be used directly in the route function signature.

@auth_router.post("/regenerate_api_key")
async def regenerate_api_key(user_id: int = Depends(get_current_user)):
    """Regenerates the API key for the currently authenticated user."""
    new_api_key = str(uuid.uuid4())

    # Update the API key in the database for the user identified by the token
    query = "UPDATE users.users SET api_key = %s WHERE id = %s"
    # Assuming run_query doesn't return rowcount, we proceed optimistically.
    # Add error handling if db_connector raises exceptions on failure.
    db_connector.run_query(query, (new_api_key, user_id), return_df=False)

    # Return the new API key in the response
    return JSONResponse(content={"message": "API key regenerated successfully.", "api_key": new_api_key})
