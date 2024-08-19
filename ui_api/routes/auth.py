import logging
import uuid
import time
from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from fastapi.responses import JSONResponse, HTMLResponse

from datetime import datetime, timedelta
import os
import bcrypt
from jose import jwt, JWTError

from database.database import db_connector
from ui_api.helpers import get_refresh_token_from_cookie
from ui_api.models.auth import Token, UserLogin, UserRegistration
from helpers.email_utils import send_authentication_email, is_valid_email

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1
REFRESH_TOKEN_EXPIRE_DAYS = 1  # 1 minute (1 day / 1440 minutes per day)

auth_router = APIRouter()



def add_delay(start_time: float, min_delay: float = 0.5):
    """Add a delay to ensure a minimum processing time. This is to prevent timing attacks, where a hacker can determine
    if a username is wrong versus a password based on network response speeds.
    This consistently provides a uniform network response time."""
    end_time = time.monotonic()
    time_taken = end_time - start_time
    delay = max(0, min_delay - time_taken)
    time.sleep(delay)


@auth_router.post("/login", response_model=Token)
async def login(user_credentials: UserLogin, response: Response):
    """The response argument in the FastAPI login method is automatically provided by FastAPI when the endpoint is called.
    FastAPI injects this Response object, allowing you to modify the response, such as setting cookies or headers.
    This happens behind the scenes and does not require an explicit pass from the React component."""
    username = user_credentials.username
    password = user_credentials.password
    start_time = time.monotonic()

    # Authenticate user
    user = authenticate_user(username, password)
    if not user:
        # Introduce a deliberate delay
        add_delay(start_time)
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    # Update last_logged_in
    query = "UPDATE users.users SET last_logged_in = %s WHERE username = %s"
    db_connector.run_query(query, (datetime.now(), username), return_df=False)

    # Generate JWT tokens. We generate refresh_tokens only when a user logs in.
    access_token = create_access_token(user["id"])
    refresh_token = create_refresh_token(user["id"])

    # Set the refresh token as an HTTP-only cookie
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, samesite="strict")
    # setting the refresh token as an HTTP-only cookie is a more secure approach since it prevents JavaScript
    # from accessing the token, reducing the risk of XSS attacks.
    # The React component needs to be updated to reflect this change and should not
    # expect the refresh_token in the response body.

    add_delay(start_time)

    return {"access_token": access_token, "token_type": "bearer"}


def authenticate_user(username: str, password: str) -> dict | None:
    """
    Authenticate a user by their username and password.

    Args:
        username (str): The username of the user attempting to authenticate.
        password (str): The password of the user attempting to authenticate.

    Returns:
        dict | None: A dictionary containing the user_id if authentication is successful, otherwise None.
    """
    query = "SELECT id, password_hash FROM users.users WHERE username = %s"
    result = db_connector.run_query(query, (username,))
    if not result.empty and verify_password(password, result.at[0, "password_hash"]):
        return result.iloc[0].to_dict()  # Convert the user row to a dict
    return None


def create_access_token(user_id: int):
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"exp": expire, "sub": str(user_id)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(user_id: int):
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode = {"exp": expire, "sub": str(user_id)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_password(input_password: str, stored_password_hash: str):
    return bcrypt.checkpw(
        input_password.encode("utf-8"), stored_password_hash.encode("utf-8")
    )


@auth_router.post("/register")
async def register(user_registration: UserRegistration):
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
    query = "INSERT INTO users.users (username, password_hash, email, auth_token, api_key) VALUES (%s, %s, %s, %s, %s)"
    db_connector.run_query(query, (username, password_hash, email, auth_token, api_key), return_df=False)

    host = os.getenv("HOST")
    # Send the authentication email
    authentication_link = f"{host}/authenticate/{auth_token}"
    send_authentication_email(email, authentication_link)

    return JSONResponse(
        content={"message": "Registration successful. Please check your email for the authentication link."})


@auth_router.get("/authenticate/{auth_token}", response_class=HTMLResponse)
async def authenticate(auth_token: str):
    # Check if the authentication token exists in the database
    query = "SELECT * FROM users.users WHERE auth_token = %s"
    result = db_connector.run_query(query, (auth_token,))

    if result.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid authentication token")

    # Update the user's authentication status
    query = "UPDATE users.users SET auth_token = NULL WHERE auth_token = %s"
    db_connector.run_query(query, (auth_token,), return_df=False)

    # Fetch the domain from the environment variable
    domain = os.getenv("DOMAIN", "http://localhost:3000")  # Default to localhost for development

    # HTML content to redirect to login page
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Authentication Successful</title>
    </head>
    <body>
        <h1>Email authentication successful. Redirecting to login...</h1>
        <script>
            setTimeout(function() {
                window.location.href = '{domain}/login'; // Redirect to the frontend login route
            }, 1000); // 1 second delay
        </script>
    </body>
    </html>
    """.replace("{domain}", domain)

    return HTMLResponse(content=html_content)


@auth_router.post("/refresh", response_model=Token)
async def refresh_token(refresh_token: str = Depends(get_refresh_token_from_cookie)):
    try:
        print("Attempting to decode refresh token.")
        payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        print("Token decoded successfully.")

        user_id: str = payload.get("sub")
        if user_id is None:
            print("Invalid refresh token: user_id is None.")
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "User ID is none"})
        print(f"Refresh token is valid for user_id: {user_id}.")

    except JWTError as e:
        print(f"JWTError: {str(e)}")
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Invalid Refresh Token"})

    new_access_token = create_access_token(user_id)
    print(f"New access token created for user_id: {user_id}.")
    print(new_access_token)
    return {"access_token": new_access_token, "token_type": "bearer"}


# Middleware to protect routes
def get_current_user(request: Request):
    token = request.headers.get("Authorization")
    if token is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = token.replace("Bearer ", "")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return user_id
    except JWTError:
        raise HTTPException(status_code=401, detail="Not authenticated")


@auth_router.post("/regenerate_api_key")
async def regenerate_api_key(request: Request, user_id: int = Depends(get_current_user)):
    # Generate a new API key
    new_api_key = str(uuid.uuid4())

    # Update the API key in the database
    query = "UPDATE users.users SET api_key = %s WHERE id = %s"
    db_connector.run_query(query, (new_api_key, user_id), return_df=False)

    return JSONResponse(content={"message": "API key regenerated successfully.", "new_api_key": new_api_key})
