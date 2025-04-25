import os
import time
import bcrypt
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from jose import jwt, JWTError

from database.database import db_connector

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 600  # Consider reducing this significantly for production

def add_delay(start_time: float, min_delay: float = 0.5):
    """Add a delay to ensure a minimum processing time. This is to prevent timing attacks, where a hacker can determine
    if a username is wrong versus a password based on network response speeds.
    This consistently provides a uniform network response time."""
    end_time = time.monotonic()
    time_taken = end_time - start_time
    delay = max(0, min_delay - time_taken)
    time.sleep(delay)


def verify_password(input_password: str, stored_password_hash: str) -> bool:
    """Verifies a password against a stored hash."""
    return bcrypt.checkpw(
        input_password.encode("utf-8"), stored_password_hash.encode("utf-8")
    )


def authenticate_user(username: str, password: str) -> Optional[dict]:
    """
    Authenticate a user by their username and password.

    Args:
        username (str): The username of the user attempting to authenticate.
        password (str): The password of the user attempting to authenticate.

    Returns:
        dict | None: A dictionary containing the user data (including id and email) if authentication is successful, otherwise None.
    """
    query = "SELECT id, password_hash FROM users.users WHERE username = %s"
    result = db_connector.run_query(query, (username,))
    if not result.empty and verify_password(password, result.at[0, "password_hash"]):
        # Fetch user data including id and email upon successful password verification
        user_data_query = "SELECT id, email FROM users.users WHERE username = %s"
        user_data = db_connector.run_query(user_data_query, (username,), fetch_one=True)
        return user_data # Returns a dict like {'id': 1, 'email': 'user@example.com'} or None
    return None


def create_access_token(user_id: int) -> str:
    """Creates a new JWT access token."""
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"exp": expire, "sub": str(user_id)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(user_id: int) -> str:
    """Creates a new JWT refresh token."""
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode = {"exp": expire, "sub": str(user_id)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# Dependency to get the current user ID from the access token
def get_current_user(request: Request) -> int:
    """
    Dependency function to extract and verify the user ID from the Authorization header.
    Raises HTTPException if the token is missing, invalid, or expired.
    """
    token = request.headers.get("Authorization")
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if token is None:
        raise credentials_exception
    try:
        # Expecting "Bearer <token>"
        scheme, _, param = token.partition(" ")
        if scheme.lower() != "bearer" or not param:
             raise credentials_exception

        payload = jwt.decode(param, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: str = payload.get("sub")
        if user_id_str is None:
            raise credentials_exception
        # Validate that the user exists in the database (optional but recommended)
        # user = db_connector.run_query("SELECT id FROM users.users WHERE id = %s", (int(user_id_str),), fetch_one=True)
        # if user is None:
        #     raise credentials_exception
        return int(user_id_str)
    except JWTError:
        raise credentials_exception
    except (ValueError, TypeError): # Handle case where user_id is not an integer or sub is missing/wrong type
        raise credentials_exception
