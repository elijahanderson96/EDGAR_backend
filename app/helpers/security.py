from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException, Depends # Add Depends
from fastapi.security import APIKeyHeader # Import APIKeyHeader
from jose import JWTError, jwt
from passlib.context import CryptContext
from starlette import status

# Import helpers and models needed for API key verification
from app.helpers import users as user_helpers
from app.models.user import User

from config import configs

# Use settings from config/configs.py
SECRET_KEY = configs.JWT_SECRET_KEY
ALGORITHM = configs.JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = configs.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_DAYS = configs.REFRESH_TOKEN_EXPIRE_DAYS
EMAIL_VERIFICATION_TOKEN_EXPIRE_HOURS = configs.EMAIL_VERIFICATION_TOKEN_EXPIRE_HOURS
PASSWORD_RESET_TOKEN_EXPIRE_HOURS = configs.PASSWORD_RESET_TOKEN_EXPIRE_HOURS

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed password."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hashes a plain password."""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    # Ensure 'sub' is a string for JWT standard compliance
    if "sub" in to_encode:
        to_encode["sub"] = str(to_encode["sub"])
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Creates a JWT refresh token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    # Ensure 'sub' is a string
    if "sub" in to_encode:
        to_encode["sub"] = str(to_encode["sub"])
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_verification_token(data: dict) -> str:
    """Creates a JWT email verification token."""
    expire = datetime.now(timezone.utc) + timedelta(hours=EMAIL_VERIFICATION_TOKEN_EXPIRE_HOURS)
    to_encode = data.copy()
    # Ensure 'sub' is a string
    if "sub" in to_encode:
        to_encode["sub"] = str(to_encode["sub"])
    to_encode.update({"exp": expire, "type": "verification"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_password_reset_token(data: dict) -> str:
    """Creates a JWT password reset token."""
    expire = datetime.now(timezone.utc) + timedelta(hours=PASSWORD_RESET_TOKEN_EXPIRE_HOURS)
    to_encode = data.copy()
    # Ensure 'sub' is a string
    if "sub" in to_encode:
        to_encode["sub"] = str(to_encode["sub"])
    to_encode.update({"exp": expire, "type": "password_reset"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str, expected_type: str) -> Optional[dict]:
    """Verifies a JWT token and returns the payload if valid and matches expected type."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        token_type = payload.get("type")
        user_id: Optional[int] = payload.get("sub")  # Changed from str to int based on DB schema
        username: Optional[str] = payload.get("username")  # Added username for potential use

        if user_id is None or token_type != expected_type:
            print(f"Invalid token: type mismatch (expected {expected_type}, got {token_type}) or missing sub.")
            raise credentials_exception

        # JWTError is raised automatically if expired or signature is invalid
        return payload
    except JWTError as e:
        print(f"Token verification failed: {e}")
        raise credentials_exception  # Re-raise as HTTPException


# --- API Key Verification ---
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def verify_api_key(api_key: str = Depends(api_key_header)) -> User:
    """
    Dependency to verify the API key provided in the 'X-API-Key' header.
    Returns the authenticated user or raises HTTPException 401/403.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key missing",
        )

    user = await user_helpers.get_user_by_api_key(api_key)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )

    # Optionally, check if the user associated with the API key is active/verified
    if not user.is_authenticated:
         raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account associated with this API key is not verified.",
        )

    # Return the user object (Pydantic model)
    # Use model_validate for Pydantic v2
    return User.model_validate(user)
