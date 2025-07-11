from pydantic import BaseModel, EmailStr, ConfigDict
from typing import Optional
from datetime import date


class UserBase(BaseModel):
    username: str
    email: EmailStr


class UserInDBBase(UserBase):
    id: int
    is_authenticated: bool = False
    last_logged_in: Optional[date] = None
    api_key: Optional[str] = None

    # Use model_config instead of Config class for Pydantic v2
    model_config = ConfigDict(from_attributes=True)


# Properties to return to client
class User(UserInDBBase):
    pass


# Properties stored in DB
class UserInDB(UserInDBBase):
    password_hash: str
    auth_token: Optional[str] = None  # To store hashed refresh token or verification/reset tokens if needed
