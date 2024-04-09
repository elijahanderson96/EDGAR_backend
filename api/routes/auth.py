import uuid

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import status
from fastapi.responses import JSONResponse

from database.database import db_connector
from api.models.auth import UserLogin, UserRegistration
from jose import jwt
from datetime import datetime, timedelta
import os
import bcrypt
from helpers.email_utils import send_authentication_email, is_valid_email

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"
auth_router = APIRouter()


@auth_router.post("/login")
async def login(user_credentials: UserLogin):
    username = user_credentials.username
    password = user_credentials.password

    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    # Update last_logged_in
    query = "UPDATE users.users SET last_logged_in = %s WHERE username = %s"
    db_connector.run_query(query, (datetime.now(), username), return_df=False)

    # Generate JWT token
    token = create_jwt_token(user["id"])

    return {"access_token": token, "token_type": "bearer"}


def authenticate_user(username: str, password: str):
    query = "SELECT * FROM users.users WHERE username = %s"
    result = db_connector.run_query(query, (username,))

    if not result.empty and verify_password(password, result.at[0, "password_hash"]):
        return result.iloc[0].to_dict()  # Convert the user row to a dict
    return None


def create_jwt_token(user_id: int):
    expire = datetime.utcnow() + timedelta(minutes=1)  # Token expiration time
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email format"
        )

    # Check if the username or email already exists
    query = "SELECT * FROM users.users WHERE username = %s OR email = %s"
    result = db_connector.run_query(query, (username, email))

    if not result.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username or email already exists")

    # Hash the password
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    # Generate a unique authentication token
    auth_token = str(uuid.uuid4())

    # Insert the new user into the database
    query = "INSERT INTO users.users (username, password_hash, email, auth_token) VALUES (%s, %s, %s, %s)"
    db_connector.run_query(query, (username, password_hash, email, auth_token), return_df=False)

    # Send the authentication email
    authentication_link = f"http://localhost:8000/authenticate/{auth_token}"
    send_authentication_email(email, authentication_link)

    return JSONResponse(
        content={"message": "Registration successful. Please check your email for the authentication link."})


@auth_router.get("/authenticate/{auth_token}")
async def authenticate(auth_token: str):
    # Check if the authentication token exists in the database
    query = "SELECT * FROM users.users WHERE auth_token = %s"
    result = db_connector.run_query(query, (auth_token,))

    if result.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid authentication token")

    # Update the user's authentication status
    query = "UPDATE users.users SET is_authenticated = true WHERE auth_token = %s"
    db_connector.run_query(query, (auth_token,), return_df=False)

    return JSONResponse(content={"message": "Email authentication successful. You can now log in."})
