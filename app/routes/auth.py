import logging

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Request, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt

from app.helpers import security
from app.helpers import users as user_helpers
from app.models.auth import Token, UserCreate, PasswordResetRequest, PasswordResetConfirm, UserLogin
from app.models.user import User
from helpers.email_utils import send_authentication_email, is_valid_email

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["Authentication"])

# OAuth2 scheme pointing to the /token endpoint for form data login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


# --- Dependency to get current user ---
async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Dependency to verify the access token and return the current user.
    Raises HTTPException 401 if token is invalid, expired, or user not found/verified.
    """
    payload = security.verify_token(token, expected_type="access")  # verify_token now raises HTTPException on failure
    user_id: int = payload.get("sub")  # Already checked for None in verify_token

    user = await user_helpers.get_user_by_id(user_id)
    if user is None:
        logger.warning(f"User not found for ID extracted from token: {user_id}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_authenticated:
        logger.warning(f"Attempt to access protected route by unverified user ID: {user_id}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email not verified",
        )
    # Use Pydantic v2 model_validate instead of from_orm
    return User.model_validate(user)


@router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register_user(user_in: UserCreate, background_tasks: BackgroundTasks, request: Request):
    """Registers a new user and sends a verification email."""
    # Validate email format server-side as well
    if not is_valid_email(user_in.email):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email format")

    # Check if username or email already exists
    existing_user_email = await user_helpers.get_user_by_email(user_in.email)
    if existing_user_email:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")
    existing_user_username = await user_helpers.get_user_by_username(user_in.username)
    if existing_user_username:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already taken")

    # Create user (initially not authenticated)
    db_user = await user_helpers.create_user(user_in)
    if not db_user:
        logger.error(f"Failed to create user database entry for {user_in.username}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not create user")

    # Generate verification token
    verification_token = security.create_verification_token(data={"sub": db_user.id})
    # Construct verification link (ensure request.base_url is correct behind proxy if needed)
    base_url = str(request.base_url).rstrip('/')
    verification_link = f"{base_url}/auth/verify-email?token={verification_token}"

    # Send verification email in the background
    background_tasks.add_task(
        send_authentication_email,  # Assuming this function is suitable
        recipient_email=db_user.email,
        authentication_link=verification_link,
        # You might want to customize the subject/body for verification
        # subject="Verify Your Email Address",
        # body=f"Please click here to verify: {verification_link}"
    )
    logger.info(f"Registration successful for {db_user.username}, verification email queued.")
    # Use Pydantic v2 model_validate instead of from_orm
    return User.model_validate(db_user)


@router.get("/verify-email", status_code=status.HTTP_200_OK)
async def verify_email(token: str):
    """Verifies the user's email address using the provided token."""
    try:
        payload = security.verify_token(token, expected_type="verification")
    except HTTPException as e:
        # Reraise specific errors from verify_token if needed, or return generic error
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Invalid or expired verification token") from e

    user_id = payload.get("sub")  # Already checked for None in verify_token

    user = await user_helpers.get_user_by_id(user_id)
    if not user:
        # This case should be rare if token is valid, but handle defensively
        logger.warning(f"Verification attempt for non-existent user ID: {user_id}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found for this token")

    if user.is_authenticated:
        logger.info(f"Email already verified for user ID: {user_id}")
        return {"message": "Email already verified"}

    success = await user_helpers.set_user_authenticated(user_id)
    if not success:
        logger.error(f"Failed to update user authenticated status for ID: {user_id}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Could not verify email due to a server error")

    logger.info(f"Email successfully verified for user ID: {user_id}")
    return {"message": "Email verified successfully"}


# Endpoint for standard JSON payload login
@router.post("/login", response_model=Token)
async def login_for_access_token_json(user_credentials: UserLogin, response: Response):
    """Authenticates user with username/email and password, returns JWT tokens in response body and optionally cookies."""
    user = await user_helpers.get_user_by_email(user_credentials.username)
    if not user:
        user = await user_helpers.get_user_by_username(user_credentials.username)

    if not user or not security.verify_password(user_credentials.password, user.password_hash):
        logger.warning(f"Failed login attempt for username/email: {user_credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_authenticated:
        logger.warning(f"Login attempt by unverified user: {user.username} ({user.email})")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email not verified. Please check your email for the verification link.",
        )

    # Update last login time (fire and forget, don't block login if fails)
    try:
        await user_helpers.update_user_last_login(user.id)
    except Exception as e:
        logger.error(f"Failed to update last login for user ID {user.id}: {e}")

    # Generate tokens
    access_token = security.create_access_token(data={"sub": user.id, "username": user.username})
    refresh_token = security.create_refresh_token(data={"sub": user.id})

    # Store refresh token securely (WARNING: current implementation is insecure)
    if not await user_helpers.store_refresh_token(user.id, refresh_token):
        logger.error(f"Failed to store refresh token for user ID {user.id}")
        # Decide if login should fail if refresh token can't be stored
        # raise HTTPException(status_code=500, detail="Could not process login.")

    logger.info(f"User {user.username} logged in successfully.")

    # Set tokens in HttpOnly cookies (more secure for web clients)
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,
        samesite="strict",  # or 'strict'
        secure=True,  # Set secure=True if served over HTTPS
        max_age=security.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        samesite="strict",  # or 'strict'
        secure=True,  # Set secure=True if served over HTTPS
        max_age=security.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    )

    # Also return tokens in the body for non-browser clients
    return Token(access_token=access_token, refresh_token=refresh_token)


# Endpoint for OAuth2 form data login (useful for browsable API docs)
@router.post("/token", response_model=Token)
async def login_for_access_token_form(response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    """Authenticates user via form data (username/password), returns JWT tokens."""
    # Reuse the JSON login logic by creating the Pydantic model
    user_credentials = UserLogin(username=form_data.username, password=form_data.password)
    # Call the JSON login handler, passing the response object
    return await login_for_access_token_json(user_credentials=user_credentials, response=response)


@router.post("/refresh", response_model=Token)
async def refresh_access_token(response: Response, request: Request):
    """Provides a new access token using a valid refresh token from cookies."""
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token missing")

    try:
        payload = security.verify_token(refresh_token, expected_type="refresh")
    except HTTPException as e:
        logger.warning(f"Refresh token verification failed: {e.detail}")
        # Clear potentially invalid cookies
        response.delete_cookie("access_token")
        response.delete_cookie("refresh_token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token") from e

    user_id = payload.get("sub")  # Already checked

    # Verify if the refresh token is still valid in the database
    is_valid_in_db = await user_helpers.verify_refresh_token(user_id, refresh_token)
    if not is_valid_in_db:
        logger.warning(f"Refresh token for user ID {user_id} is no longer valid in DB (revoked or changed).")
        # Clear cookies as the token is invalid
        response.delete_cookie("access_token")
        response.delete_cookie("refresh_token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token has been invalidated")

    user = await user_helpers.get_user_by_id(user_id)
    if not user:
        logger.error(f"User {user_id} associated with valid refresh token not found.")
        response.delete_cookie("access_token")
        response.delete_cookie("refresh_token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    # Issue new tokens
    new_access_token = security.create_access_token(data={"sub": user.id, "username": user.username})
    # Rolling refresh tokens: issue a new refresh token and invalidate the old one
    new_refresh_token = security.create_refresh_token(data={"sub": user.id})
    if not await user_helpers.store_refresh_token(user.id, new_refresh_token):
        logger.error(f"Failed to store new refresh token for user ID {user.id} during refresh.")
        # Decide how to handle this - maybe allow login but log error?
        # For now, raise error as it might indicate DB issue.
        raise HTTPException(status_code=500, detail="Could not process token refresh.")

    logger.info(f"Access token refreshed for user {user.username}")

    # Set new tokens in cookies
    response.set_cookie(
        key="access_token",
        value=f"Bearer {new_access_token}",
        httponly=True, samesite="lax", secure=True,  # Adjust secure based on HTTPS
        max_age=security.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )
    response.set_cookie(
        key="refresh_token",
        value=new_refresh_token,
        httponly=True, samesite="lax", secure=True,  # Adjust secure based on HTTPS
        max_age=security.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    )

    # Also return tokens in body
    return Token(access_token=new_access_token, refresh_token=new_refresh_token)


@router.post("/logout", status_code=status.HTTP_200_OK)
async def logout(response: Response, request: Request):
    """Invalidates the refresh token and clears cookies."""
    refresh_token = request.cookies.get("refresh_token")
    user_id = None
    if refresh_token:
        try:
            # Decode token *without* verifying expiry to get user_id for invalidation
            payload = jwt.decode(refresh_token, security.SECRET_KEY, algorithms=[security.ALGORITHM],
                                 options={"verify_exp": False})
            if payload.get("type") == "refresh":
                user_id = payload.get("sub")
        except JWTError as e:
            logger.warning(f"Error decoding refresh token during logout: {e}")
            # Proceed to clear cookies even if token is malformed

    if user_id:
        # Invalidate the refresh token in the database
        await user_helpers.invalidate_refresh_token(user_id)
        logger.info(f"Refresh token invalidated for user ID {user_id} during logout.")
    else:
        logger.info("Logout called without a valid refresh token user ID.")

    # Clear cookies on the client side
    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")
    logger.info("Logout successful, cookies cleared.")
    return {"message": "Logout successful"}


@router.post("/request-password-reset", status_code=status.HTTP_200_OK)
async def request_password_reset(request_data: PasswordResetRequest, background_tasks: BackgroundTasks,
                                 request: Request):
    """Sends a password reset email if the user exists."""
    if not is_valid_email(request_data.email):
        # Don't reveal invalid format, treat like non-existent user
        logger.info(f"Password reset requested for invalid email format: {request_data.email}")
        return {"message": "If an account with this email exists, a password reset link has been sent."}

    user = await user_helpers.get_user_by_email(request_data.email)
    if user:
        # Generate password reset token
        reset_token = security.create_password_reset_token(data={"sub": user.id})
        base_url = str(request.base_url).rstrip('/')
        # TODO: Update frontend URL/path for the password reset page/component
        reset_link = f"http://localhost:3000/reset-password?token={reset_token}"  # Example frontend link

        # Send email (adapt send_authentication_email or create a new function)
        # It's better to have a dedicated function for password reset emails
        background_tasks.add_task(
            send_authentication_email,  # Replace with dedicated function if possible
            recipient_email=user.email,
            authentication_link=reset_link,  # Pass the reset link
            # Customize subject/body if using generic function:
            # subject="Password Reset Request",
            # body=f"Click here to reset your password: {reset_link}"
        )
        logger.info(f"Password reset email queued for {user.email}")
    else:
        # Do not reveal if the email exists - security best practice
        logger.info(f"Password reset requested for non-existent or unverified email: {request_data.email}")

    # Always return success to prevent email enumeration
    return {"message": "If an account with this email exists, a password reset link has been sent."}


@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(reset_data: PasswordResetConfirm):
    """Resets the user's password using a valid token."""
    try:
        payload = security.verify_token(reset_data.token, expected_type="password_reset")
    except HTTPException as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Invalid or expired password reset token") from e

    user_id = payload.get("sub")  # Already checked

    # Update password and invalidate refresh token
    success = await user_helpers.update_user_password(user_id, reset_data.new_password)
    if not success:
        logger.error(f"Failed to reset password for user ID {user_id}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Could not reset password due to a server error")

    logger.info(f"Password successfully reset for user ID {user_id}")
    return {"message": "Password has been reset successfully."}


# Example protected route using the dependency
@router.get("/users/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_user)):
    """Gets the current logged-in user's information."""
    logger.info(f"Accessing /users/me for user: {current_user.username}")
    return current_user
