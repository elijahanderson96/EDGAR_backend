import logging
import uuid  # Import uuid library
from typing import Optional, Dict, Any
from datetime import date

from database.async_database import db_connector  # Use the async connector
from app.models.user import UserInDB, User
from app.models.auth import UserCreate  # Import UserCreate from auth model
from app.helpers.security import get_password_hash

logger = logging.getLogger(__name__)


async def _map_record_to_user_in_db(user_record: Optional[Dict]) -> Optional[UserInDB]:
    """Helper function to map a database record (dict) to UserInDB model."""
    if not user_record:
        return None
    try:
        # Convert asyncpg.Record to a mutable dictionary
        user_data = dict(user_record)
        # Ensure boolean conversion is handled correctly on the dictionary
        user_data['is_authenticated'] = bool(user_data.get('is_authenticated', False))
        # Create the Pydantic model from the dictionary
        return UserInDB(**user_data)
    except Exception as e:
        # Log the original record if conversion fails
        logger.error(f"Error mapping record to UserInDB: {e} - Record: {user_record}")
        return None


async def get_user_by_email(email: str) -> Optional[UserInDB]:
    """Fetches a user by email."""
    query = "SELECT * FROM users.users WHERE email = $1"
    try:
        # Ensure fetch_one=True returns a single record dict or None
        user_record = await db_connector.run_query(query, params=[email], return_df=False, fetch_one=True)
        return await _map_record_to_user_in_db(user_record)
    except Exception as e:
        logger.error(f"Error fetching user by email {email}: {e}")
        return None


async def get_user_by_username(username: str) -> Optional[UserInDB]:
    """Fetches a user by username."""
    query = "SELECT * FROM users.users WHERE username = $1"
    try:
        user_record = await db_connector.run_query(query, params=[username], return_df=False, fetch_one=True)
        return await _map_record_to_user_in_db(user_record)
    except Exception as e:
        logger.error(f"Error fetching user by username {username}: {e}")
        return None


async def get_user_by_id(user_id: int) -> Optional[UserInDB]:
    """Fetches a user by ID."""
    query = "SELECT * FROM users.users WHERE id = $1"
    try:
        user_record = await db_connector.run_query(query, params=[user_id], return_df=False, fetch_one=True)
        return await _map_record_to_user_in_db(user_record)
    except Exception as e:
        logger.error(f"Error fetching user by id {user_id}: {e}")
        return None


async def get_user_by_api_key(api_key: str) -> Optional[UserInDB]:
    """Fetches a user by API key."""
    query = "SELECT * FROM users.users WHERE api_key = $1"
    try:
        user_record = await db_connector.run_query(query, params=[api_key], return_df=False, fetch_one=True)
        return await _map_record_to_user_in_db(user_record)
    except Exception as e:
        logger.error(f"Error fetching user by api_key {api_key}: {e}")
        return None


def generate_api_key() -> str:
    """Generates a unique API key."""
    return str(uuid.uuid4())


async def create_user(user: UserCreate) -> Optional[UserInDB]:
    """Creates a new user in the database with a unique API key."""
    hashed_password = get_password_hash(user.password)
    api_key = generate_api_key()
    query = """
        INSERT INTO users.users (username, email, password_hash, is_authenticated, api_key)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, username, email, password_hash, last_logged_in, auth_token, is_authenticated, api_key;
    """
    # Start as not authenticated, include generated API key
    params = [user.username, user.email, hashed_password, False, api_key]
    try:
        # Ensure run_query with RETURNING works correctly with fetch_one=True
        new_user_record = await db_connector.run_query(query, params=params, return_df=False, fetch_one=True)
        return await _map_record_to_user_in_db(new_user_record)
    except Exception as e:  # Catch potential unique constraint violations etc.
        logger.error(f"Error creating user {user.username} ({user.email}): {e}")
        # Consider checking for specific DB errors like unique violation (e.g., asyncpg.exceptions.UniqueViolationError)
        return None


async def update_user_field(user_id: int, field: str, value: Any) -> bool:
    """Updates a specific field for a user. Returns True on success, False otherwise."""
    # Basic validation to prevent SQL injection via field name
    allowed_fields = ['last_logged_in', 'auth_token', 'is_authenticated', 'password_hash', 'api_key']
    if field not in allowed_fields:
        logger.error(f"Attempted to update disallowed field: {field} for user ID {user_id}")
        return False

    # Use placeholders correctly ($1, $2)
    query = f"UPDATE users.users SET {field} = $1 WHERE id = $2 RETURNING id"
    params = [value, user_id]
    try:
        # Check if the update affected any row by checking the result
        result = await db_connector.run_query(query, params=params, return_df=False, fetch_one=True)
        if result and result['id'] == user_id:
            logger.info(f"Successfully updated {field} for user ID {user_id}")
            return True
        else:
            logger.warning(
                f"Update for {field} did not affect user ID {user_id} (user might not exist or value unchanged)")
            return False  # Or True if no change is not an error state
    except Exception as e:
        logger.error(f"Error updating {field} for user ID {user_id}: {e}")
        return False


async def set_user_authenticated(user_id: int) -> bool:
    """Sets the user's is_authenticated flag to True."""
    return await update_user_field(user_id, 'is_authenticated', True)


async def update_user_last_login(user_id: int) -> bool:
    """Updates the user's last_logged_in timestamp."""
    return await update_user_field(user_id, 'last_logged_in', date.today())


async def update_user_password(user_id: int, new_password: str) -> bool:
    """Updates the user's password hash and clears the auth_token."""
    new_password_hash = get_password_hash(new_password)
    # Use a transaction to ensure both updates succeed or fail together
    async with db_connector.pool.acquire() as connection:
        async with connection.transaction():
            try:
                # Update password
                await connection.execute("UPDATE users.users SET password_hash = $1 WHERE id = $2", new_password_hash,
                                         user_id)
                # Clear auth token
                await connection.execute("UPDATE users.users SET auth_token = NULL WHERE id = $2", user_id)
                logger.info(f"Successfully updated password and cleared auth_token for user ID {user_id}")
                return True
            except Exception as e:
                logger.error(f"Error during password update transaction for user ID {user_id}: {e}")
                # Transaction automatically rolls back
                return False


async def store_refresh_token(user_id: int, refresh_token: str) -> bool:
    """Stores the refresh token for the user."""
    # SECURITY WARNING: Storing raw refresh tokens is insecure.
    # Consider hashing them or using a dedicated, secure token store (e.g., Redis with TTL).
    # hashed_token = get_password_hash(refresh_token) # Example if hashing
    # return await update_user_field(user_id, 'auth_token', hashed_token)
    logger.warning(f"Storing raw refresh token for user ID {user_id}. THIS IS INSECURE for production.")
    return await update_user_field(user_id, 'auth_token', refresh_token)  # Storing raw token


async def verify_refresh_token(user_id: int, provided_token: str) -> bool:
    """Verifies a provided refresh token against the stored one."""
    user = await get_user_by_id(user_id)
    if not user or not user.auth_token:
        logger.warning(f"Refresh token verification failed for user ID {user_id}: User or stored token not found.")
        return False

    # SECURITY WARNING: Direct comparison if storing raw tokens.
    # If storing hashed tokens: return verify_password(provided_token, user.auth_token)
    is_valid = user.auth_token == provided_token
    if not is_valid:
        logger.warning(f"Invalid refresh token provided for user ID {user_id}.")
    return is_valid


async def invalidate_refresh_token(user_id: int) -> bool:
    """Clears the stored refresh token for the user (e.g., on logout or password change)."""
    logger.info(f"Invalidating refresh token for user ID {user_id}")
    return await update_user_field(user_id, 'auth_token', None)
