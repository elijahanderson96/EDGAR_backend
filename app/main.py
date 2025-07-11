import logging
import os

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from jose import JWTError

# Import the auth router
from app.routes import auth
# Import log path - ensure path is correct relative to project root
from config.filepaths import APP_LOGS
# Import the async connector - ensure path is correct relative to project root
from database.async_database import db_connector
# Import cache loading functions
from app.cache import load_symbols_cache, load_dates_cache

# --- Logging Configuration ---
# Ensure the log directory exists
os.makedirs(APP_LOGS, exist_ok=True)
log_file_path = os.path.join(APP_LOGS, 'app.log')

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()  # Also print logs to console
    ]
)
# Get logger for this module
logger = logging.getLogger(__name__)

# --- FastAPI App Initialization ---
app = FastAPI(
    title="SEC Data API",
    description="API for user authentication and accessing SEC financial data.",
    version="0.1.0"
)


# --- Database Connection Pool Management ---
@app.on_event("startup")
async def startup_event():
    """Initialize database connection pool on startup."""
    logger.info("Application startup: Initializing database connection pool...")
    try:
        # Ensure the connector has an initialize method
        if hasattr(db_connector, 'initialize') and callable(db_connector.initialize):
            await db_connector.initialize()
            logger.info("Database connection pool initialized successfully.")
            # Load caches after pool is initialized
            logger.info("Loading caches...")
            await load_symbols_cache()
            await load_dates_cache()
            logger.info("Caches loaded.")
        else:
            logger.warning("Database connector does not have an 'initialize' method.")
            # Optionally attempt cache loading even if initialize is missing, if pool might be created elsewhere
            # logger.info("Attempting to load caches without explicit pool initialization...")
            # await load_symbols_cache()
            # await load_dates_cache()
    except Exception as e:
        logger.error(f"CRITICAL: Failed to initialize database connection pool: {e}", exc_info=True)
        # Depending on the severity, you might want to prevent startup
        # raise SystemExit(f"Could not connect to database: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection pool on shutdown."""
    logger.info("Application shutdown: Closing database connection pool...")
    try:
        # Ensure the connector has a close method
        if hasattr(db_connector, 'close') and callable(db_connector.close):
            await db_connector.close()
            logger.info("Database connection pool closed.")
        else:
            logger.warning("Database connector does not have a 'close' method.")
    except Exception as e:
        logger.error(f"Error closing database connection pool: {e}", exc_info=True)


# --- CORS Middleware ---
# Configure CORS (Cross-Origin Resource Sharing)
# Adjust origins based on your React client's URL(s)
origins = [
    "http://localhost",  # Allow local development (often needed for server itself)
    "http://localhost:3000",  # Default React dev server port
    "http://localhost:5173",  # Default Vite React dev server port
    # Add your deployed frontend URL here, e.g., "https://your-frontend.com"
    # Add your deployed API URL if different and needed for self-calls
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # List of allowed origins
    allow_credentials=True,  # Allow cookies to be sent with requests
    allow_methods=["*"],  # Allows all standard methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


# --- Exception Handlers ---
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Request validation error: {exc.errors()}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )


@app.exception_handler(JWTError)
async def jwt_exception_handler(request: Request, exc: JWTError):
    logger.warning(f"JWT Error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": "Invalid or expired token"},
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An internal server error occurred"},
    )


# --- API Routers ---
# Import routers
from app.routes import auth, financials, options, docs

app.include_router(auth.router)
app.include_router(financials.router)
app.include_router(options.router)
app.include_router(docs.router)


# Add other routers here as your application grows
# e.g., app.include_router(financials.router)

# --- Root Endpoint ---
@app.get("/", tags=["Root"])
async def read_root():
    """Provides a simple welcome message for the API root."""
    logger.info("Root endpoint '/' accessed.")
    return {"message": "Welcome to the SEC Data API"}

# --- Run Instructions ---
# Typically run using: uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
# The following block is for direct execution `python app/main.py` (less common for FastAPI)
# if __name__ == "__main__":
#     import uvicorn
#     logger.info("Starting Uvicorn server directly from main.py")
#     uvicorn.run(app, host="0.0.0.0", port=8000)
