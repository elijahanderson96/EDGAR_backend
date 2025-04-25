import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), "config", ".env"), verbose=False, override=True)
print("Loading environment variables")

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ENV = os.getenv("ENV")

print(f"We are in a {ENV} environment.")

# --- JWT Settings ---
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "default_secret_key_for_development_only")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 7))
EMAIL_VERIFICATION_TOKEN_EXPIRE_HOURS = int(os.getenv("EMAIL_VERIFICATION_TOKEN_EXPIRE_HOURS", 24))
PASSWORD_RESET_TOKEN_EXPIRE_HOURS = int(os.getenv("PASSWORD_RESET_TOKEN_EXPIRE_HOURS", 1))

# --- Database Settings ---
db_config = {
    "host": os.getenv("POSTGRES_HOST_ADDRESS"),
    "port": os.getenv("POSTGRES_PORT"),
    "user": os.getenv("POSTGRES_USERNAME"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "dbname": os.getenv("POSTGRES_NAME"),
}

dsn = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

AA_PASSWORD = os.getenv("AA_PASSWORD")
AA_USERNAME = os.getenv("AA_USERNAME")
