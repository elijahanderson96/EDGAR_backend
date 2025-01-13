# Database Documentation

## Database Layout

The database is structured into several schemas and tables as follows:

### Schemas
- **users**: Contains user-related data.
- **metadata**: Stores metadata information such as symbols and dates.
- **financials**: Holds financial data including historical data and company facts.

### Tables

#### Users Schema
- **users**: Stores user information with columns:
  - `id`: SERIAL PRIMARY KEY
  - `username`: VARCHAR(255) UNIQUE NOT NULL
  - `password_hash`: VARCHAR(255) NOT NULL
  - `email`: VARCHAR(255) UNIQUE NOT NULL
  - `last_logged_in`: DATE
  - `auth_token`: VARCHAR(255)
  - `is_authenticated`: BOOLEAN
  - `api_key`: VARCHAR(255) UNIQUE

#### Metadata Schema
- **symbols**: Contains symbol information with columns:
  - `symbol_id`: SERIAL PRIMARY KEY
  - `cik`: VARCHAR(11) UNIQUE NOT NULL
  - `symbol`: VARCHAR(255) UNIQUE NOT NULL
  - `title`: VARCHAR(255)

- **dates**: Stores date information with columns:
  - `date_id`: SERIAL PRIMARY KEY
  - `date`: DATE UNIQUE NOT NULL

- **api_usage**: Tracks API usage with columns:
  - `id`: SERIAL PRIMARY KEY
  - `user_id`: INT NOT NULL
  - `billing_period`: VARCHAR(7) NOT NULL
  - `endpoint_name`: VARCHAR(255) NOT NULL
  - `route_count`: INT DEFAULT 0

#### Financials Schema
- **historical_data**: Contains historical financial data with columns:
  - `id`: SERIAL PRIMARY KEY
  - `symbol_id`: INT REFERENCES metadata.symbols(symbol_id)
  - `date_id`: INT REFERENCES metadata.dates(date_id)
  - `open`, `high`, `low`, `close`, `adj_close`: NUMERIC
  - `volume`: BIGINT

- **company_facts**: Stores company facts with columns:
  - `id`: SERIAL PRIMARY KEY
  - `symbol_id`: INT REFERENCES metadata.symbols(symbol_id)
  - `fact_name`: VARCHAR(255)
  - `start_date_id`, `end_date_id`, `filed_date_id`: INT REFERENCES metadata.dates(date_id)
  - `fiscal_year`: INT
  - `fiscal_period`: VARCHAR(2)
  - `form`: VARCHAR(10)
  - `value`: NUMERIC
  - `accn`: VARCHAR(20)

## Database Connectors

### AsyncpgConnector
- **Purpose**: Manages asynchronous connections to a PostgreSQL database using `asyncpg`.
- **Key Methods**:
  - `initialize()`: Initializes the connection pool.
  - `close()`: Closes the connection pool.
  - `run_query()`: Executes a query and optionally returns results as a DataFrame.
  - `drop_existing_rows()`: Drops duplicate rows from a DataFrame based on existing database records.

### PostgreSQLConnector
- **Purpose**: Provides synchronous interaction with a PostgreSQL database using `psycopg2`.
- **Key Methods**:
  - `connect()`: Establishes a connection to the database.
  - `run_query()`: Executes a query and returns results as a DataFrame or a single value.
  - `create_table()`, `add_primary_key()`, `add_unique_key()`, `add_foreign_key()`: Methods for managing database schema.
  - `insert_dataframe()`: Inserts a DataFrame into a database table.
  - `insert_and_return_id()`: Inserts a row and returns the generated ID.
  - `update_table()`, `delete_rows_with_condition()`: Methods for updating and deleting records.

This document provides an overview of the database structure and the connectors used to interact with it.
