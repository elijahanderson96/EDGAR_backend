from datetime import datetime, timedelta

import pandas as pd

from database.database import PostgreSQLConnector
from edgar.symbols import resolve_cik_to_symbol_mapping
from config.configs import db_config

# db = PostgreSQLConnector(
#     host="localhost",
#     port="5432",
#     user="postgres",
#     password="password",
# )

db = PostgreSQLConnector(
    host=db_config['host'],
    port=db_config['port'],
    user=db_config['user'],
    password=db_config['password'],
)

# Create the "edgar" database
# db.create_database("edgar")
db.dbname = "edgar"

# # Get the symbol mapping

cik_to_symbol_mapping = resolve_cik_to_symbol_mapping()

db.create_schema('users')

user_columns = {
    'id': 'SERIAL PRIMARY KEY',
    'username': 'VARCHAR(255) UNIQUE NOT NULL',
    'password_hash': 'VARCHAR(255) NOT NULL',
    'email': 'VARCHAR(255) UNIQUE NOT NULL',
    'last_logged_in': 'DATE',
    'auth_token': 'VARCHAR(255)',
    'is_authenticated': 'BOOLEAN',
    'api_key': 'VARCHAR(255) UNIQUE'
}

db.create_table('users', columns=user_columns, schema='users')

db.run_query('CREATE SCHEMA IF NOT EXISTS metadata;', return_df=False)
db.run_query('CREATE SCHEMA IF NOT EXISTS financials;', return_df=False)

# Create symbols table in metadata schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS metadata.symbols (
        symbol_id SERIAL PRIMARY KEY,
        cik VARCHAR(11) UNIQUE NOT NULL,
        symbol VARCHAR(255) UNIQUE NOT NULL,
        title VARCHAR(255)
    );
''', return_df=False)

# Create dates table in metadata schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS metadata.dates (
        date_id SERIAL PRIMARY KEY,
        date DATE UNIQUE NOT NULL
    );
''', return_df=False)

print("Database setup completed.")


def populate_dates_table():
    start_date = datetime.strptime('01-01-1990', '%m-%d-%Y')
    end_date = datetime.strptime('12-31-2050', '%m-%d-%Y')

    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append({'date': current_date.strftime('%Y-%m-%d')})
        current_date += timedelta(days=1)

    # Convert list to DataFrame
    df = pd.DataFrame(dates)

    # Assuming db is an instance of your database connection class
    db.insert_dataframe(df, name='dates', schema='metadata', if_exists='append')


def populate_symbols_table(symbols):
    # Insert DataFrame into metadata.symbols table
    db.insert_dataframe(symbols, name='symbols', schema='metadata', if_exists='append')


populate_dates_table()
populate_symbols_table(symbols=cik_to_symbol_mapping)
db.run_query('GRANT ALL PRIVILEGES ON DATABASE edgar TO read_only;', return_df=False)
db.run_query('GRANT ALL PRIVILEGES ON DATABASE edgar TO doadmin;', return_df=False)

db.run_query('ALTER DATABASE edgar OWNER TO read_only;', return_df=False)

# Grant permissions to the user for all schemas and tables
db.run_query('GRANT USAGE, CREATE ON SCHEMA users, metadata, financials TO doadmin;', return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA users TO doadmin;', return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA metadata TO doadmin;',
             return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA financials TO doadmin;',
             return_df=False)
db.run_query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA users TO doadmin;', return_df=False)
db.run_query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO doadmin;', return_df=False)
db.run_query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA financials TO doadmin;', return_df=False)

db.run_query('GRANT USAGE, CREATE ON SCHEMA users, metadata, financials TO read_only;', return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dagster TO read_only;', return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dagster TO doadmin;', return_df=False)

db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA users TO read_only;', return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA metadata TO read_only;', return_df=False)
db.run_query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA financials TO read_only;', return_df=False)
db.run_query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA users TO read_only;', return_df=False)
db.run_query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO read_only;', return_df=False)
db.run_query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA financials TO read_only;', return_df=False)

db.run_query("""
CREATE TABLE IF NOT EXISTS financials.historical_data (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),  -- Foreign key to metadata.symbols
        date_id INT REFERENCES metadata.dates(date_id),  -- Foreign key to metadata.dates
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        adj_close NUMERIC,
        volume BIGINT,
        CONSTRAINT unique_historical_data UNIQUE(symbol_id, date_id)
    );
    """, return_df=False)

print("All tables created successfully with unique constraints on symbol_id, start_date_id, and end_date_id.")

create_table_query = """
CREATE TABLE IF NOT EXISTS metadata.api_usage (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    billing_period VARCHAR(7) NOT NULL,  -- Format: MM-YYYY
    endpoint_name VARCHAR(255) NOT NULL, -- Name of the route/endpoint (e.g., "assets", "revenue", etc.)
    route_count INT DEFAULT 0,           -- Count of how many times this endpoint was accessed
    UNIQUE (user_id, billing_period, endpoint_name)  -- Ensures one record per user, billing period, and endpoint
);
"""

# Run the query to create the table
db.run_query(create_table_query, return_df=False)
columns = """
            id SERIAL PRIMARY KEY,
            symbol_id INT REFERENCES metadata.symbols(symbol_id),
            fact_name VARCHAR(255),
            unit VARCHAR(31),
            start_date_id INT REFERENCES metadata.dates(date_id),
            end_date_id INT REFERENCES metadata.dates(date_id),
            filed_date_id INT REFERENCES metadata.dates(date_id),
            fiscal_year INT,
            fiscal_period VARCHAR(2),
            form VARCHAR(10),
            value NUMERIC,
            accn VARCHAR(20)
        """
create_table_query = f"CREATE TABLE IF NOT EXISTS financials.company_facts ({columns});"
db.run_query(create_table_query, return_df=False)

db.run_query('''CREATE INDEX idx_company_facts_fact_name ON financials.company_facts (fact_name);
''', return_df=False)

db.run_query('''CREATE INDEX idx_company_facts_symbol_id_fact_name ON financials.company_facts (symbol_id, fact_name);
''', return_df=False)

db.run_query(create_table_query, return_df=False)

columns = """
            id SERIAL PRIMARY KEY,
            symbol_id INT REFERENCES metadata.symbols(symbol_id),
            fact_name VARCHAR(255),
            unit VARCHAR(31),
            start_date_id INT REFERENCES metadata.dates(date_id),
            end_date_id INT REFERENCES metadata.dates(date_id),
            filed_date_id INT REFERENCES metadata.dates(date_id),
            value NUMERIC,
            qoq_growth NUMERIC(5,3),
            yoy_growth NUMERIC(5,3)
        """
create_table_query = f"CREATE TABLE IF NOT EXISTS financials.quarterly_facts ({columns});"
db.run_query(create_table_query, return_df=False)

db.run_query('''CREATE INDEX idx_quarterly_facts_fact_name ON financials.quarterly_facts (fact_name);''', return_df=False)

db.run_query('''CREATE INDEX idx_quarterly_facts_symbol_id_fact_name ON financials.quarterly_facts (symbol_id, fact_name);''', return_df=False)



db.run_query("""

DO $$
DECLARE
    table_record RECORD;
BEGIN
    -- Loop through all tables in the financials schema
    FOR table_record IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'financials'
    LOOP
        -- Alter the owner of each table to "read_only"
        EXECUTE format('ALTER TABLE financials.%I OWNER TO read_only;', table_record.tablename);
    END LOOP;

    -- Repeat the process for the metadata schema
    FOR table_record IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'metadata'
    LOOP
        EXECUTE format('ALTER TABLE metadata.%I OWNER TO read_only;', table_record.tablename);
    END LOOP;
END $$;
""", return_df=False)
