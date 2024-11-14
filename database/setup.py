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

# Define additional tables for financial metrics

# Shares table in financials schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.shares (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        value NUMERIC,
        frame VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.revenue (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

# Assets Table
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.assets (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

# Liabilities Table
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.liabilities (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

# EPS (Earnings Per Share) Basic Table
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.eps_basic (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

# EPS (Earnings Per Share) Diluted Table
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.eps_diluted (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

# Gross Profit Table
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.net_income_loss (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.cash_operating_activities (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.cash_investing_activities (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.cash_financing_activities (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.current_assets (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.current_liabilities (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.gross_profit (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.operating_income_loss (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.retained_earnings (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.common_stock (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.interest_expense (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.preferred_stock (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.depreciation_and_amortization (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.cost_of_revenue (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.operating_expenses (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.inventory (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.property_plant_and_equipment (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.goodwill (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.intangible_assets (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.total_stockholders_equity (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, end_date_id)
    );
''', return_df=False)

db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.research_and_development_expense (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        accn VARCHAR(20),
        start_date_id INT REFERENCES metadata.dates(date_id),
        end_date_id INT REFERENCES metadata.dates(date_id),
        value NUMERIC,
        UNIQUE(symbol_id, start_date_id, end_date_id)
    );
''', return_df=False)

print("All tables created successfully with unique constraints on symbol_id, start_date_id, and end_date_id.")

create_table_query = """
CREATE TABLE IF NOT EXISTS metadata.api_usage (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    billing_period VARCHAR(7) NOT NULL,  -- Format: MM-YYYY
    metadata_route_count INT DEFAULT 0,
    assets_route_count INT DEFAULT 0,
    cash_financing_activities_route_count INT DEFAULT 0,
    cash_investing_activities_route_count INT DEFAULT 0,
    cash_operating_activities_route_count INT DEFAULT 0,
    common_stock_route_count INT DEFAULT 0,
    comprehensive_income_route_count INT DEFAULT 0,
    cost_of_revenue_route_count INT DEFAULT 0,
    current_assets_route_count INT DEFAULT 0,
    current_liabilities_route_count INT DEFAULT 0,
    depreciation_and_amortization_route_count INT DEFAULT 0,
    eps_basic_route_count INT DEFAULT 0,
    eps_diluted_route_count INT DEFAULT 0,
    goodwill_route_count INT DEFAULT 0,
    gross_profit_route_count INT DEFAULT 0,
    historical_data_route_count INT DEFAULT 0,
    intangible_assets_route_count INT DEFAULT 0,
    interest_expense_route_count INT DEFAULT 0,
    inventory_route_count INT DEFAULT 0,
    liabilities_route_count INT DEFAULT 0,
    net_income_loss_route_count INT DEFAULT 0,
    operating_expenses_route_count INT DEFAULT 0,
    operating_income_route_count INT DEFAULT 0,
    operating_income_loss_route_count INT DEFAULT 0,
    preferred_stock_route_count INT DEFAULT 0,
    property_plant_and_equipment_route_count INT DEFAULT 0,
    research_and_development_expense_route_count INT DEFAULT 0,
    retained_earnings_route_count INT DEFAULT 0,
    revenue_route_count INT DEFAULT 0,
    shares_route_count INT DEFAULT 0,
    total_stockholders_equity_route_count INT DEFAULT 0,
    UNIQUE (user_id, billing_period)  -- Ensures one record per user per billing period
);
"""

# Run the query to create the table
db.run_query(create_table_query, return_df=False)

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
