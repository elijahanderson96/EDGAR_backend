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

# Create the "company_facts" schema
# db.create_schema("company_facts")
#
# # Get the symbol mapping
cik_to_symbol_mapping = resolve_cik_to_symbol_mapping()
#
# # Create tables for each symbol
# for cik, company_info in cik_to_symbol_mapping.items():
#     symbol = company_info["symbol"]
#     table_name = symbol.lower().replace("-", "_")
#
#     columns = {
#         "id": "SERIAL PRIMARY KEY",
#         "fact_key": "TEXT",
#         "label": "TEXT",
#         "description": "TEXT",
#         "accession_number": "TEXT",
#         "end_date": "DATE",
#         "filed_date": "DATE",
#         "form": "TEXT",
#         "fiscal_period": "TEXT",
#         "fiscal_year": "INTEGER",
#         "value": "NUMERIC",
#         "frame": "TEXT"
#     }
#
#     db.create_table(table_name, columns, schema="company_facts")
#
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
        symbol VARCHAR(255) UNIQUE NOT NULL
    );
''', return_df=False)

# Create dates table in metadata schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS metadata.dates (
        date_id SERIAL PRIMARY KEY,
        date DATE UNIQUE NOT NULL
    );
''', return_df=False)

# Create balance_sheet table in financials schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.balance_sheet (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        report_date_id INT REFERENCES metadata.dates(date_id),
        filing_date_id INT REFERENCES metadata.dates(date_id),
        data JSON NOT NULL
    );
''', return_df=False)

# Create cash_flow table in financials schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.cash_flow (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        report_date_id INT REFERENCES metadata.dates(date_id),
        filing_date_id INT REFERENCES metadata.dates(date_id),
        data JSON NOT NULL
    );
''', return_df=False)

# Create income table in financials schema
db.run_query('''
    CREATE TABLE IF NOT EXISTS financials.income (
        id SERIAL PRIMARY KEY,
        symbol_id INT REFERENCES metadata.symbols(symbol_id),
        report_date_id INT REFERENCES metadata.dates(date_id),
        filing_date_id INT REFERENCES metadata.dates(date_id),
        data JSON NOT NULL
    );
''', return_df=False)

# Assuming db_connector is your database connection instance
db.run_query('''
    CREATE TABLE IF NOT EXISTS metadata.api_usage (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users.users(id),
    billing_period VARCHAR(7) NOT NULL,
    cash_flow_route_count INT DEFAULT 0 NOT NULL,
    balance_sheet_route_count INT DEFAULT 0 NOT NULL,
    income_statement_route_count INT DEFAULT 0 NOT NULL,
    stock_prices_route_count INT DEFAULT 0 NOT NULL,
    metadata_route_count INT DEFAULT 0 NOT NULL,
    UNIQUE(user_id, billing_period)
    )
''', return_df=False)


db.run_query('CREATE INDEX idx_api_usage_user_id_month_id ON metadata.api_usage(user_id, billing_period);',
             return_df=False)

# Create indexes for symbols table
db.run_query('CREATE INDEX IF NOT EXISTS idx_symbols_symbol ON metadata.symbols(symbol);', return_df=False)

# Create indexes for dates table
db.run_query('CREATE INDEX IF NOT EXISTS idx_dates_date ON metadata.dates(date);', return_df=False)

# Create indexes for balance_sheet table
db.run_query('CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_id ON financials.balance_sheet(symbol_id);',
             return_df=False)
db.run_query('CREATE INDEX IF NOT EXISTS idx_balance_sheet_report_date_id ON financials.balance_sheet(report_date_id);',
             return_df=False)
db.run_query('CREATE INDEX IF NOT EXISTS idx_balance_sheet_filing_date_id ON financials.balance_sheet(filing_date_id);',
             return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_report_date_id ON financials.balance_sheet(symbol_id, report_date_id);',
    return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_filing_date_id ON financials.balance_sheet(symbol_id, filing_date_id);',
    return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_balance_sheet_report_filing_date_id ON financials.balance_sheet(report_date_id, filing_date_id);',
    return_df=False)

# Create indexes for cash_flow table
db.run_query('CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_id ON financials.cash_flow(symbol_id);', return_df=False)
db.run_query('CREATE INDEX IF NOT EXISTS idx_cash_flow_report_date_id ON financials.cash_flow(report_date_id);',
             return_df=False)
db.run_query('CREATE INDEX IF NOT EXISTS idx_cash_flow_filing_date_id ON financials.cash_flow(filing_date_id);',
             return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_report_date_id ON financials.cash_flow(symbol_id, report_date_id);',
    return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_filing_date_id ON financials.cash_flow(symbol_id, filing_date_id);',
    return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_cash_flow_report_filing_date_id ON financials.cash_flow(report_date_id, filing_date_id);',
    return_df=False)

# Create indexes for income table
db.run_query('CREATE INDEX IF NOT EXISTS idx_income_symbol_id ON financials.income(symbol_id);', return_df=False)
db.run_query('CREATE INDEX IF NOT EXISTS idx_income_report_date_id ON financials.income(report_date_id);',
             return_df=False)
db.run_query('CREATE INDEX IF NOT EXISTS idx_income_filing_date_id ON financials.income(filing_date_id);',
             return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_income_symbol_report_date_id ON financials.income(symbol_id, report_date_id);',
    return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_income_symbol_filing_date_id ON financials.income(symbol_id, filing_date_id);',
    return_df=False)
db.run_query(
    'CREATE INDEX IF NOT EXISTS idx_income_report_filing_date_id ON financials.income(report_date_id, filing_date_id);',
    return_df=False)

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
    db.insert_dataframe(symbols[['symbol']], name='symbols', schema='metadata', if_exists='append')


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
