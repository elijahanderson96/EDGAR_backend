from database import PostgreSQLConnector
from edgar.symbols import resolve_cik_to_symbol_mapping

connector = PostgreSQLConnector(
    host="localhost",
    port="5432",
    user="postgres",
    password="password",
)

# # Create the "edgar" database
connector.create_database("edgar")
connector.dbname = "edgar"

# Create the "company_facts" schema
connector.create_schema("company_facts")

# Get the symbol mapping
cik_to_symbol_mapping = resolve_cik_to_symbol_mapping()

# Create tables for each symbol
for cik, company_info in cik_to_symbol_mapping.items():
    symbol = company_info["symbol"]
    table_name = symbol.lower().replace("-", "_")

    columns = {
        "id": "SERIAL PRIMARY KEY",
        "fact_key": "TEXT",
        "label": "TEXT",
        "description": "TEXT",
        "accession_number": "TEXT",
        "end_date": "DATE",
        "filed_date": "DATE",
        "form": "TEXT",
        "fiscal_period": "TEXT",
        "fiscal_year": "INTEGER",
        "value": "NUMERIC",
        "frame": "TEXT"
    }

    connector.create_table(table_name, columns, schema="company_facts")


connector.create_schema('users')

user_columns = {
    'id': 'SERIAL PRIMARY KEY',
    'username': 'VARCHAR(255) UNIQUE NOT NULL',
    'password_hash': 'VARCHAR(255) NOT NULL',
    'email': 'VARCHAR(255) UNIQUE NOT NULL',
    'last_logged_in': 'DATE',
    'auth_token': 'VARCHAR(255)',
    'is_authenticated': 'BOOLEAN'
}

connector.create_table('users', columns=user_columns, schema='users')
