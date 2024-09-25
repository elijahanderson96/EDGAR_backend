from database.database import db_connector


def get_symbol_id(symbol):
    query = 'SELECT symbol_id FROM metadata.symbols WHERE symbol = %s'
    result = db_connector.run_query(query, (symbol,), fetch_one=True)
    if result:
        return result
    else:
        raise ValueError(f"Symbol {symbol} not found in metadata.symbols table.")


def get_date_id(date):
    query = 'SELECT date_id FROM metadata.dates WHERE date = %s'
    result = db_connector.run_query(query, (date,), fetch_one=True)
    if result:
        return result
    else:
        raise ValueError(f"Date {date} not found in metadata.dates table.")
