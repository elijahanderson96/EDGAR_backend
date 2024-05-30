import time

import yfinance as yf
from database.database import db_connector
from edgar.symbols import symbols


def load_historical_prices(symbol):
        try:
            stock_data = yf.download(symbol)
            print(stock_data)
            stock_data['symbol'] = symbol
            stock_data.reset_index(inplace=True)  # Reset the index to convert 'Date' to a regular column
            stock_data.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
                                       'Volume': 'volume'}, inplace=True)
            stock_data['date'] = stock_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))  # Convert 'date' to string format

            db_connector.insert_dataframe(stock_data, name='historical_prices', schema='stock_prices', if_exists='append')
            return stock_data
        except Exception as e:
            print(f'Error inserting {symbol} in database: {str(e)}')


if __name__ == '__main__':
    print(symbols)
    symbols = symbols['symbol'].to_list()
    [load_historical_prices(symbol) for symbol in symbols]
