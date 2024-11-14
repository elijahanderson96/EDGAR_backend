import os
import sys
import subprocess
import datetime
from edgar.symbols import symbols
from models.Extract import Extract
import random

print(symbols)
# Get the current working directory
script_dir = os.getcwd()
root_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.append(root_dir)

symbols = symbols['symbol'].to_list()[0:10]


# random.shuffle(symbols)

#
# import yfinance as yf
# import pandas as pd
#
#
# def get_sorted_symbols_by_market_cap(ticker_symbols):
#     # Dictionary to store market cap for each symbol
#     market_cap_data = {}
#
#     for ticker in ticker_symbols:
#         stock = yf.Ticker(ticker)
#         market_cap = stock.info.get('marketCap')
#
#         if market_cap is not None:
#             market_cap_data[ticker] = market_cap
#         else:
#             print(f"Market cap data not available for {ticker}")
#
#     # Convert to a DataFrame for easy sorting
#     market_cap_df = pd.DataFrame(list(market_cap_data.items()), columns=['Ticker', 'Market Cap'])
#
#     # Sort by Market Cap in descending order
#     sorted_market_cap_df = market_cap_df.sort_values(by='Market Cap', ascending=False).reset_index(drop=True)
#
#     # Return sorted tickers as a list
#     return sorted_market_cap_df['Ticker'].tolist()
#
#
# # Example usage with a subset of popular tickers (expand as needed)
#
# sorted_symbols = get_sorted_symbols_by_market_cap(symbols)
# print("Sorted symbols by market cap:", sorted_symbols)

def run_command(command):
    print(f"Running command: {' '.join(command)}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
                               universal_newlines=True)

    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())

    stderr = process.communicate()[1]
    if stderr:
        print(stderr)

    process.wait()  # Wait for the process to complete


def download_10q_reports(symbol, start_date, end_date):
    download_command = ['python', os.path.join(script_dir, 'edgar', 'downloader.py'), start_date, end_date, '--symbol',
                        symbol]
    run_command(download_command)


def convert_html_to_pngs(symbol):
    detection_command = ['python', os.path.join(script_dir, 'models', 'html_to_img_converter.py'), symbol]
    run_command(detection_command)


def detect_tables(symbol):
    detection_command = ['python', os.path.join(script_dir, 'models', 'TableDetection.py'), symbol]
    run_command(detection_command)


def extract_data(symbol, model_path):
    symbol_dir = os.path.join(script_dir, 'sec-edgar-filings', symbol)
    filings = os.listdir(os.path.join(symbol_dir, '10-Q'))

    for filing in filings:
        print(f'PROCCESSING {filing}...')
        try:
            extractor = Extract(symbol=symbol,
                                filings_dir=os.path.join(symbol_dir, '10-Q', filing),
                                model_path=model_path)

            cash_flow = extractor.extract_cash_flow()
            balance_sheet = extractor.extract_balance_sheet()
            income = extractor.extract_income_statement()
            extractor.save_data(validate=False)

        except Exception as e:
            print(e)


def main(start_date, end_date, model_path):
    sec_edgar_filings_dir = os.path.join(os.getcwd(), 'sec-edgar-filings')
    for symbol in symbols:
        try:
            symbol_dir = os.path.join(sec_edgar_filings_dir, symbol)

            # Check if the directory for this symbol already exists
            if os.path.exists(symbol_dir):
                print(f"Skipping {symbol} as its directory already exists.")
                # continue  # Skip to the next symbol

            print(f"Downloading docs for {symbol}")
            download_10q_reports(symbol, start_date, end_date)

            # print(f"Extracting tables from latest {symbol}")
            # convert_html_to_pngs(symbol)
            #
            # print(f"detecting tables for {symbol}")
            # detect_tables(symbol)
            #
            # print(f"Extracting data from {symbol}")
            # extract_data(symbol, model_path)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description='Process 10-Q reports within a date range')
    # parser.add_argument('start_date', type=str, help='Start date for the 10-Q reports (YYYY-MM-DD)')
    # parser.add_argument('end_date', type=str, help='End date for the 10-Q reports (YYYY-MM-DD)')
    # parser.add_argument('model_path', type=str, help='Path to the model for data extraction')
    #
    # args = parser.parse_args()
    #
    # # Validate date format
    # try:
    #     datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    #     datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
    # except ValueError:
    #     parser.error("Date format should be YYYY-MM-DD")
    #
    # main(args.start_date, args.end_date, args.model_path)
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 12, 31)

    # Format the dates as strings in the format YYYY-MM-DD
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Define the model path
    model_path = r'C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train57\weights\best.pt'

    # Call the main function with the formatted date strings
    main(start_date=start_date_str, end_date=end_date_str, model_path=model_path)
