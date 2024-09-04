import argparse
from sec_edgar_downloader import Downloader
import sys
import os
from datetime import datetime

# Add the parent directory to the sys.path to locate the edgar module
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.append(root_dir)

from edgar.symbols import symbols


def download_10q_reports(start_date, end_date, download_dir, symbol=None):
    dl = Downloader("Elijah", "elijahanderson96@gmail.com", download_dir)

    if symbol:
        symbols_list = [symbol]
    else:
        symbols_list = symbols['symbol'].to_list()

    for sym in symbols_list:
        print(f'Getting 10Q for {sym} from {start_date} to {end_date}')
        dl.get("10-Q", sym, after=start_date, before=end_date, download_details=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download 10-Q reports for a given date range')
    parser.add_argument('start_date', type=str, help='Start date for the 10-Q reports (YYYY-MM-DD)')
    parser.add_argument('end_date', type=str, help='End date for the 10-Q reports (YYYY-MM-DD)')
    parser.add_argument('--download_dir', type=str, default='',
                        help='Directory to download reports')
    parser.add_argument('--symbol', type=str, help='Specific symbol to download the report for')

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        parser.error("Date format should be YYYY-MM-DD")

    download_10q_reports(args.start_date, args.end_date, args.download_dir, args.symbol)
