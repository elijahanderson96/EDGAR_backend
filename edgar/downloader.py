import argparse
from sec_edgar_downloader import Downloader
import sys
import os

# Add the parent directory to the sys.path to locate the edgar module
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.append(root_dir)

from edgar.symbols import symbols


def download_10q_reports(download_date, download_dir, symbol=None):
    dl = Downloader("Elijah", "elijahanderson96@gmail.com", download_dir)

    if symbol:
        symbols_list = [symbol]
    else:
        symbols_list = symbols['symbol'].to_list()

    for sym in symbols_list:
        print(f'Getting 10Q for {sym} on {download_date}')
        dl.get("10-Q", sym, after=download_date, download_details=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download 10-Q reports for a given day')
    parser.add_argument('download_date', type=str, help='Date for the 10-Q reports (YYYY-MM-DD)')
    parser.add_argument('--download_dir', type=str, default='latest_quarterly_reports',
                        help='Directory to download reports')
    parser.add_argument('--symbol', type=str, help='Specific symbol to download the report for')

    args = parser.parse_args()
    download_10q_reports(args.download_date, args.download_dir, args.symbol)
