import os
import subprocess
import datetime

from edgar.symbols import symbols


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

    process.wait()


def download_10q_reports(symbol, start_date, end_date):
    download_command = [
        'python', os.path.join(os.getcwd(), 'edgar', 'downloader.py'),
        start_date, end_date, '--symbol', symbol
    ]
    run_command(download_command)


def process_html_file(html_path, symbol):
    process_command = [
        'python', os.path.join(os.getcwd(), 'sec.py'),  # Path to the processing script
        html_path,
        symbol  # Pass the symbol as an argument
    ]
    run_command(process_command)


def main(start_date, end_date):
    sec_edgar_filings_dir = os.path.join(os.getcwd(), 'sec-edgar-filings')
    symbols_list = symbols['symbol'].to_list()[0:2]  # Adjust list as needed

    for symbol in symbols_list:
        try:
            symbol_dir = os.path.join(sec_edgar_filings_dir, symbol)

            if not os.path.exists(symbol_dir):
                download_10q_reports(symbol, start_date, end_date)
                print(f"Downloading docs for {symbol}")
            else:
                print(f"Skipping {symbol} as its directory already exists.")

            # filings_dir = os.path.join(symbol_dir, '10-Q')
            # if os.path.exists(filings_dir):
            #     for filing in os.listdir(filings_dir):
            #         html_path = os.path.join(filings_dir, filing, 'primary-document.html')
            #         if os.path.exists(html_path):
            #             print(f"Processing HTML file for {symbol}: {html_path}")
            #             process_html_file(html_path, symbol)

        except Exception as e:
            print(e)


if __name__ == "__main__":
    # Uncomment for command-line usage:
    # parser = argparse.ArgumentParser(description='Download and process 10-Q reports within a date range')
    # parser.add_argument('start_date', type=str, help='Start date for the 10-Q reports (YYYY-MM-DD)')
    # parser.add_argument('end_date', type=str, help='End date for the 10-Q reports (YYYY-MM-DD)')
    # args = parser.parse_args()
    #
    # try:
    #     datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    #     datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
    # except ValueError:
    #     parser.error("Date format should be YYYY-MM-DD")
    #
    # main(args.start_date, args.end_date)

    # Use direct date assignment for console execution
    start_date = datetime.date(2012, 1, 1)
    end_date = datetime.date(2024, 12, 31)

    # Format the dates as strings in the format YYYY-MM-DD
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    main(start_date_str, end_date_str)
