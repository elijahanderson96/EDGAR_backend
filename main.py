import os
import sys
import time
import subprocess
import datetime
from edgar.symbols import symbols
from models.Extract import Extract

# Get the current working directory
script_dir = os.getcwd()
root_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.append(root_dir)


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


def download_10q_reports(symbol, download_date):
    download_command = ['python', os.path.join(script_dir, 'edgar', 'downloader.py'), download_date, '--symbol', symbol]
    run_command(download_command)
    time.sleep(1)  # Add delay between downloads if needed


def detect_tables(symbol):
    detection_command = ['python', os.path.join(script_dir, 'models', 'table_detection.py'), symbol]
    run_command(detection_command)
    time.sleep(1)  # Add delay between detections if needed


def classify_tables(symbol):
    classify_command = ['python', os.path.join(script_dir, 'models', 'Classify.py'), symbol]
    run_command(classify_command)
    time.sleep(1)  # Add delay between classifications if needed


def extract_data(symbol, model_path):
    symbol_dir = os.path.join(script_dir, 'latest_quarterly_reports', 'sec-edgar-filings', symbol)
    filings = os.listdir(os.path.join(symbol_dir, '10-Q'))

    for filing in filings:
        print(f'PROCESSING {filing}...')
        extractor = Extract(symbol=symbol,
                            filings_dir=os.path.join(symbol_dir, '10-Q', filing),
                            model_path=model_path)

        cash_flow, balance_sheet, income_statement = extractor.run()
        extractor.save_data()


def main():
    download_date = (datetime.datetime.now() - datetime.timedelta(weeks=52)).strftime('%Y-%m-%d')
    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train36\weights\best.pt"

    for symbol in symbols['symbol'].to_list()[3:]:
        print(f"Downloading docs for {symbol}")
        download_10q_reports(symbol, download_date)

        print(f"Extracting tables from latest {symbol}")
        detect_tables(symbol)

        print(f"Classifying {symbol}")
        classify_tables(symbol)

        print(f"Extracting data from {symbol}")
        extract_data(symbol, model_path)


if __name__ == "__main__":
    main()
