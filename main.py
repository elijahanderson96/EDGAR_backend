import subprocess
import datetime
import sys
import os
import time

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


def main():
    # download_date = datetime.datetime.now().strftime('%Y-%m-%d')
    # download_date = (datetime.datetime.now() - datetime.timedelta(weeks=520)).strftime('%Y-%m-%d')
    #
    # command = ['python', os.path.join(script_dir, 'edgar', 'downloader.py'), download_date]
    #
    # # Uncomment the next line to run the downloader script
    # run_command(command)

    # Now we detect all the tables.
    latest_report_symbols = os.listdir(os.path.join(script_dir, 'latest_quarterly_reports', 'sec-edgar-filings'))

    for symbol in latest_report_symbols:
        if symbol == 'MCK':
            command = ['python', os.path.join(script_dir, 'models', 'table_detection.py'), symbol]
            time.sleep(1)
            run_command(command)

    # # now we classify the tables as balance sheet, cash flow, income, or nothing.
    command = ['python', os.path.join(script_dir, 'models', 'Classify.py')] + ['MCK']#latest_report_symbols[16:20]
    run_command(command)


if __name__ == "__main__":
    main()
