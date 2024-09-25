import os
import shutil

from edgar.symbols import symbols


def aggregate_files(symbol_dir, upload_dir):
    # Ensure the upload_dir exists
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)

    # Define the subdirectories to search in
    sub_dirs = ['balance_sheet', 'cash_flow', 'income']

    # Traverse the symbol directory
    for root, dirs, files in os.walk(symbol_dir):
        # Check if the current directory is one of the required subdirectories
        if any(sub_dir in root for sub_dir in sub_dirs):
            # Copy files in the current directory to upload_dir
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path):
                    shutil.copy(file_path, upload_dir)
                    print(f"Copied {file_path} to {upload_dir}")

# Define the paths
upload_dir = "images_to_upload"       # Path to the upload directory
for symbol in symbols['symbol'].to_list()[1:]:
    symbol_dir = rf"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\{symbol}"  # Path to the symbol directory (e.g., AAPL)
# Call the function
    aggregate_files(symbol_dir, upload_dir)
