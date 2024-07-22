import os
import shutil
import logging
from config.filepaths import ROOT_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def copy_files_to_root(symbol):
    # Define the directories to search
    search_dirs = ['balance_sheet', 'cash_flow', 'income']

    # Define the root level directory where files will be copied
    target_dir = os.path.join(ROOT_DIR, 'aggregated_files')

    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Define the parent directory
    parent_dir = os.path.join(ROOT_DIR, 'latest_quarterly_reports','sec-edgar-filings', symbol, '10-Q')

    logging.info(f"Searching for files in {parent_dir}...")

    # Walk through the directory structure
    for root, dirs, files in os.walk(parent_dir):
        for dir_name in dirs:
            if dir_name in search_dirs:
                source_dir = os.path.join(root, dir_name)
                for file_name in os.listdir(source_dir):
                    source_file = os.path.join(source_dir, file_name)
                    if os.path.isfile(source_file) and 'annotated' not in file_name:
                        target_file = os.path.join(target_dir, f"{symbol}_{file_name}")
                        logging.info(f"Copying {source_file} to {target_file}")
                        shutil.copy2(source_file, target_file)

    logging.info(f"All files copied to {target_dir}")


if __name__ == "__main__":
    # Example usage
    symbol = 'NXPI'
    copy_files_to_root(symbol)
