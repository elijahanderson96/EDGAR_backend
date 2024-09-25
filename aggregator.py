import os
import shutil
import logging
import random
from math import ceil
from config.filepaths import ROOT_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def copy_files_to_batches(batch_size):
    # Define the directories to search
    search_dirs = ['balance_sheet', 'cash_flow', 'income']

    # Define the root level directory where files will be copied
    target_dir = os.path.join(ROOT_DIR, 'aggregated_files')

    # Ensure the target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Define the parent directory containing all symbols
    parent_dir = os.path.join(ROOT_DIR, 'sec-edgar-filings')

    logging.info(f"Searching for files in {parent_dir}...")

    # Gather all source files
    file_list = []
    for root, dirs, files in os.walk(parent_dir):
        for dir_name in dirs:
            if dir_name in search_dirs:
                source_dir = os.path.join(root, dir_name)
                for file_name in os.listdir(source_dir):
                    source_file = os.path.join(source_dir, file_name)
                    if os.path.isfile(source_file) and 'annotated' not in file_name:
                        symbol = root.split(os.sep)[-2]
                        file_list.append((symbol, source_file))

    # Shuffle the files randomly
    random.shuffle(file_list)

    # Gather all existing files in aggregated_files to avoid duplication
    existing_files = set()
    for root, dirs, files in os.walk(target_dir):
        for file_name in files:
            existing_files.add(file_name)

    # Filter out files that already exist in the target_dir batches
    unique_file_list = [
        (symbol, source_file) for symbol, source_file in file_list
        if f"{symbol}_{os.path.basename(source_file)}" not in existing_files
    ]

    # Find the highest existing batch number
    existing_batches = [int(d.split('_')[-1]) for d in os.listdir(target_dir) if d.startswith('batch_') and d.split('_')[-1].isdigit()]
    if existing_batches:
        next_batch_num = max(existing_batches) + 1
    else:
        next_batch_num = 1

    # Calculate number of batches
    total_files = len(unique_file_list)
    num_batches = ceil(total_files / batch_size)

    # Move files into batch subdirectories starting from the next available batch number
    for batch_num in range(next_batch_num, next_batch_num + num_batches):
        batch_dir = os.path.join(target_dir, f"batch_{batch_num}")
        os.makedirs(batch_dir, exist_ok=True)

        batch_files = unique_file_list[(batch_num - next_batch_num) * batch_size: (batch_num - next_batch_num + 1) * batch_size]
        for symbol, source_file in batch_files:
            file_name = os.path.basename(source_file)
            target_file = os.path.join(batch_dir, f"{symbol}_{file_name}")
            logging.info(f"Moving {source_file} to {target_file}")
            shutil.move(source_file, target_file)

    logging.info(f"Unique files moved into {num_batches} batch subdirectories starting from batch {next_batch_num}.")


if __name__ == "__main__":
    # Define batch size, for example, 32 files per batch
    batch_size = 32
    copy_files_to_batches(batch_size)
