import logging
import argparse
import re
import time

from ultralytics import YOLO
import cv2
import shutil
import sys
import os

# Add the parent directory to the sys.path to locate the edgar module
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.append(root_dir)

from config.filepaths import ROOT_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load pre-trained YOLOv8 model
print("Loading pre-trained YOLOv8 model...")
model = YOLO(r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\classify\train9\weights\best.pt")


def perform_inference(image_path, model):
    try:
        print(f"Performing inference on image {image_path}")
        img = cv2.imread(image_path)
        results = model(img)
        class_labels = ["balance_sheet", "cash_flow", "income", "nothing"]
        for result in results:
            classification = class_labels[result.probs.top1]
            return classification

    except Exception as e:
        logging.error(f"Error during inference on image {image_path}: {e}")


def classify_and_move(files):
    for file_path in files:
        if os.path.isfile(file_path):
            try:
                print(f"Processing file: {file_path}")
                classification = perform_inference(file_path, model)
                print(f"Classified {file_path} as {classification}")

                # Move the file to the appropriate directory
                parent_dir = os.path.dirname(file_path)
                destination_dir = os.path.join(parent_dir, classification)
                print(f"Creating directory: {destination_dir}")
                os.makedirs(destination_dir, exist_ok=True)

                destination_path = os.path.join(destination_dir, os.path.basename(file_path))
                shutil.move(file_path, destination_path)

                print(f"Moved file {file_path} to {destination_path}")
                time.sleep(2)
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")


def main(symbol_dirs):
    all_files = []
    for symbol_dir in symbol_dirs:
        symbol_path = os.path.join(ROOT_DIR, "latest_quarterly_reports", 'sec-edgar-filings', symbol_dir, '10-Q')
        print(f"Searching directory: {symbol_path}")
        for root, dirs, files in os.walk(symbol_path):
            # Ensure we don't create tables directory infinitely
            for file in files:
                if not file.endswith(".png"):
                    continue
                print(file)
                file_path = os.path.join(root, file)
                all_files.append(file_path)
    print(f"All files to process: {all_files}")
    classify_and_move(all_files)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Classify and move files')
    parser.add_argument('symbol_dirs', metavar='D', type=str, nargs='*', help='a list of symbol directories to process')
    args = parser.parse_args()

    print(f"Directories to process: {args.symbol_dirs}")
    main(args.symbol_dirs)
