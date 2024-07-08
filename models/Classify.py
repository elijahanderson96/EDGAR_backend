import logging
import argparse
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
model = YOLO(r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\classify\train2\weights\best.pt")


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
                parent_dir = os.path.dirname(os.path.dirname(file_path))
                destination_dir = os.path.join(parent_dir, classification)
                print(f"Creating directory: {destination_dir}")
                os.makedirs(destination_dir, exist_ok=True)

                destination_path = os.path.join(destination_dir, os.path.basename(file_path))
                shutil.move(file_path, destination_path)

                print(f"Moved file {file_path} to {destination_path}")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")


def main(symbol_dirs):
    all_files = []
    for symbol_dir in symbol_dirs:
        symbol_path = os.path.join(ROOT_DIR, "latest_quarterly_report_tables", symbol_dir)
        print(f"Searching directory: {symbol_path}")
        for root, _, files in os.walk(symbol_path):
            for file in files:
                file_path = os.path.join(root, file)
                all_files.append(file_path)
    print(f"All files to process: {all_files}")
    classify_and_move(all_files)


def run_tests():
    # Define test cases
    test_images = [
        (r"C:\Users\Elijah\PycharmProjects\edgar_backend\latest_quarterly_report_tables\CMC\0000022444-24-000089_table_page6_table1.png", "balance_sheet"),
        (r"C:\Users\Elijah\PycharmProjects\edgar_backend\latest_quarterly_report_tables\ADBE\0000796343-24-000150_table_page8_table1.png", "cash_flow"),
    ]

    # Run inference on test cases
    for image_path, expected_class in test_images:
        result_class = perform_inference(image_path, model)
        assert result_class == expected_class, f"Test failed for {image_path}: expected {expected_class}, got {result_class}"
        print(f"Test passed for {image_path}: expected {expected_class}, got {result_class}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Classify and move files')
    parser.add_argument('symbol_dirs', metavar='D', type=str, nargs='*', help='a list of symbol directories to process')
    parser.add_argument('--test', action='store_true', help='Run test cases')
    args = parser.parse_args()

    if args.test:
        run_tests()
    else:
        if not args.symbol_dirs:
            parser.error("the following arguments are required: D")
        print(f"Directories to process: {args.symbol_dirs}")
        main(args.symbol_dirs)
