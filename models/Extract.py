import os
from statistics import median

import cv2
import easyocr
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from ultralytics import YOLO
import logging
from typing import Dict, List
import re
from dateutil import parser

from config.filepaths import ROOT_DIR


class Extract:
    """
    Extract class for extracting information from a data table in an image using YOLOv8 and EasyOCR.

    """

    def __init__(self, symbol, model_path):  # model_path: str, image_path: str):
        """
        Initializes the DataTableExtractor with the given model and image paths.

        Args:
            symbol (str): The name of the stock symbol.
            model_path (str): Path to the YOLOv8 model.

        """
        self.symbol = symbol
        self.model_path = model_path
        self.filings_dir = os.path.join(ROOT_DIR, "latest_quarterly_reports", "sec-edgar-filings", symbol)
        self.primary_document = self.find_primary_document()
        self.full_submission = self.find_full_submission()
        self.filed_as_of_date = self._extract_filed_as_of_date()
        self.report_date = self.extract_report_date()

        # Set up table directory path
        self.table_dir = os.path.join("latest_quarterly_reports", "sec-edgar-filings", symbol, "tables")

        if not os.path.exists(self.table_dir):
            raise FileNotFoundError(f"Table directory for symbol {symbol} not found")

        self.image_paths = self.get_image_paths()

        # Our images are labeled simply. These are the labels we use to extract all relevant information from a table.
        self.class_names = ["Unit", "Data", "Column Title", "Column Group Title"]
        self.class_text_mapping = {class_name: [] for class_name in self.class_names}

        self.cash_flow = None
        self.balance_sheet = None
        self.income_statement = None

        self.model = YOLO(model_path)
        logging.info(f"Model loaded from {model_path}.")
        self.reader = easyocr.Reader(['en'])

    def find_primary_document(self):
        for root, _, files in os.walk(self.filings_dir):
            if "primary-document.html" in files:
                return os.path.join(root, "primary-document.html")
        raise FileNotFoundError("Primary document not found")

    def find_full_submission(self):
        for root, _, files in os.walk(self.filings_dir):
            if "full-submission.txt" in files:
                return os.path.join(root, "full-submission.txt")
        raise FileNotFoundError("Full submission text not found")

    def _extract_filed_as_of_date(self):
        with open(self.full_submission, 'r', encoding='utf-8') as f:
            text = f.read()
        match = re.search(r'FILED AS OF DATE:\s*(\d{8})', text)
        if match:
            filed_as_of_date = match.group(1)
            formatted_date = f"{filed_as_of_date[:4]}-{filed_as_of_date[4:6]}-{filed_as_of_date[6:]}"
            return formatted_date
        return None

    def extract_report_date(self):
        with open(self.primary_document, 'r', encoding='utf-8') as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find any tags containing the quarterly period text
        tags = soup.find_all(string=re.compile(r"(?i)For the (?:Quarterly|quarterly) period ended"))

        if tags:
            # Extract the text from the next tag
            next_tag = tags[0].find_next()
            if next_tag:
                date_string = next_tag.get_text(strip=True)
                try:
                    report_date = parser.parse(date_string).strftime("%Y-%m-%d")
                    return report_date
                except ValueError:
                    logging.error("Invalid date format.")
            else:
                logging.error("Date not found in the next tag.")
        else:
            logging.error("Quarterly period text not found in the HTML content.")
        return None

    def get_image_paths(self) -> Dict[str, List[str]]:
        """
        Retrieves the image paths categorized by balance_sheet, cash_flow, and income.

        Returns:
            Dict[str, List[str]]: A dictionary containing lists of image paths categorized by balance_sheet, cash_flow, and income.
        """
        categories = ["balance_sheet", "cash_flow", "income"]
        image_paths = {category: [] for category in categories}

        for category in categories:
            category_dir = os.path.join(self.table_dir, category)
            if os.path.exists(category_dir):
                for root, _, files in os.walk(category_dir):
                    for file in files:
                        if file.endswith(".png"):
                            image_paths[category].append(os.path.join(root, file))

        return image_paths

    def perform_inference(self, image_path, conf_threshold: float = 0.75, output_path: str = None):
        """
        Performs inference on the image to detect bounding boxes.

        Args:
            image_path (str): Path to the image file.
            conf_threshold (float): Confidence threshold for detections.
            output_path (str): File path to output a labeled file. Pass this if you wish to see
            the bounding boxes the model outputs during inference.
        """
        try:
            img = cv2.imread(image_path)
            results = self.model(img, conf=conf_threshold)

            # Define colors for each class
            colors = {
                "Unit": (255, 0, 0),  # Red
                "Data": (0, 255, 0),  # Green
                "Column Title": (0, 0, 255),  # Blue
                "Column Group Title": (255, 255, 0)  # Cyan
            }

            # Create a copy of the image to draw bounding boxes
            img_with_boxes = img.copy()

            for result in results:
                for box in result.boxes:
                    class_name = self.class_names[int(box.cls[0])]
                    color = colors.get(class_name, (255, 255, 255))  # Default to white if class is not found

                    x_min, y_min, x_max, y_max = map(int, box.xyxy[0].tolist())
                    cv2.rectangle(img_with_boxes, (x_min, y_min), (x_max, y_max), color, 2)

            # Create a legend image
            legend_height = 100
            legend = np.zeros((legend_height, img.shape[1], 3), dtype=np.uint8)

            y_offset = 10
            for class_name, color in colors.items():
                cv2.putText(legend, class_name, (10, y_offset), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
                y_offset += 20

            # Concatenate the legend at the bottom of the image
            img_with_legend = np.vstack((img_with_boxes, legend))

            # Save the resulting image
            parent_dir, original_filename = os.path.split(image_path)
            filename, ext = os.path.splitext(original_filename)
            annotated_filename = f"{filename}_annotated_image{ext}"
            result_output_path = os.path.join(parent_dir, annotated_filename)

            cv2.imwrite(result_output_path, img_with_legend)

            logging.info(f"Inference results: {results}")
            logging.info(f"Annotated image saved to: {result_output_path}")
            return results
        except Exception as e:
            logging.error(f"Error during inference: {e}")
            raise

    def extract_text_from_bounding_boxes(self, results, image_path, conf_threshold: float = 0.25) -> dict:
        """
        Extracts text from bounding boxes and stores them in class_text_mapping.

        Args:
            results: Inference results containing bounding boxes.
            image_path (str): Path to the image file.
            conf_threshold (float): Confidence threshold for detections.
        """
        try:
            self.class_text_mapping = {class_name: [] for class_name in self.class_names}
            print(image_path)
            image = cv2.imread(image_path)
            boxes_with_classes = []

            for result in results:
                for i, box in enumerate(result.boxes):
                    if box.conf[0] >= conf_threshold:  # Check confidence threshold
                        class_name = self.class_names[int(box.cls[0])]
                        x_min, y_min, x_max, y_max = map(int, box.xyxy[0])
                        boxes_with_classes.append((x_min, y_min, x_max, y_max, class_name))

            # Separate boxes by class for sorting
            column_title_boxes = [box for box in boxes_with_classes if box[4] == 'Column Title']
            column_group_title_boxes = [box for box in boxes_with_classes if box[4] == 'Column Group Title']
            data_boxes = [box for box in boxes_with_classes if box[4] == 'Data']
            unit_boxes = [box for box in boxes_with_classes if box[4] == 'Unit']

            # Sort the boxes with class 'Column Title' and 'Column Group Title' by their x_min coordinate (left to right)
            column_title_boxes.sort(key=lambda b: b[0])
            column_group_title_boxes.sort(key=lambda b: b[0])

            # Sort data and unit boxes by their y_min coordinate (top to bottom)
            data_boxes.sort(key=lambda b: b[1])
            unit_boxes.sort(key=lambda b: b[1])

            # Combine sorted lists maintaining the order
            sorted_boxes_with_classes = column_title_boxes + column_group_title_boxes + data_boxes + unit_boxes

            for (x_min, y_min, x_max, y_max, class_name) in sorted_boxes_with_classes:
                if class_name == 'Data':
                    x_min = int(x_min * .9)
                    x_max = int(x_max * 1.1)
                elif class_name == 'Unit':
                    x_min = int(x_min * .5)
                    x_max = int(x_max * 2)

                cropped_img = image[y_min:y_max, x_min:x_max]
                ocr_result = self.reader.readtext(cropped_img)

                if class_name == 'Column Title':
                    self._handle_date_class(ocr_result)

                elif class_name == 'Data':
                    self.handle_data(ocr_result)

                elif class_name == 'Unit':
                    self._handle_unit_class(ocr_result)

                elif class_name == 'Column Group Title':
                    self.class_text_mapping[class_name].extend([text for (bbox, text, prob) in ocr_result])

            logging.info(f"Extracted text from bounding boxes: {self.class_text_mapping}")
            return self.class_text_mapping

        except Exception as e:
            logging.error(f"Error during text extraction: {e}")
            raise

    def _handle_date_class(self, ocr_result):
        """
        Handles the 'Date' class by consolidating all list elements into a single string and normalizing dates.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        consolidated_text = " ".join([text for (bbox, text, prob) in ocr_result]).replace(',', '')
        self.class_text_mapping['Column Title'].append(consolidated_text)

    def _handle_unit_class(self, ocr_result):
        """
        Handles the 'Unit' class by resolving what the unit is.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        unit_texts = [text for (bbox, text, prob) in ocr_result]

        units = {'financial_unit': 1, 'share_unit': 1}
        for text in unit_texts:
            if 'million' in text.lower():
                units['financial_unit'] = 1000000
            elif 'thousand' in text.lower():
                units['financial_unit'] = 1000
            if 'share' in text.lower() and 'thousand' in text.lower():
                units['share_unit'] = 1000

        self.class_text_mapping['Unit'].append(units)

    def handle_data(self, ocr_result):
        """
        Handles data by parsing the OCR results into key-value pairs.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        # Collect all text into a list
        all_texts = []
        for (bbox, text, prob) in ocr_result:
            text = text.replace('$', '').replace(',', '').strip()
            if text and text != 'S':
                all_texts.append(text)

        # Parse the list to create key-value pairs
        parsed_data = {}
        current_key = None

        for text in all_texts:
            # Check if the text is a numerical value surrounded by parentheses
            if text.startswith('(') and text.endswith(')') and text[1:-1].isdigit():
                text = '-' + text[1:-1]  # Convert to negative value
            if text.isdigit() or (text.startswith('-') and text[1:].isdigit()):  # If the text is a number
                if current_key:
                    if current_key not in parsed_data:
                        parsed_data[current_key] = []
                    parsed_data[current_key].append(text)
            else:  # The text is a key
                current_key = text

        self.class_text_mapping['Data'].append(parsed_data) if parsed_data else None

    @staticmethod
    def create_dataframe(class_text_mapping):
        data_entries = class_text_mapping['Data']
        column_titles = class_text_mapping['Column Title']

        # if not data_entries or not column_titles:
        #     raise ValueError("Data entries or Column Titles are missing")

        lengths = [len(value) for entry in data_entries for value in entry.values()]

        median_length = int(median(lengths))

        # Sometimes a report will only have 1 of 2 or 1 of 4 columns populated with data. We are going to ignore t
        # these entries for simplicity for now. Perhaps in the future we can find a reliable means to parse them
        # into the frame, but for now, we're omitting.
        filtered_entries = {key: value for entry in data_entries for key, value in entry.items() if
                            len(value) == median_length}

        expected_num_columns = len(next(iter(data_entries[0].values())))

        # if len(column_titles) != expected_num_columns:
        #     raise ValueError(
        #         f"Length mismatch: Expected {expected_num_columns} column titles, got {len(column_titles)}")

        print(filtered_entries)
        return pd.DataFrame(filtered_entries,
                            index=column_titles if column_titles else [str(i) for i in range(median_length)]).T

    def run(self):
        results = {}

        for category, image_list in self.image_paths.items():
            category_text_mappings = []
            for image_path in image_list:
                inference_results = self.perform_inference(image_path)
                text_mapping = self.extract_text_from_bounding_boxes(inference_results, image_path)
                category_text_mappings.append(text_mapping)

            combined_text_mapping = {class_name: [] for class_name in self.class_names}
            for mapping in category_text_mappings:
                for class_name in self.class_names:
                    combined_text_mapping[class_name].extend(mapping[class_name])

            results[category] = self.create_dataframe(combined_text_mapping)

        self.cash_flow = results['cash_flow']
        self.balance_sheet = results['balance_sheet']
        self.income_statement = results['income']

        return self.cash_flow, self.balance_sheet, self.income_statement


if __name__ == "__main__":
    # import argparse
    #
    # parser = argparse.ArgumentParser(description="YOLOv8 Data Table Extraction")
    # parser.add_argument("--model_path", type=str, required=True, help="Path to the YOLOv8 model file.")
    # parser.add_argument("--image_path", type=str, required=True, help="Path to the image file")
    # args = parser.parse_args()
    # extractor = Extract(model_path=args.model_path, image_path=args.image_path)
    # frame = extractor.run()

    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train24\weights\best.pt"
    self = Extract(symbol='ACCD', model_path=model_path)
    cash_flow, balance_sheet, income_statement = self.run()
