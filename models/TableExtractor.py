import os
from datetime import datetime

import cv2
import easyocr
import pandas as pd
from ultralytics import YOLO
import logging
import re
from typing import Dict, List


class DataTableExtractor:
    """
    DataTableExtractor class for extracting information from a data table in an image using YOLOv8 and EasyOCR.

    Attributes:
        model_path (str): Path to the YOLOv8 model.
        image_path (str): Path to the image file.
        class_text_mapping (Dict[str, List[List[str]]]): Mapping of class names to lists of OCR extracted text.
    """

    def __init__(self, model_path: str, image_path: str):
        """
        Initializes the DataTableExtractor with the given model and image paths.

        Args:
            model_path (str): Path to the YOLOv8 model.
            image_path (str): Path to the image file.
        """
        self.model_path = model_path
        self.image_path = image_path

        # Our images are labeled simply. These are the labels we use to extract all relevant information from a table.
        self.class_names = ["Current Assets", "Non Current Assets", "Current Liabilities", "Non Current Liabilities",
                            "Equity", "Date", "Period", "Unit"]
        self.class_text_mapping = {}

        self.model = YOLO(model_path)
        logging.info(f"Model loaded from {model_path}.")
        self.reader = easyocr.Reader(['en'])

        # current_column_coords indicates the most recent reported data within a table. Often times reports will compare
        # past data to current data to show growth, we are only interested in the current report. We can access
        # historical data within our database.
        self.current_column_coords = None
        self.max_date = None

    def perform_inference(self, conf_threshold: float = 0.25, output_path: str = None):
        """
        Performs inference on the image to detect bounding boxes.

        Args:
            conf_threshold (float): Confidence threshold for detections.
            output_path (str): File path to output a labeled file. Pass this if you wish to see
            the bounding boxes the model outputs during inference.
        """
        print(output_path)
        try:
            img = cv2.imread(self.image_path)
            results = self.model(img, conf=conf_threshold)
            output_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\test_output.png"

            if not output_path:
                return results

            for i, results in enumerate(results):
                result_img = results.plot()
                result_output_path = os.path.splitext(output_path)[0] + f"_{i}.jpg"
                cv2.imwrite(result_output_path, result_img)

            logging.info(f"Inference results: {results}")
            return results
        except Exception as e:
            logging.error(f"Error during inference: {e}")
            raise

    def extract_text_from_bounding_boxes(self, results, conf_threshold: float = 0.25) -> None:
        """
        Extracts text from bounding boxes and stores them in class_text_mapping.

        Args:
            results: Inference results containing bounding boxes.
            conf_threshold (float): Confidence threshold for detections.
        """
        try:
            image = cv2.imread(self.image_path)
            boxes_with_classes = []

            for result in results:
                for i, box in enumerate(result.boxes):
                    if box.conf[0] >= conf_threshold:  # Check confidence threshold
                        class_name = self.class_names[int(box.cls[0])]
                        x_min, y_min, x_max, y_max = map(int, box.xyxy[0])
                        boxes_with_classes.append((x_min, y_min, x_max, y_max, class_name))

            # Sort the boxes with class 'Date' by their x_min coordinate
            boxes_with_classes.sort(key=lambda b: (b[4] == 'Date', b[0]))

            for (x_min, y_min, x_max, y_max, class_name) in boxes_with_classes:
                cropped_img = image[y_min:y_max, x_min:x_max]
                ocr_result = self.reader.readtext(cropped_img)

                if class_name not in self.class_text_mapping:
                    self.class_text_mapping[class_name] = []

                if class_name == 'Date':
                    self._handle_date_class(ocr_result)

                elif class_name in ['Current Assets', 'Non Current Assets', 'Current Liabilities',
                                    'Non Current Liabilities', 'Equity']:
                    self._handle_balance_sheet_class(ocr_result, class_name)

                elif class_name == 'Unit':
                    self._handle_unit_class(ocr_result)

            logging.info(f"Extracted text from bounding boxes: {self.class_text_mapping}")
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
        print(consolidated_text)
        # Regular expressions for various date formats
        date_patterns = [
            r'(\d{2}/\d{2}/\d{4})',  # MM/DD/YYYY
            r'(\d{2}-\d{2}-\d{4})',  # MM-DD-YYYY
            r'(\d{4}/\d{2}/\d{2})',  # YYYY/MM/DD
            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            r'(\b\w+ \d{1,2}, \d{4}\b)',  # Month Day, Year
            r'(\d{1,2} \b\w+ \d{4}\b)',  # Day Month Year
            r'(\b\w+ \d{1,2} \d{4}\b)'  # Month Day Year
        ]
        print('date patterns')
        dates = []
        for pattern in date_patterns:
            matches = re.findall(pattern, consolidated_text)
            for match in matches:
                try:
                    print(match)
                    # Try to parse the date and convert to a standard format (YYYY-MM-DD)
                    normalized_date = self._normalize_date(match)
                    dates.append(normalized_date)
                except ValueError:
                    continue

        self.class_text_mapping['Date'].extend(dates) if dates else None

    def _normalize_date(self, date_str):
        """
        Normalizes a date string to the format YYYY-MM-DD.

        Args:
            date_str (str): The date string to normalize.

        Returns:
            str: The normalized date string.
        """
        for fmt in ('%m/%d/%Y', '%m-%d/%Y', '%Y/%m/%d', '%Y-%m-%d', '%B %d, %Y', '%d %B %Y', '%B %d %Y'):
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                pass
        raise ValueError(f'Unknown date format: {date_str}')

    def _handle_unit_class(self, ocr_result):
        """
        Handles the 'Unit' class by resolving what the unit is.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        unit_texts = [text for (bbox, text, prob) in ocr_result]
        # Implement unit resolution logic here
        resolved_unit = self._resolve_unit(unit_texts)
        self.class_text_mapping['Unit'].append(resolved_unit)

    def _resolve_unit(self, unit_texts: List[str]) -> Dict[str, int]:
        """
        Resolves what the unit is, commonly millions or thousands.

        Args:
            unit_texts (List[str]): List of texts extracted from the 'Unit' class.

        Returns:
            Dict[str, int]: Dictionary of unit conversions.
        """
        units = {'financial_unit': 1, 'share_unit': 1}
        for text in unit_texts:
            if 'million' in text.lower():
                units['financial_unit'] = 1000000
            elif 'thousand' in text.lower():
                units['financial_unit'] = 1000
            if 'share' in text.lower() and 'thousand' in text.lower():
                units['share_unit'] = 1000
        return units

    def _handle_balance_sheet_class(self, ocr_result, class_name):
        """
        Handles balance sheet classes ('Current Assets', 'Non Current Assets', 'Current Liabilities',
        'Non Current Liabilities', 'Equity') by parsing the OCR results into key-value pairs.

        Args:
            ocr_result: OCR results from EasyOCR.
            class_name (str): The class name to handle.
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
        numeric_pattern = re.compile(r'^-?\d+(,\d{3})*(\.\d+)?$')
        negative_pattern = re.compile(r'^\((\d+(,\d{3})*(\.\d+)?)\)$')

        i = 0
        while i < len(all_texts):
            text = all_texts[i]
            if numeric_pattern.match(text.replace(',', '')):
                # This is a numeric value
                if current_key:
                    if current_key not in parsed_data:
                        parsed_data[current_key] = []
                    parsed_data[current_key].append(text)
                    # Check if the next item is also a numeric value
                    if i + 1 < len(all_texts) and numeric_pattern.match(all_texts[i + 1].replace(',', '')):
                        parsed_data[current_key].append(all_texts[i + 1])
                        i += 1  # Move to the next item after the second numeric value
                    current_key = None
            elif negative_pattern.match(text.replace(',', '')):
                # This is a negative numeric value
                negative_value = '-' + negative_pattern.match(text.replace(',', '')).group(1)
                if current_key:
                    if current_key not in parsed_data:
                        parsed_data[current_key] = []
                    parsed_data[current_key].append(negative_value)
                    # Check if the next item is also a numeric value
                    if i + 1 < len(all_texts) and numeric_pattern.match(all_texts[i + 1].replace(',', '')):
                        parsed_data[current_key].append(all_texts[i + 1])
                        i += 1  # Move to the next item after the second numeric value
                    current_key = None
            else:
                # This is a key (column title)
                if current_key:
                    # Append to the existing key
                    current_key += ' ' + text
                else:
                    current_key = text

                # Check if the next item is a continuation of the current key
                if i + 1 < len(all_texts) and not (
                        numeric_pattern.match(all_texts[i + 1].replace(',', '')) or negative_pattern.match(
                        all_texts[i + 1].replace(',', ''))):
                    current_key += ' ' + all_texts[i + 1]
                    i += 1  # Move to the next item after the continuation

            i += 1

        self.class_text_mapping[class_name].append(parsed_data)

    def create_balance_sheet_dataframe(self):
        frames = ['Current Assets', 'Non Current Assets', 'Current Liabilities', 'Non Current Liabilities', 'Equity']
        dfs = []

        for frame in frames:
            try:
                dfs.append(pd.DataFrame(self.class_text_mapping[frame][0], index=self.class_text_mapping['Date']).T)
                print(f"Found {frame} inside the document. Adding it to list of frames.")
            except KeyError as e:
                print(f"Could not find {frame}, it was likely not detected within the table and therefore cannot"
                      f"be extracted. Skipping.")

        balance_sheet = pd.concat(dfs)

        return balance_sheet

    def run(self):
        results = self.perform_inference(output_path=r'C:\Users\Elijah\PycharmProjects\edgar_backend\image')
        self.extract_text_from_bounding_boxes(results)
        results = self.create_balance_sheet_dataframe()
        return results


if __name__ == "__main__":
    # import argparse
    #
    # parser = argparse.ArgumentParser(description="YOLOv8 Data Table Extraction")
    # parser.add_argument("--model_path", type=str, required=True, help="Path to the YOLOv8 model file.")
    # parser.add_argument("--image_path", type=str, required=True, help="Path to the image file")
    # args = parser.parse_args()
    # extractor = DataTableExtractor(model_path=args.model_path, image_path=args.image_path)

    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train14\weights\best.pt"
    # image_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\CSCO\0000858877-13-000013_table_page3_table1.png"
    image_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\AAPL\0000320193-18-000070_table_page5_table1.png"
    self = DataTableExtractor(model_path=model_path, image_path=image_path)
    frame = self.run()
