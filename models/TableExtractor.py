import os

import cv2
import easyocr
import pandas as pd
from ultralytics import YOLO
import logging
import re
from datetime import datetime
from typing import Dict, List, Tuple


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
        self.class_names = ['Data Table', 'Period', 'Unit', 'Date']
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
        try:
            img = cv2.imread(self.image_path)
            results = self.model(img, conf=conf_threshold)

            if not output_path:
                return results

            for i, results in enumerate(results):
                result_img = results.plot()
                result_output_path = os.path.splitext(output_path)[0] + f"_{i}.jpg"
                cv2.imwrite(result_output_path, result_img)
                logging.info(f"Result saved to {result_output_path}")

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
            for result in results:
                for i, box in enumerate(result.boxes):
                    if box.conf[0] >= conf_threshold:  # Check confidence threshold
                        x_min, y_min, x_max, y_max = map(int, box.xyxy[0])
                        cropped_img = image[y_min:y_max, x_min:x_max]
                        ocr_result = self.reader.readtext(cropped_img)
                        class_name = self.class_names[int(box.cls[0])]
                        if class_name not in self.class_text_mapping:
                            self.class_text_mapping[class_name] = []

                        if class_name == 'Date':
                            self._handle_date_class(ocr_result)
                        elif class_name == 'Data Table':
                            self._handle_data_table_class(ocr_result)
                        elif class_name == 'Unit':
                            self._handle_unit_class(ocr_result)
                        elif class_name == 'Period':
                            self._handle_period_class(ocr_result)

            logging.info(f"Extracted text from bounding boxes: {self.class_text_mapping}")
        except Exception as e:
            logging.error(f"Error during text extraction: {e}")
            raise

    def _handle_date_class(self, ocr_result):
        """
        Handles the 'Date' class by consolidating all list elements into a single string and stripping any commas.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        consolidated_text = " ".join([text for (bbox, text, prob) in ocr_result]).replace(',', '')
        self.class_text_mapping['Date'].append(consolidated_text)

    def _handle_data_table_class(self, ocr_result):
        """
        Handles the 'Data Table' class by performing any necessary cleaning of the text.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        cleaned_texts = []
        for (bbox, text, prob) in ocr_result:
            if text in ('$', 'S'):
                continue  # Remove the dollar sign
            if '(' in text and ')' in text:
                text = text.replace('(', '-').replace(')', '')  # Convert to negative number
            cleaned_texts.append(text)
        self.class_text_mapping['Data Table'].append(cleaned_texts)

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

    def _handle_period_class(self, ocr_result):
        """
        Handles the 'Period' class by resolving the most current period.

        Args:
            ocr_result: OCR results from EasyOCR.
        """
        period_texts = [text for (bbox, text, prob) in ocr_result]
        # Implement period resolution logic here
        resolved_period = self._resolve_period(period_texts)
        self.class_text_mapping['Period'].append(resolved_period)

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

    def _resolve_period(self, period_texts: List[str]) -> str:
        """
        Resolves the most current period.

        Args:
            period_texts (List[str]): List of texts extracted from the 'Period' class.

        Returns:
            str: The resolved period.
        """
        # Example implementation, customize this based on your requirements
        current_period = ""
        for text in period_texts:
            if 'weeks ended' in text.lower():
                current_period = text
                break
            elif 'months ended' in text.lower():
                current_period = text
        return current_period

    def data_table_to_dataframe(self):
        """
        Converts the extracted data table into a pandas DataFrame using 'Date' values as column headers.
        """
        try:
            data_table = self.class_text_mapping.get('Data Table', [])
            dates = self.class_text_mapping.get('Date', [])
            if not data_table or not data_table[0] or not dates:
                raise ValueError("No data table or dates found in the extracted text.")

            # Flatten the list of lists into a single list
            flat_list = data_table[0]

            # Initialize lists to store the parsed table data
            row_headers = []
            row_data = []
            current_header = None

            numeric_pattern = re.compile(r'^-?\d+(,\d{3})*(\.\d+)?$')

            # Iterate over the flat list to populate row_headers and row_data
            for item in flat_list:
                print(f"Item: {item}, Type: {type(item)}")
                # Check if the item is numeric
                if numeric_pattern.match(item.replace(',', '')):
                    row_data[-1].append(item)
                else:
                    current_header = item
                    row_headers.append(current_header)
                    row_data.append([])

            # Create a DataFrame
            df = pd.DataFrame(row_data, index=row_headers, columns=dates)

            # Convert numeric columns to appropriate data types
            for col in dates:
                df[col] = pd.to_numeric(df[col].str.replace(',', '').str.replace('(', '-').str.replace(')', ''),
                                        errors='coerce')

            logging.info(f"Data Table DataFrame:\n{df}")
            return df
        except Exception as e:
            logging.error(f"Error converting data table to DataFrame: {e}")
            raise

    def run(self):
        results = self.perform_inference()
        self.extract_text_from_bounding_boxes(results)
        df = self.data_table_to_dataframe()

if __name__ == "__main__":
    # import argparse
    #
    # parser = argparse.ArgumentParser(description="YOLOv8 Data Table Extraction")
    # parser.add_argument("--model_path", type=str, required=True, help="Path to the YOLOv8 model file.")
    # parser.add_argument("--image_path", type=str, required=True, help="Path to the image file")
    # args = parser.parse_args()
    #
    # extractor = DataTableExtractor(model_path=args.model_path, image_path=args.image_path)

    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train4\weights\best.pt"
    #image_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\COST\0000909832-13-000011_table_page4_table1.png"
    image_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\COST\0000909832-13-000011_table_page5_table1.png"
    self = DataTableExtractor(model_path=model_path, image_path=image_path)
