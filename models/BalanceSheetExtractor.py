import os
from datetime import datetime
from statistics import median

import cv2
import easyocr
import numpy as np
import pandas as pd
from ultralytics import YOLO
import logging
import re
from typing import Dict, List


class Extract:
    """
    Extract class for extracting information from a data table in an image using YOLOv8 and EasyOCR.

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
        self.class_names = ["Unit", "Data", "Column Title", "Column Group Title"]
        self.class_text_mapping = {class_name: [] for class_name in self.class_names}

        self.model = YOLO(model_path)
        logging.info(f"Model loaded from {model_path}.")
        self.reader = easyocr.Reader(['en'])

    def perform_inference(self, conf_threshold: float = 0.35, output_path: str = None):
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
            output_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\test_output.png"

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
            result_output_path = output_path or r"C:\Users\Elijah\PycharmProjects\edgar_backend\test_output.png"
            cv2.imwrite(result_output_path, img_with_legend)

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

            # Sort the boxes with class 'Column Title' and 'Column Group Title' by their x_min coordinate
            boxes_with_classes.sort(key=lambda b: (b[4] in ['Column Title'], b[0]))

            for (x_min, y_min, x_max, y_max, class_name) in boxes_with_classes:
                if class_name == 'Data':
                    x_min = int(x_min * .9)
                    x_max = int(x_max * 1.1)
                    y_min = int(y_min * .98)
                    y_max = int(y_max * 1.02)

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
        # for bbox, text, prob in ocr_result:
        #     print(text)
        #     input('')
        # print(f"Consolidated text is: {consolidated_text}")
        #
        # # Regular expressions for various date formats
        # date_patterns = [
        #     r'(\d{2}/\d{2}/\d{4})',  # MM/DD/YYYY
        #     r'(\d{2}-\d{2}-\d{4})',  # MM-DD-YYYY
        #     r'(\d{4}/\d{2}/\d{2})',  # YYYY/MM/DD
        #     r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
        #     r'(\b\w+ \d{1,2}, \d{4}\b)',  # Month Day, Year
        #     r'(\d{1,2} \b\w+ \d{4}\b)',  # Day Month Year
        #     r'(\b\w+ \d{1,2} \d{4}\b)',  # Month Day Year
        #     r'(\b\w+ \d{4}\b) \d{1,2}'  # Month  Year Day
        #
        # ]
        # dates = []
        # for pattern in date_patterns:
        #     matches = re.findall(pattern, consolidated_text)
        #     for match in matches:
        #         try:
        #             # Try to parse the date and convert to a standard format (YYYY-MM-DD)
        #             normalized_date = self._normalize_date(match)
        #             print(normalized_date)
        #             input("Break")
        #             dates.append(normalized_date)
        #         except ValueError:
        #             continue

        self.class_text_mapping['Column Title'].append(consolidated_text)

    def _normalize_date(self, date_str):
        """
        Normalizes a date string to the format YYYY-MM-DD.

        Args:
            date_str (str): The date string to normalize.

        Returns:
            str: The normalized date string.
        """
        for fmt in ('%m/%d/%Y', '%m-%d/%Y', '%Y/%m/%d', '%Y-%m-%d', '%B %d, %Y', '%d %B %Y', '%B %d %Y', '%B %Y %d'):
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
            if text.isdigit():  # If the text is a number
                if current_key:
                    if current_key not in parsed_data:
                        parsed_data[current_key] = []
                    parsed_data[current_key].append(text)
            else:  # The text is a key
                current_key = text

        self.class_text_mapping['Data'].append(parsed_data) if parsed_data else None

    def create_dataframe(self):
        data_entries = self.class_text_mapping['Data']
        column_titles = self.class_text_mapping['Column Title']

        # Ensure there are data entries and column titles
        if not data_entries or not column_titles:
            raise ValueError("Data entries or Column Titles are missing")

        # Determine the length of each data entry
        lengths = [len(next(iter(entry.values()))) for entry in data_entries]

        # Calculate the median length
        median_length = int(median(lengths))

        # Filter out entries that do not match the median length
        filtered_entries = [entry for entry in data_entries if len(entry.values()) == median_length]

        # Ensure the length of the column titles matches the expected number of columns
        expected_num_columns = len(next(iter(data_entries[0].values())))
        if len(column_titles) != expected_num_columns:
            raise ValueError(
                f"Length mismatch: Expected {expected_num_columns} column titles, got {len(column_titles)}")

        # Prepare a list to collect DataFrames
        dataframes = []

        # Process each data entry to construct a DataFrame
        for entry in filtered_entries:
            df = pd.DataFrame(entry)
            dataframes.append(df)

        # Concatenate all DataFrames
        concatenated_df = pd.concat(dataframes, axis=1).T.drop_duplicates()

        # Assign column titles
        concatenated_df.columns = column_titles

        return concatenated_df

    def run(self):
        results = self.perform_inference(output_path=r'C:\Users\Elijah\PycharmProjects\edgar_backend\image')
        self.extract_text_from_bounding_boxes(results)
        results = self.create_dataframe()
        return results


if __name__ == "__main__":
    # import argparse
    #
    # parser = argparse.ArgumentParser(description="YOLOv8 Data Table Extraction")
    # parser.add_argument("--model_path", type=str, required=True, help="Path to the YOLOv8 model file.")
    # parser.add_argument("--image_path", type=str, required=True, help="Path to the image file")
    # args = parser.parse_args()
    # extractor = Extract(model_path=args.model_path, image_path=args.image_path)
    # frame = extractor.run()

    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train19\weights\best.pt"
    image_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\yolo_dataset\images\train\0000320193-18-000007_table_page6_table1.png"
    self = Extract(model_path=model_path, image_path=image_path)
    frame = self.run()
