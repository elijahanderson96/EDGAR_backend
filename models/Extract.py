from statistics import median

import cv2
import easyocr
import numpy as np
import pandas as pd
from ultralytics import YOLO
import logging
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

    def create_dataframe(self):
        data_entries = self.class_text_mapping['Data']
        column_titles = self.class_text_mapping['Column Title']

        if not data_entries or not column_titles:
            raise ValueError("Data entries or Column Titles are missing")

        lengths = [len(value) for entry in data_entries for value in entry.values()]

        median_length = int(median(lengths))

        # Sometimes a report will only have 1 of 2 or 1 of 4 columns populated with data. We are going to ignore t
        # these entries for simplicity for now. Perhaps in the future we can find a reliable means to parse them
        # into the frame, but for now, we're omitting.
        filtered_entries = {key: value for entry in data_entries for key, value in entry.items() if
                            len(value) == median_length}

        expected_num_columns = len(next(iter(data_entries[0].values())))

        if len(column_titles) != expected_num_columns:
            raise ValueError(
                f"Length mismatch: Expected {expected_num_columns} column titles, got {len(column_titles)}")

        return pd.DataFrame(filtered_entries, index=self.class_text_mapping['Column Title']).T

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

    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train24\weights\best.pt"
    image_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\AAPL\0000320193-21-000010_table_page6_table1.png"
    self = Extract(model_path=model_path, image_path=image_path)
    frame = self.run()
