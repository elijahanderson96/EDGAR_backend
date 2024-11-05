import json
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

from database.database import db_connector
from database.helpers import get_date_id, get_symbol_id
from helpers.email_utils import send_validation_email

from models.Transform import ValidateColumns


class Extract:
    def __init__(self, symbol, filings_dir, model_path):
        self.symbol = symbol
        self.model_path = model_path
        self.filings_dir = filings_dir
        self.primary_document = self.find_primary_document()
        self.full_submission = self.find_full_submission()
        self.filed_as_of_date = self._extract_filed_as_of_date()
        self.report_date = self.extract_report_date()
        self.aggregated_data = []

        print(f"Filed as of date determined to be: {self.filed_as_of_date}")
        print(f"Report date determined to be: {self.report_date}")

        # Set up table directory path
        self.table_dir = os.path.join(self.filings_dir, "tables")

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
        print(f"Model loaded from {model_path}.")
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
            for tag in tags:
                # Look for the date in the next few sibling elements
                next_siblings = tag.find_all_next(string=True, limit=5)
                combined_text = " ".join([tag] + [sibling.strip() for sibling in next_siblings])

                # Replace non-breaking spaces and HTML entities with regular spaces
                combined_text = combined_text.replace(u'\xa0', ' ').replace('&nbsp;', ' ')

                # Search for the date within the combined text
                date_match = re.search(r"(\w+\s+\d{1,2},\s+\d{4})", combined_text)

                if date_match:
                    date_string = date_match.group(0)
                    try:
                        report_date = parser.parse(date_string).strftime("%Y-%m-%d")
                        return report_date
                    except ValueError:
                        logging.error("Invalid date format in tag.")
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
                        if file.endswith(".png") and 'annotated_image' not in file:
                            image_paths[category].append(os.path.join(root, file))

        return image_paths

    def letterbox(self, image, new_shape=(1280, 1280), color=(114, 114, 114), auto=True, scaleFill=False, scaleup=True):
        # Resize image to a 32-pixel-multiple rectangle
        shape = image.shape[:2]  # current shape [height, width]
        if isinstance(new_shape, int):
            new_shape = (new_shape, new_shape)

        # Scale ratio (new / old) and new unpadded shape
        r = min(new_shape[0] / shape[0], new_shape[1] / shape[1])
        if not scaleup:  # only scale down, do not scale up (for better val mAP)
            r = min(r, 1.0)

        ratio = r, r  # width, height ratios
        new_unpad = int(round(shape[1] * r)), int(round(shape[0] * r))

        # Compute padding
        dw, dh = new_shape[1] - new_unpad[0], new_shape[0] - new_unpad[1]  # wh padding
        if auto:  # minimum rectangle
            dw, dh = np.mod(dw, 32), np.mod(dh, 32)  # wh padding
        elif scaleFill:  # stretch
            dw, dh = 0.0, 0.0
            new_unpad = new_shape
            ratio = new_shape[1] / shape[1], new_shape[0] / shape[0]  # width, height ratios

        dw /= 2  # divide padding into 2 sides
        dh /= 2

        if shape[::-1] != new_unpad:  # resize
            image = cv2.resize(image, new_unpad, interpolation=cv2.INTER_LINEAR)
        top, bottom = int(round(dh - 0.1)), int(round(dh + 0.1))
        left, right = int(round(dw - 0.1)), int(round(dw + 0.1))
        image = cv2.copyMakeBorder(image, top, bottom, left, right, cv2.BORDER_CONSTANT, value=color)  # add border
        return image, ratio, (dw, dh)

    def perform_inference(self, image_path, conf_threshold: float = 0.65, output_path: str = None):
        """
        Performs inference on the image to detect bounding boxes.

        Args:
            image_path (str): Path to the image file.
            conf_threshold (float): Confidence threshold for detections.
            output_path (str): File path to output a labeled file. Pass this if you wish to see
            the bounding boxes the model outputs during inference.
        """
        try:
            # Load the image
            img = cv2.imread(image_path)

            # Apply letterboxing (resize with padding)
            padded_img, ratio, (dw, dh) = self.letterbox(img)

            # Perform inference on the letterboxed image
            results = self.model(padded_img, conf=conf_threshold, verbose=False)

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

                    # Adjust bounding box positions to match the original image
                    x_min = int((box.xyxy[0][0] - dw) / ratio[0])
                    y_min = int((box.xyxy[0][1] - dh) / ratio[1])
                    x_max = int((box.xyxy[0][2] - dw) / ratio[0])
                    y_max = int((box.xyxy[0][3] - dh) / ratio[1])

                    # Draw the bounding box
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

            print(f"Annotated image saved to: {result_output_path}")
            return results
        except Exception as e:
            logging.error(f"Error during inference: {e}")
            raise

    def resize_image(self, image, scale_factor: float = 2.0):
        height, width = image.shape[:2]
        resized_image = cv2.resize(image, (int(width * scale_factor), int(height * scale_factor)))
        return resized_image

    def preprocess_image(self, image, show=False, save_path=None):
        # Convert image to grayscale
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        # Increase contrast
        contrast_img = cv2.convertScaleAbs(gray, alpha=1.5, beta=0)

        # Apply Gaussian blur to reduce noise
        denoised_img = cv2.GaussianBlur(contrast_img, (5, 5), 0)

        # Binarize the image
        _, binary_img = cv2.threshold(denoised_img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

        # Display the image in a pop-up window
        if show:
            # Show the image in a window using OpenCV
            cv2.imshow("Preprocessed Image", binary_img)
            cv2.waitKey(0)  # Wait for a key press to close the window
            cv2.destroyAllWindows()

        # Save the preprocessed image to disk if a save path is provided
        if save_path:
            cv2.imwrite(save_path, binary_img)
            print(f"Preprocessed image saved to {save_path}")

        return binary_img

    def extract_text_from_bounding_boxes(self, results, image_path, conf_threshold: float = 0.25) -> dict:
        """
        Extracts text from bounding boxes and handles image resizing to match the inference stage.

        Args:
            results: Inference results containing bounding boxes.
            image_path (str): Path to the image file.
            conf_threshold (float): Confidence threshold for detections.
        """
        try:
            self.class_text_mapping = {class_name: [] for class_name in self.class_names}
            image = cv2.imread(image_path)

            # Apply letterboxing (resize with padding) just like in perform_inference
            letterboxed_img, ratio, (dw, dh) = self.letterbox(image)
            boxes_with_classes = []

            for result in results:
                for i, box in enumerate(result.boxes):
                    if box.conf[0] >= conf_threshold:
                        class_name = self.class_names[int(box.cls[0])]
                        # Adjust the bounding box coordinates to match the letterboxed image
                        x_min = int((box.xyxy[0][0] - dw) / ratio[0])
                        y_min = int((box.xyxy[0][1] - dh) / ratio[1])
                        x_max = int((box.xyxy[0][2] - dw) / ratio[0])
                        y_max = int((box.xyxy[0][3] - dh) / ratio[1])
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
                    x_min = int(x_min * .85)
                    x_max = int(x_max * 1.15)
                elif class_name == 'Unit':
                    x_min = int(x_min * .95)
                    x_max = int(x_max * 1.05)

                cropped_img = image[y_min:y_max, x_min:x_max]
                enlarged_cropped_img = self.resize_image(cropped_img,)
                #preprocessed_img = self.preprocess_image(enlarged_cropped_img, show=True,
                #                                         save_path="preprocessed_image.png")

                ocr_result = self.reader.readtext(enlarged_cropped_img)

                if class_name == 'Column Title':
                    self._handle_date_class(ocr_result, type='Column Title')
                elif class_name == 'Data':
                    self.handle_data(ocr_result)
                elif class_name == 'Unit':
                    self._handle_unit_class(ocr_result)
                elif class_name == 'Column Group Title':
                    self._handle_date_class(ocr_result, type='Group Title')

            self.class_text_mapping['Data'] = self.aggregated_data
            print(f"Extracted text from bounding boxes: {self.class_text_mapping}")
            return self.class_text_mapping

        except Exception as e:
            logging.error(f"Error during text extraction: {e}")
            raise

    def _handle_date_class(self, ocr_result, type):
        consolidated_text = " ".join([text for (bbox, text, prob) in ocr_result])
        cleaned_text = re.sub(r'[^A-Za-z0-9\s]', '', consolidated_text)
        cleaned_text = cleaned_text.replace("Mnth", "Month")
        self.class_text_mapping['Column Title'].append(cleaned_text) if type == 'Column Title' \
            else self.class_text_mapping['Column Group Title'].append(cleaned_text)

    def _handle_unit_class(self, ocr_result):
        # TODO: The last if statement is going to break in some cases. Try to implement a better solution.
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
        all_texts = []
        [all_texts.append(text) for bbox, text, prob in ocr_result if text and text not in ('S', '$', '5')]
        self.aggregated_data.extend(all_texts)

    @staticmethod
    def clean_value(item):
        item = item.strip()  # Remove leading and trailing spaces
        # Remove any enclosing parentheses and determine if it should be negative
        if item.startswith('(') and item.endswith(')'):
            return -float(item.strip('()').replace(',', '')) \
                if re.match(r'^-?\d+(\.\d+)?$', item.strip('()').replace(',', '')) else item.strip('()')
        elif item.endswith(')'):
            return -float(item.strip(')').replace(',', '')) \
                if re.match(r'^-?\d+(\.\d+)?$', item.strip(')').replace(',', '')) else item.strip(')')
        elif item.startswith('('):
            return float(item.strip('(').replace(',', '')) \
                if re.match(r'^-?\d+(\.\d+)?$', item.strip('(').replace(',', '')) else item.strip('(')
        return float(item.replace(',', '')) if re.match(r'^-?\d+(\.\d+)?$', item.replace(',', '')) else item

    @staticmethod
    def handle_eps_and_shares_count(input_list):
        i = 0
        while i < len(input_list):
            item = input_list[i]

            if isinstance(item, str) and item.strip().lower() in ["basic", "diluted"]:
                key = item.strip().lower()

                if i + 1 < len(input_list):
                    try:
                        next_value = float(input_list[i + 1].replace(',', ''))

                        if next_value < 1000:
                            input_list[i] = f"Earnings per Share: {key.capitalize()}"
                        else:
                            # Otherwise, it's shares used in the calculation
                            input_list[i] = f"Shares used in Earnings per Share Calculation: {key.capitalize()}"
                    except ValueError:
                        pass
            i += 1

        return input_list

    def parse_financial_data(self, input_list):
        input_list = self.handle_eps_and_shares_count(input_list)
        financial_data = {}
        current_key_parts = []
        values = []

        for item in input_list:
            cleaned_item = self.clean_value(item)

            # If the item is a number, it should be part of the values for the current key
            if isinstance(cleaned_item, (int, float)):
                values.append(cleaned_item)
            else:
                # If we have a current key and values, store them in the dictionary
                if current_key_parts and values:
                    current_key = ' '.join(current_key_parts).strip()
                    financial_data[current_key] = values
                    values = []
                    current_key_parts = []

                # Append the item to the current key parts
                current_key_parts.append(cleaned_item)

        # After the loop, ensure the last key and values are added to the dictionary
        if current_key_parts and values:
            current_key = ' '.join(current_key_parts).strip()
            financial_data[current_key] = values

        # Post-process the keys to ensure quality by merging any neighboring string keys
        processed_data = self._post_process_keys(financial_data)

        return processed_data

    def _post_process_keys(self, data):
        cleaned_data = {}
        previous_key = None

        for key, value in data.items():
            if previous_key and not data[previous_key] and isinstance(previous_key, str) and isinstance(key, str):
                combined_key = f"{previous_key} {key}".strip()
                cleaned_data[combined_key] = value
            else:
                cleaned_data[key] = value
                previous_key = key

        return cleaned_data

    def _extract_data_for_category(self, category):
        self.aggregated_data = []

        image_list = self.image_paths.get(category, [])
        if not image_list:
            return None

        for image_path in image_list:
            inference_results = self.perform_inference(image_path)
            self.extract_text_from_bounding_boxes(inference_results, image_path)

        parsed_data = self.parse_financial_data(self.aggregated_data)

        if parsed_data:
            lengths = [len(v) for v in parsed_data.values() if isinstance(v, list)]
            median_length = int(median(lengths)) if lengths else 0

            # Filter entries where the length of the value matches the median length
            filtered_data = {k: v for k, v in parsed_data.items() if isinstance(v, list) and len(v) == median_length}

            if filtered_data:
                category_dataframe = pd.DataFrame.from_dict(filtered_data, orient='index').T

                financial_unit = 1  # Default to 1 if no financial unit is found
                share_unit = 1  # Default to 1 if no share unit is found

                if 'Unit' in self.class_text_mapping and len(self.class_text_mapping['Unit']) > 0:
                    financial_unit = self.class_text_mapping['Unit'][0].get('financial_unit', 1)
                    share_unit = self.class_text_mapping['Unit'][0].get('share_unit', 1)

                column_titles = self._generate_column_titles()

                # Set column titles (which are now the index names)
                if len(column_titles) == category_dataframe.shape[0]:
                    category_dataframe.index = column_titles

                share_unit_keywords = ["Shares used in Earnings per Share Calculation"]
                exclude_keywords = ["Earnings per Share", "per share"]

                def apply_units(val, col_title):
                    if any(keyword in col_title for keyword in share_unit_keywords):
                        return val * share_unit if isinstance(val, (int, float)) else val

                    if any(keyword in col_title for keyword in exclude_keywords):
                        return val

                    return val * financial_unit if isinstance(val, (int, float)) else val

                for col in category_dataframe.columns:
                    category_dataframe[col] = category_dataframe[col].apply(lambda val: apply_units(val, col))

                return category_dataframe

        return None

    def _generate_column_titles(self):
        """
        Generates column titles by combining 'Column Group Title' and 'Column Title'.
        Distributes the column titles evenly across the available group titles.

        Returns:
            List[str]: The generated column titles.
        """
        column_titles = self.class_text_mapping['Column Title']
        group_titles = self.class_text_mapping['Column Group Title']

        if group_titles:
            chunk_size = len(column_titles) // len(group_titles)
            combined_titles = []

            for i, group_title in enumerate(group_titles):
                start_idx = i * chunk_size
                end_idx = (i + 1) * chunk_size
                combined_titles.extend([f"{group_title} {col_title}" for col_title in column_titles[start_idx:end_idx]])

            return combined_titles
        else:
            return column_titles

    def extract_cash_flow(self):
        self.cash_flow = self._extract_data_for_category('cash_flow')
        return self.cash_flow

    def extract_balance_sheet(self):
        self.balance_sheet = self._extract_data_for_category('balance_sheet')
        return self.balance_sheet

    def extract_income_statement(self):
        self.income_statement = self._extract_data_for_category('income')
        return self.income_statement


    def save_data(self, validate=False):
        """
        Save the dataframes for cash_flow, balance_sheet, and income_statement to the database
        with the appropriate date and symbol fields. If validate is True, it sends an email for
        validation instead of directly saving to the database.
        """

        def is_index_placeholder(index):
            # Check if the index is monotonically increasing or follows expected date structure
            return (index == 0).all() or index.is_monotonic_increasing == False

        validator = ValidateColumns()
        for df_name, df in {'cash_flow': self.cash_flow, 'balance_sheet': self.balance_sheet,
                            'income_statement': self.income_statement}.items():

            if is_index_placeholder(df.index):
                raise ValueError(f"Error: Index for {df_name} contains placeholders; cannot save data.")

            invalid_columns = []
            corrected_columns = {}

            for column_name in df.columns:
                is_valid, corrected_column = validator.validate_line_item(column_name)
                if not is_valid and not corrected_column:
                    invalid_columns.append(column_name)
                elif corrected_column and corrected_column != column_name:
                    corrected_columns[column_name] = corrected_column

            if corrected_columns:
                df.rename(columns=corrected_columns, inplace=True)

            if invalid_columns:
                print(f"Removing invalid columns from {df_name}: {invalid_columns}")
                df.drop(columns=invalid_columns, inplace=True)


        if any(df is None or df.empty for df in [self.cash_flow, self.balance_sheet, self.income_statement]):
            raise ValueError("Error: One or more required dataframes are either None or empty.")

        symbol_id = get_symbol_id(self.symbol)
        report_date_id, filing_date_id = map(get_date_id, [self.report_date, self.filed_as_of_date])

        tables = {
            'cash_flow': self.cash_flow,
            'balance_sheet': self.balance_sheet,
            'income': self.income_statement
        }

        dataframes_to_validate, image_paths_to_attach = {}, []

        for table_name, dataframe in tables.items():
            if validate:
                dataframe['report_date_id'] = report_date_id
                dataframe['filed_as_of_date_id'] = filing_date_id
                dataframe['symbol_id'] = symbol_id
                dataframes_to_validate[table_name] = dataframe.T.reset_index()
            else:
                data_json = dataframe.T.reset_index().to_dict(orient='records')
                record = {
                    'symbol_id': symbol_id,
                    'report_date_id': report_date_id,
                    'filing_date_id': filing_date_id,
                    'data': json.dumps(data_json)
                }
                insert_query = f'''
                    INSERT INTO financials.{table_name} (symbol_id, report_date_id, filing_date_id, data)
                    VALUES (%s, %s, %s, %s)
                '''
                db_connector.run_query(insert_query, (symbol_id, report_date_id, filing_date_id, record['data']),
                                       return_df=False)

        if validate and (dataframes_to_validate or self.image_paths):
            for image_list in self.image_paths.values():
                for image_path in image_list:
                    annotated_image = f"{os.path.splitext(image_path)[0]}_annotated_image{os.path.splitext(image_path)[1]}"
                    image_paths_to_attach += [p for p in [image_path, annotated_image] if os.path.exists(p)]

            if dataframes_to_validate or image_paths_to_attach:
                send_validation_email(dataframes_to_validate, "elijahanderson96@gmail.com", image_paths_to_attach)


if __name__ == "__main__":
    symbol = 'GEO'
    symbol_dir = r'C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\GEO'
    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train57\weights\best.pt"

    filings = os.listdir(os.path.join(symbol_dir, '10-Q'))
    filing = filings[0]

    # for filing in filings[-1:]:
    print(f'PROCCESSING {filing}...')
    self = Extract(symbol=symbol,
                   filings_dir=os.path.join(symbol_dir, '10-Q', filing),
                   model_path=model_path)

    cash_flow = self.extract_cash_flow()
    balance_sheet = self.extract_balance_sheet()
    income = self.extract_income_statement()
    self.save_data(validate=False)
