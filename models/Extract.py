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

from database.async_database import db_connector
from database.helpers import get_date_id, get_symbol_id
from helpers.email_utils import send_validation_email


class Extract:
    def __init__(self, symbol, filings_dir, model_path):
        self.aggregated_data = []
        self.symbol = symbol
        self.model_path = model_path
        self.filings_dir = filings_dir
        self.primary_document = self.find_primary_document()
        self.full_submission = self.find_full_submission()
        self.filed_as_of_date = self._extract_filed_as_of_date()
        self.report_date = self.extract_report_date()

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

            print(f"Annotated image saved to: {result_output_path}")
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

            print(f"Extracted text from bounding boxes: {self.class_text_mapping}")
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
        # Collect all text into a list from all the different data bounding boxes
        all_texts = []
        [all_texts.append(text) for bbox, text, prob in ocr_result if text and text not in ('S', '$')]

        # aggregate the data into a list for each table type (balance sheet, income, cash flow).
        self.aggregated_data.extend(all_texts)

    @staticmethod
    def clean_value(item):
        """
        Clean and convert values, stripping parentheses and converting to negative if needed.
        Args:
            item (str): The string to clean and convert.

        Returns:
            float or str: The cleaned and converted value or the original string if it's not a number.
        """
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

    def parse_financial_data(self, input_list):
        """
        Parses financial data from a list into a dictionary with keys and associated numerical values.

        Args:
            input_list (list): The list containing financial data.

        Returns:
            dict: The parsed financial data as a dictionary.
        """
        financial_data = {}
        current_key = None
        values = []

        for item in input_list:
            cleaned_item = self.clean_value(item)

            # If it's a number, it should be part of the values for the current key
            if isinstance(cleaned_item, (int, float)):
                values.append(cleaned_item)
            else:
                # If we have a current key and values, store them in the dictionary
                if current_key and values:
                    financial_data[current_key] = values
                    values = []

                # Update current key
                current_key = cleaned_item

        # After loop, ensure the last key and values are added to the dictionary
        if current_key and values:
            financial_data[current_key] = values

        return financial_data

    def _extract_data_for_category(self, category):
        """
        Generic method to extract data for a given category (cash flow, balance sheet, or income).

        Args:
            category (str): The category of data to extract (e.g., 'cash_flow', 'balance_sheet', 'income').

        Returns:
            pd.DataFrame or None: The extracted data as a DataFrame or None if no data found.
        """
        self.aggregated_data = []

        image_list = self.image_paths.get(category, [])
        if not image_list:
            return None

        for image_path in image_list:
            inference_results = self.perform_inference(image_path)
            text_mapping = self.extract_text_from_bounding_boxes(inference_results, image_path)

            # Flatten the extracted data into a single list
            self.aggregated_data.extend(text_mapping['Data'])

        # After processing all images, parse the aggregated data
        parsed_data = self.parse_financial_data(self.aggregated_data)

        # Apply filtering logic based on the median length of values
        if parsed_data:
            # Calculate the lengths of the values
            lengths = [len(v) for v in parsed_data.values() if isinstance(v, list)]
            median_length = int(median(lengths)) if lengths else 0

            # Filter entries where the length of the value matches the median length
            filtered_data = {k: v for k, v in parsed_data.items() if isinstance(v, list) and len(v) == median_length}

            # Convert filtered data to DataFrame if there are valid entries
            if filtered_data:
                category_dataframe = pd.DataFrame.from_dict(filtered_data, orient='index').T

                financial_unit = self.class_text_mapping['Unit'][0].get('financial_unit', 1)
                share_unit = self.class_text_mapping['Unit'][0].get('share_unit', 1)

                column_titles = self._generate_column_titles()

                print(column_titles)

                # Set column titles (which are now the index names)
                if len(column_titles) == category_dataframe.shape[0]:
                    category_dataframe.index = column_titles

                # Apply financial and share units to the DataFrame
                # If the column contains "basic", "diluted", or "share", apply share_unit instead of financial_unit
                excluded_keywords = ["basic", "diluted", "share"]

                def apply_units(val, col_title):
                    if any(keyword in col_title.lower() for keyword in excluded_keywords):
                        return val * share_unit if isinstance(val, (int, float)) else val
                    return val * financial_unit if isinstance(val, (int, float)) else val

                # Apply units to all values in the DataFrame based on column names
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
            # Determine the chunk size for each group title
            chunk_size = len(column_titles) // len(group_titles)
            combined_titles = []

            # Append group title to corresponding column titles
            for i, group_title in enumerate(group_titles):
                start_idx = i * chunk_size
                end_idx = (i + 1) * chunk_size
                combined_titles.extend([f"{group_title} {col_title}" for col_title in column_titles[start_idx:end_idx]])

            return combined_titles
        else:
            # Use column titles only if no group titles are present
            return column_titles

    def extract_cash_flow(self):
        return self._extract_data_for_category('cash_flow')

    def extract_balance_sheet(self):
        return self._extract_data_for_category('balance_sheet')

    def extract_income_statement(self):
        return self._extract_data_for_category('income')

    def save_data(self, validate=False):
        """
        Save the dataframes for cash_flow, balance_sheet, and income_statement to the database
        with the appropriate date and symbol fields. If validate is True, it sends an email for
        validation instead of directly saving to the database.
        """
        symbol_id = get_symbol_id(self.symbol)
        report_date_id, filing_date_id = get_date_id(self.report_date), get_date_id(self.filed_as_of_date)

        tables = {
            'cash_flow': self.cash_flow,
            'balance_sheet': self.balance_sheet,
            'income': self.income_statement
        }

        dataframes_to_validate = {}

        for table_name, dataframe in tables.items():
            if dataframe is not None and not dataframe.empty:
                if validate:
                    # Add report_date and filed_as_of_date as columns to the DataFrame for validation
                    dataframe['report_date_id'] = report_date_id
                    dataframe['filed_as_of_date_id'] = filing_date_id
                    dataframe['symbol_id'] = symbol_id

                    # Transpose and reset index to make the DataFrame ready for validation
                    dataframe = dataframe.T.reset_index()
                    dataframes_to_validate[table_name] = dataframe
                else:
                    # For direct DB insertion, prepare the dataframe for insertion without adding extra columns
                    dataframe = dataframe.T.reset_index()

                    # Collapse dataframe to JSON-like dictionary for DB insertion
                    data_json = dataframe.to_dict(orient='records')

                    # Prepare the record for insertion
                    record = {
                        'symbol_id': symbol_id,
                        'report_date_id': report_date_id,
                        'filing_date_id': filing_date_id,
                        'data': json.dumps(data_json)  # Convert the data to a JSON string
                    }

                    # Insert the record into the appropriate financial table
                    insert_query = f'''
                        INSERT INTO financials.{table_name} (symbol_id, report_date_id, filing_date_id, data)
                        VALUES (%s, %s, %s, %s)
                    '''
                    db_connector.run_query(insert_query, (
                        record['symbol_id'], record['report_date_id'], record['filing_date_id'], record['data']),
                                           return_df=False)

        if validate and (dataframes_to_validate or self.image_paths):
            # Fetch both annotated and unannotated images
            image_paths_to_attach = []

            for category, image_list in self.image_paths.items():
                for image_path in image_list:
                    # Add unannotated image to the list
                    if os.path.exists(image_path):
                        image_paths_to_attach.append(image_path)

                    # Construct the path for the annotated image
                    annotated_image = os.path.splitext(image_path)[0] + "_annotated_image" + \
                                      os.path.splitext(image_path)[1]

                    if os.path.exists(annotated_image):
                        image_paths_to_attach.append(annotated_image)

            # Send the dataframes and images (both annotated and unannotated) for validation via email
            if dataframes_to_validate or image_paths_to_attach:
                recipient_email = "elijahanderson96@gmail.com"  # Replace with the actual recipient email
                send_validation_email(dataframes_to_validate, recipient_email, image_paths_to_attach)

            print("Data and images (both annotated and unannotated) sent for validation via email.")
        elif not validate:
            print("Data saved to the database.")


if __name__ == "__main__":
    symbol = 'MU'
    symbol_dir = r'C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\MU'
    model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train5\weights\best.pt"

    filings = os.listdir(os.path.join(symbol_dir, '10-Q'))
    filing = filings[-2]

    # for filing in filings[-1:]:
    print(f'PROCCESSING {filing}...')
    self = Extract(symbol=symbol,
                   filings_dir=os.path.join(symbol_dir, '10-Q', filing),
                   model_path=model_path)

    cash_flow = self.extract_cash_flow()
    balance_sheet = self.extract_balance_sheet()
    income = self.extract_income_statement()
    #self.save_data()
