# Run the below lines to download the corpus of words, which we validate against, if it is not already downloaded.
# import nltk
# nltk.download('words')

import os
import logging
import re
from nltk.corpus import words
from bs4 import BeautifulSoup

from config.filepaths import FILINGS_DIR, APP_LOGS, ROOT_DIR

print(words.words())

class ReportParser:
    def __init__(self, symbol: str, report_type: str) -> None:
        self.symbol = symbol.upper()
        self.report_type = report_type

        self.report_file_path = os.path.join(FILINGS_DIR, self.symbol, self.report_type)
        self.valid_words = set(words.words())
        self._set_logger()

    def read_all_reports_for_symbol(self):
        self.reports = {}
        # for root, dirs, files in os.walk(self.report_file_path):
        #     for file in files:
        #         if file == "full-submission.txt":
        file_path = os.path.join(self.report_file_path, "0000070858-19-000042", "full-submission.txt")
        parent_dir = os.path.basename(os.path.dirname(file_path))
        self.reports[parent_dir] = self._read_file(file_path)
        self.logger.info(f"Read file: {file_path}")

    def parse_reports(self):
        parsed_reports = {}
        # report_content= self.reports[parent_dir]
        for parent_dir, report_content in self.reports.items():
            relevant_text = self._parse_report(report_content)
            parsed_reports[parent_dir] = relevant_text

            self.logger.info(f"Parsed report: {parent_dir}")

        self.parsed_reports = parsed_reports

    def _parse_report(self, report_content):
        soup = BeautifulSoup(report_content, "xml")
        text = soup.get_text(separator=" ", strip=True)
        return self._clean_text(text)

    @staticmethod
    def _is_valid_word(token):
        return token.lower() in self.valid_words

    @staticmethod
    def _clean_text(text):
        # Remove special characters and punctuation
        text = re.sub(r'[^a-zA-Z\s]', '', text)

        # Tokenize the text
        tokens = text.split()

        # Filter out non-words
        valid_tokens = [token for token in tokens if self._is_valid_word(token)]

        # Join the valid tokens back into a single string
        cleaned_text = ' '.join(valid_tokens)

        return cleaned_text

    @staticmethod
    def _read_file(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()

    def _set_logger(self):
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create a console handler and set the logging level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a formatter and add it to the console handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        # Add the console handler to the logger
        self.logger.addHandler(console_handler)

        # Create a file handler and set the logging level
        file_handler = logging.FileHandler(os.path.join(APP_LOGS, 'report_parser.log'))
        file_handler.setLevel(logging.INFO)

        # Add the formatter to the file handler
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

        self.logger.info(f"Initialized ReportParser for {self.symbol} - {self.report_type}")


if __name__ == '__main__':
    self = ReportParser(symbol='BAC', report_type='10-Q')
    self.read_all_reports_for_symbol()
    self.parse_reports()
