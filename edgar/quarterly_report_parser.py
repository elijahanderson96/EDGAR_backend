import os
import logging
import re

from bs4 import BeautifulSoup

from config.filepaths import FILINGS_DIR, APP_LOGS, ROOT_DIR


class ReportParser:
    def __init__(self, symbol: str, report_type: str) -> None:
        self.symbol = symbol.upper()
        self.report_type = report_type

        self.report_file_path = os.path.join(FILINGS_DIR, self.symbol, self.report_type)

        self._set_logger()

    def read_all_reports_for_symbol(self):
        self.reports = {}
        # for root, dirs, files in os.walk(self.report_file_path):
        #     for file in files:
        #         if file == "full-submission.txt":
        file_path = os.path.join(self.report_file_path, "0001564590-23-000733", "full-submission.txt")
        parent_dir = os.path.basename(os.path.dirname(file_path))
        self.reports[parent_dir] = self._read_file(file_path)
        self.logger.info(f"Read file: {file_path}")

    def parse_reports(self):
        parsed_reports = {}
        #report_content= self.reports[parent_dir]
        for parent_dir, report_content in self.reports.items():
            print(parent_dir)
            parsed_tables = self._parse_report(report_content)
            parsed_reports[parent_dir] = parsed_tables

            self.logger.info(f"Parsed report: {parent_dir}")

        self.parsed_reports = parsed_reports

    def _parse_report(self, report_content):
        # Extract the text from the report using BeautifulSoup
        soup = BeautifulSoup(report_content, "html.parser")
        p = soup.find_all('p')
        text_content = soup.get_text()

        # Remove unnecessary whitespace and newlines
        text_content = ' '.join(text_content.split())

        with open(f'{ROOT_DIR}/soup_text.txt', 'w', encoding='utf-8') as text_file:
            text_file.write(text_content)



    def _read_file(self, file_path):
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
    self = ReportParser(symbol='MSFT', report_type='10-Q')
    self.read_all_reports_for_symbol()
    self.parse_reports()
