import nltk
# nltk.download('words')
# nltk.download('stopwords')

import os
import logging
import re
import pandas as pd
from nltk.corpus import words, stopwords
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from config.filepaths import FILINGS_DIR
from database.database import db_connector
from edgar.historical_prices import load_historical_prices


class FeatureExtractor:
    def __init__(self, symbol: str, report_type: str) -> None:
        self.symbol = symbol.upper()
        self.report_type = report_type
        self.report_file_path = os.path.join(FILINGS_DIR, self.symbol, self.report_type)
        self.valid_words = set(word.lower() for word in words.words())
        self.stop_words = set(stopwords.words('english'))
        self.disallowed_words = {'td', 'div', 'solid', 'table', 'tr', 'br', 'href', 'li', 'ul', 'span', 'font',
                                 'script', 'style', 'border', 'width', 'transparent', 'begin',
                                 'message', 'accession', 'number', 'submission', 'type', 'public', 'document', 'count',
                                 'period', 'report', 'date', 'filer', 'company', 'data', 'italic', 'null',
                                 'codification', 'paragraph', 'publisher', 'section', 'subparagraph', 'times', 'width',
                                 'topic', 'role', 'name', 'accounting', 'true', 'false', 'bold', 'display', 'block'}
        self._set_logger()
        self.logger.info(f"Initialized FeatureExtractor for {self.symbol} - {self.report_type}")

        query = f"""
            SELECT date, close
            FROM stock_prices.historical_prices
            WHERE symbol = '{self.symbol}'
            ORDER BY date
        """
        self.logger.info(f"Running query: {query}")
        self.stock_data = db_connector.run_query(query)

        if self.stock_data.empty:
            self.stock_data = load_historical_prices(self.symbol)

    def remove_cleaned_files(self):
        for root, dirs, files in os.walk(self.report_file_path):
            cleaned_text_file_path = os.path.join(root, "cleaned_text.txt")
            if os.path.exists(cleaned_text_file_path):
                self.logger.info(f"Removing {cleaned_text_file_path}.")
                os.remove(cleaned_text_file_path)

    def extract_cleaned_text(self, overwrite=False):
        self.logger.info("Starting extract_cleaned_text")
        data = []
        for root, dirs, files in os.walk(self.report_file_path):
            self.logger.info(f"Processing directory: {root}")
            cleaned_text_file_path = os.path.join(root, "cleaned_text.txt")

            if os.path.exists(cleaned_text_file_path) and not overwrite:
                self.logger.info(f"Skipping directory {root} as cleaned_text.txt already exists")
                continue

            for file in files:
                if file == "full-submission.txt":
                    file_path = os.path.join(root, file)
                    self.logger.info(f"Processing file: {file_path}")
                    report_content = self._read_file(file_path)
                    try:
                        filed_as_of_date = self._extract_filed_as_of_date(report_content)
                        if filed_as_of_date:
                            self.logger.info(f"Filed as of date: {filed_as_of_date}")
                            soup = BeautifulSoup(report_content, "html.parser")
                            text = soup.get_text(separator=" ", strip=True)
                            self.logger.info("Extracted text from HTML")
                            cleaned_text = self._clean_text(text)
                            self.logger.info("Cleaned the extracted text")

                            # Write the cleaned text back to the directory
                            self._write_file(cleaned_text_file_path, cleaned_text)
                            self.logger.info(f"Wrote cleaned text to {cleaned_text_file_path}")

                            labels = self._get_stock_price_labels(filed_as_of_date)
                            if labels is not None:
                                self.logger.info(f"Obtained stock price labels: {labels}")
                                data.append((filed_as_of_date, cleaned_text, labels[0], labels[1]))
                                self.logger.info(f"Extracted cleaned text for {file_path}")
                            else:
                                self.logger.warning(f"Failed to obtain stock price labels for {file_path}")
                    except Exception as e:
                        self.logger.error(f"Error processing {file_path}: {str(e)}")

        self.logger.info("Finished extract_cleaned_text")
        return pd.DataFrame(data=data, columns=['filed_date', 'feature_text', 'label_2_weeks', 'label_12_weeks'])

    def _clean_text(self, text):
        self.logger.info("Starting _clean_text")
        # Remove special characters, punctuation, and numbers
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        self.logger.info("Removed special characters, punctuation, and numbers")

        # Convert to lowercase
        text = text.lower()
        self.logger.info("Converted text to lowercase")

        # Tokenize the text
        tokens = text.split()
        self.logger.info("Tokenized the text")

        # Filter out non-words, stop words, and disallowed words
        valid_tokens = [token for token in tokens if
                        token in self.valid_words and token not in self.stop_words
                        and token not in self.disallowed_words and len(token) > 3]
        self.logger.info("Filtered out non-words, stop words, and disallowed words")

        # Join the valid tokens back into a single string
        cleaned_text = ' '.join(valid_tokens)
        self.logger.info("Joined the valid tokens back into a single string")
        self.logger.info("Finished _clean_text")
        return cleaned_text

    def _get_stock_price_labels(self, filed_as_of_date):
        self.logger.info(f"Starting _get_stock_price_labels for {filed_as_of_date}")
        try:
            filed_date = datetime.strptime(filed_as_of_date, '%Y-%m-%d')
            end_date_2_weeks = filed_date + timedelta(days=14)
            end_date_12_weeks = filed_date + timedelta(days=12 * 7)

            if not self.stock_data.empty:
                self.logger.info("Stock data retrieved from the database")
                self.stock_data_df = pd.DataFrame(self.stock_data, columns=['date', 'close'])
                self.stock_data_df['date'] = pd.to_datetime(self.stock_data_df['date'])
                self.stock_data_df.set_index('date', inplace=True)

                close_price_filed_date = self.stock_data_df.loc[self.stock_data_df.index <= -filed_date].iloc[-1][
                    'close']
                self.logger.info(f"Close price on filed date: {close_price_filed_date}")

                try:
                    close_price_2_weeks = self.stock_data_df.loc[self.stock_data_df.index >= end_date_2_weeks].iloc[0][
                        'close']
                except IndexError:
                    close_price_2_weeks = self.stock_data_df.loc[self.stock_data_df.index <= end_date_2_weeks].iloc[-1][
                        'close']
                self.logger.info(f"Close price 2 weeks later: {close_price_2_weeks}")

                try:
                    close_price_12_weeks = \
                    self.stock_data_df.loc[self.stock_data_df.index >= end_date_12_weeks].iloc[0]['close']
                except IndexError:
                    close_price_12_weeks = \
                    self.stock_data_df.loc[self.stock_data_df.index <= end_date_12_weeks].iloc[-1]['close']
                self.logger.info(f"Close price 12 weeks later: {close_price_12_weeks}")

                label_2_weeks = 1 if close_price_2_weeks > close_price_filed_date else 0
                label_12_weeks = 1 if close_price_12_weeks > close_price_filed_date else 0
                self.logger.info(f"Calculated labels - 2 weeks: {label_2_weeks}, 12 weeks: {label_12_weeks}")

                self.logger.info("Finished _get_stock_price_labels")
                return label_2_weeks, label_12_weeks
            else:
                self.logger.warning("No stock data found in the database")
        except Exception as e:
            self.logger.error(f"Error retrieving stock prices: {str(e)}")

        self.logger.info("Finished _get_stock_price_labels (with error)")
        return None

    @staticmethod
    def _extract_filed_as_of_date(text):
        match = re.search(r'FILED AS OF DATE:\s*(\d{8})', text)
        if match:
            filed_as_of_date = match.group(1)
            formatted_date = f"{filed_as_of_date[:4]}-{filed_as_of_date[4:6]}-{filed_as_of_date[6:]}"
            return formatted_date
        return None

    @staticmethod
    def _read_file(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()

    @staticmethod
    def _write_file(file_path, content):
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def _set_logger(self):
        logger_name = f"{__name__}.{self.symbol}.{self.report_type}"
        self.logger = logging.getLogger(logger_name)
        # Rest of the code remains the same


if __name__ == '__main__':
    # extractor = FeatureExtractor(symbol='BAC', report_type='10-Q')
    # data = extractor.extract_cleaned_text()
    all_data = []

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    for symbol in os.listdir(FILINGS_DIR):
        extractor = FeatureExtractor(symbol=symbol, report_type='10-Q')
        # extractor.remove_cleaned_files()
        data = extractor.extract_cleaned_text()
        all_data.append(data)

    combined_data = pd.concat(all_data, ignore_index=True)
