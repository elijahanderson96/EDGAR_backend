import logging
from typing import Optional

import pandas as pd
import requests
from pandas import DataFrame

from database.database import db_connector
from utils.dataframes import find_new_records


class CompanyFactTransformer:
    """
    Transforms financial fact data for a given company symbol and fact name.
    """

    def __init__(self, symbol: str, fact_name: str):
        """
        Initializes the transformer with a company symbol and a specific financial fact name.

        :param symbol: The stock ticker symbol of the company.
        :param fact_name: The financial fact to retrieve and transform.
        """
        self.symbol = symbol
        self.fact_name = fact_name
        self.data: Optional[dict] = None

        self.symbols = db_connector.run_query('SELECT symbol_id, symbol FROM metadata.symbols;')
        self.symbols_mapping = dict(zip(self.symbols['symbol'], self.symbols['symbol_id']))

        self.dates = db_connector.run_query('SELECT date_id, date FROM metadata.dates;')
        self.dates['date'] = pd.to_datetime(self.dates['date'])
        self.dates_mapping = dict(zip(self.dates['date'], self.dates['date_id']))

    def fetch_data(self):
        """
        Fetches financial fact data from the local API endpoint.
        Raises an exception if the request fails.
        """
        url = f"http://localhost:8000/financials/facts/{self.symbol.upper()}/{self.fact_name}"
        response = requests.get(url, headers={'X-API-KEY': '12345'})

        if response.status_code != 200:
            raise Exception(f"Error fetching {self.fact_name} for {self.symbol}.")

        self.data = response.json()

    def standardize_data_to_quarterly(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes the financial data to quarterly periods.

        :param df: DataFrame containing raw financial fact data.
        :return: DataFrame standardized to quarterly data.
        """
        # Convert date columns to datetime objects
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['filed_date'] = pd.to_datetime(df['filed_date'])

        # Calculate the duration of each reporting period
        df['date_range_days'] = (df['end_date'] - df['start_date']).dt.days

        # Sort the DataFrame by start and end dates
        df = df.sort_values(by=['start_date', 'end_date'])

        # If any start dates are missing, drop duplicates based on end_date
        if df['start_date'].isna().any():
            return df.drop_duplicates(subset=['end_date'])

        # If all periods are annual (duration > 300 days), return as is
        if all(df['date_range_days'] > 300):
            return df

        # Group data by start_date to process overlapping periods
        start_date_groups = df.groupby('start_date')
        standardized_quarters = []

        for start_date, group in start_date_groups:
            group = group.sort_values(by='end_date')
            if len(group) == 1:
                record = group.iloc[0].to_dict()
                if 80 <= record['date_range_days'] <= 105:
                    standardized_quarters.append(record)
            else:
                prev_value = 0
                prev_end_date = None

                for _, row in group.iterrows():
                    record = row.to_dict()
                    if prev_end_date is None:
                        if 80 <= record['date_range_days'] <= 105:
                            standardized_quarters.append(record)
                            prev_value = record['value']
                            prev_end_date = record['end_date']
                    else:
                        if record['end_date'] > prev_end_date:
                            quarter_record = record.copy()
                            quarter_record['start_date'] = prev_end_date + pd.Timedelta(days=1)
                            quarter_record['date_range_days'] = (
                                    quarter_record['end_date'] - quarter_record['start_date']).days
                            if 80 <= quarter_record['date_range_days'] <= 105:
                                quarter_record['value'] = record['value'] - prev_value
                                standardized_quarters.append(quarter_record)

                            prev_value = record['value']
                            prev_end_date = record['end_date']

        # Create a DataFrame from the standardized quarters
        result_df = pd.DataFrame(standardized_quarters).drop_duplicates(subset=['start_date', 'end_date'])

        if not result_df.empty:
            result_df = result_df.sort_values(by=['end_date'])

        return result_df

    def transform_data(self) -> Optional[pd.DataFrame]:
        """
        Fetches and transforms the financial fact data into a standardized quarterly format,
        calculating QoQ growth, YoY growth, average growth over 8 quarters, Z-score,
        rolling standard deviation, and growth consistency ratio.

        :return: Transformed DataFrame or None if no data is available.
        """
        self.fetch_data()

        if not self.data:
            return None

        df = pd.DataFrame(self.data['data'])

        if df.empty:
            raise ValueError('No data is available.')

        # Convert date columns to datetime
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['filed_date'] = pd.to_datetime(df['filed_date'])

        # Sort and remove duplicate entries
        df.sort_values(by=['end_date', 'filed_date'], inplace=True)
        df.drop_duplicates(subset=['start_date', 'end_date'], inplace=True)

        # Group data by fiscal year and standardize each group to quarterly data
        grouped_df = df.groupby('fiscal_year')
        quarterly_results = grouped_df.apply(
            lambda row: self.standardize_data_to_quarterly(row), include_groups=False
        )

        # Add the symbol to the results
        quarterly_results['symbol'] = self.symbol

        # Sort by end_date to ensure correct calculation order
        quarterly_results.sort_values(by='end_date', inplace=True)

        # Calculate QoQ and YoY growth
        quarterly_results['qoq_growth'] = quarterly_results['value'].pct_change(periods=1).round(3)
        quarterly_results['yoy_growth'] = quarterly_results['value'].pct_change(periods=4).round(3)

        # # Calculate average growth over the past 8 quarters (2 years)
        # quarterly_results['avg_growth_8q'] = (
        #     quarterly_results['qoq_growth']
        #     .rolling(window=4, min_periods=4)
        #     .mean()
        # )

        # # Calculate Z-score
        # mean_value = quarterly_results['value'].mean()
        # std_value = quarterly_results['value'].std()
        # quarterly_results['z_score'] = (quarterly_results['value'] - mean_value) / std_value
        #
        # # Calculate rolling standard deviation over a 4-quarter window
        # quarterly_results['rolling_std_4q'] = (
        #     quarterly_results['value']
        #     .rolling(window=4, min_periods=4)
        #     .std()
        # )
        #
        # # Calculate rolling growth consistency ratio over 8 quarters
        # def growth_consistency_ratio(x):
        #     positive_growths = (x > 0).sum()
        #     return positive_growths / len(x) if len(x) > 0 else None
        #
        # quarterly_results['growth_consistency_ratio'] = (
        #     quarterly_results['qoq_growth']
        #     .rolling(window=4, min_periods=4)
        #     .apply(growth_consistency_ratio, raw=True)
        # )
        self.data = quarterly_results[
            [
                'symbol',
                'fact_name',
                'unit',
                'start_date',
                'end_date',
                'filed_date',
                'value',
                'qoq_growth',
                'yoy_growth',
                # 'avg_growth_8q',
                # 'z_score',
                # 'rolling_std_4q',
                # 'growth_consistency_ratio',
            ]
        ]
        return self.data

    def save_to_db(self):
        existing_data = db_connector.run_query('SELECT symbol_id, '
                                               'fact_name, '
                                               'unit, '
                                               'start_date_id, '
                                               'end_date_id, '
                                               'filed_date_id, '
                                               'value, '
                                               'qoq_growth, '
                                               'yoy_growth '
                                               'FROM financials.quarterly_facts '
                                               f'WHERE symbol_id={str(self.symbols_mapping[self.symbol])};')

        if not self.data['start_date'].isna().any():
            self.data['start_date_id'] = self.data['start_date'].apply(lambda x: self.dates_mapping[x])
        else:
            self.data['start_date_id'] = None
            existing_data['start_date_id'] = None

        self.data['end_date_id'] = self.data['end_date'].apply(lambda x: self.dates_mapping[x])
        self.data['filed_date_id'] = self.data['filed_date'].apply(lambda x: self.dates_mapping[x])
        self.data['symbol_id'] = self.data['symbol'].apply(lambda x: self.symbols_mapping[x])

        self.data = self.data[
            ['symbol_id', 'start_date_id', 'end_date_id', 'filed_date_id', 'fact_name', 'unit', 'value', 'qoq_growth',
             'yoy_growth']]

        self.data.reset_index(drop=True, inplace=True)

        for column in existing_data.columns:
            self.data[column] = self.data[column].astype(existing_data[column].dtype)

        unique_cols = ['symbol_id', 'start_date_id', 'end_date_id', 'filed_date_id', 'fact_name']
        df = find_new_records(self.data, existing_data, unique_cols)

        print(df.shape)

        if not df.empty:
            db_connector.insert_dataframe(df, schema='financials', name='quarterly_facts', if_exists='append')

        return df


def process_symbol_fact(symbol: str, fact_name: str) -> tuple[DataFrame | None, DataFrame] | None:
    """
    Processes a single combination of symbol and fact name.

    :param symbol: The stock ticker symbol.
    :param fact_name: The financial fact to process.
    :return: Transformed DataFrame or None if an error occurs.
    """
    try:
        transformer = CompanyFactTransformer(symbol=symbol, fact_name=fact_name)
        data = transformer.transform_data()
        data_to_insert = transformer.save_to_db()
        return data, data_to_insert

    except Exception as e:
        print(f"Error processing {symbol} - {fact_name}: {e}")
        return None


def refresh_standrdized_quarters(logger):
    from edgar.symbols import symbols

    symbol_list = symbols['symbol']

    # Define the set of financial fact names to process
    fact_names = {
        'NetIncomeLoss',
        'Assets',
        'Liabilities',
        'NetCashProvidedByUsedInOperatingActivities',
        'AssetsCurrent',
        'LiabilitiesCurrent'
    }

    for symbol in symbol_list:
        for fact_name in fact_names:
            try:
                data, inserted_data = process_symbol_fact(symbol, fact_name, logger=logger)
                logger.info(f"Inserted {inserted_data.shape[0]} records into database for {symbol}:{fact_name}.")
            except Exception as e:
                print(e)

if __name__ == '__main__':
    root_logger = logging.getLogger()
