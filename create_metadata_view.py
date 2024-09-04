from database.database import db_connector


def create_materialized_views():
    try:
        # Materialized View for Cash Flow
        query_cash_flow = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.cash_flow_mv AS
        SELECT
            cf.data,
            s.symbol,
            d.date AS report_date
        FROM
            financials.cash_flow cf
        JOIN
            metadata.symbols s ON cf.symbol_id = s.symbol_id
        JOIN
            metadata.dates d ON cf.report_date_id = d.date_id;
        """

        # Materialized View for Balance Sheet
        query_balance_sheet = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.balance_sheet_mv AS
        SELECT
            bs.data,
            s.symbol,
            d.date AS report_date
        FROM
            financials.balance_sheet bs
        JOIN
            metadata.symbols s ON bs.symbol_id = s.symbol_id
        JOIN
            metadata.dates d ON bs.report_date_id = d.date_id;
        """

        # Materialized View for Income Statement
        query_income = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.income_mv AS
        SELECT
            i.data,
            s.symbol,
            d.date AS report_date
        FROM
            financials.income i
        JOIN
            metadata.symbols s ON i.symbol_id = s.symbol_id
        JOIN
            metadata.dates d ON i.report_date_id = d.date_id;
        """

        # Execute the queries
        db_connector.run_query(query_cash_flow, return_df=False)
        db_connector.run_query(query_balance_sheet, return_df=False)
        db_connector.run_query(query_income, return_df=False)

        # Create indexes on the materialized views
        db_connector.run_query(
            "CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol ON financials.cash_flow_mv (symbol);", return_df=False
        )
        db_connector.run_query(
            "CREATE INDEX IF NOT EXISTS idx_cash_flow_report_date ON financials.cash_flow_mv (report_date);", return_df=False
        )
        db_connector.run_query(
            "CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol ON financials.balance_sheet_mv (symbol);", return_df=False
        )
        db_connector.run_query(
            "CREATE INDEX IF NOT EXISTS idx_balance_sheet_report_date ON financials.balance_sheet_mv (report_date);", return_df=False
        )
        db_connector.run_query(
            "CREATE INDEX IF NOT EXISTS idx_income_symbol ON financials.income_mv (symbol);", return_df=False
        )
        db_connector.run_query(
            "CREATE INDEX IF NOT EXISTS idx_income_report_date ON financials.income_mv (report_date);", return_df=False
        )

        print("Materialized views and indexes created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


# Run the async function
create_materialized_views()
