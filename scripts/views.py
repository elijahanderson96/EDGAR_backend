from database.database import db_connector
from orchestration.decorator import Pipeline, Job

@Pipeline(schedule='* * * * *')
class CreateViewManager:

    @Job(execution_order=0)
    def create_market_cap_view(self, refresh=False):
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.market_caps AS
        WITH max_date_cf AS (
            SELECT
                cf.symbol_id,
                MAX(cf.filed_date_id) AS latest_filed_date_id,
                (
                    SELECT cf_inner.value
                    FROM financials.company_facts cf_inner
                    WHERE cf_inner.symbol_id = cf.symbol_id
                      AND cf_inner.end_date_id = (
                          SELECT MAX(end_date_id)
                          FROM financials.company_facts
                          WHERE symbol_id = cf.symbol_id
                      )
                      AND cf_inner.fact_name IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
                    LIMIT 1
                ) AS shares_outstanding
            FROM financials.company_facts cf
            WHERE cf.fact_name IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
            GROUP BY cf.symbol_id
        )
        SELECT
            d.date,
            s.symbol,
            (mcf.shares_outstanding * hd.close) AS market_cap,
            mcf.shares_outstanding AS shares,
            hd.close AS price
        FROM max_date_cf mcf
        JOIN financials.historical_data hd ON mcf.symbol_id = hd.symbol_id
        JOIN metadata.dates d ON d.date_id = hd.date_id
        JOIN metadata.symbols s ON mcf.symbol_id = s.symbol_id
        ORDER BY s.symbol, d.date;
        """

        db_connector.run_query(create_query, return_df=False)

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.market_caps;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view refreshed successfully.")

            # ✅ Create an index on `symbol` after refresh
            create_index_query = """
            CREATE INDEX IF NOT EXISTS idx_market_caps_symbol ON financials.market_caps (symbol);
            """
            db_connector.run_query(create_index_query, return_df=False)
            print("✅ Index on `symbol` created successfully.")

    @Job(execution_order=1)
    def create_debt_to_equity_view(self, refresh=False):
        """Creates or refreshes the debt_to_equity materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.debt_to_equity AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('Liabilities', 'StockholdersEquity')
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'Liabilities' THEN cf.value ELSE NULL END) AS total_liabilities,
                MAX(CASE WHEN cf.fact_name = 'StockholdersEquity' THEN cf.value ELSE NULL END) AS total_equity
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('Liabilities', 'StockholdersEquity')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.total_liabilities,
            fv.total_equity,
            CASE
                WHEN fv.total_equity IS NOT NULL AND fv.total_equity <> 0 THEN fv.total_liabilities / fv.total_equity
                ELSE NULL
            END AS debt_to_equity_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.debt_to_equity created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.debt_to_equity;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.debt_to_equity refreshed successfully.")

            # Create indices
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_debt_to_equity_symbol ON financials.debt_to_equity (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_debt_to_equity_end_date ON financials.debt_to_equity (end_date);", return_df=False)
            print("✅ Indices on financials.debt_to_equity created successfully.")

    @Job(execution_order=2)
    def create_current_ratio_view(self, refresh=False):
        """Creates or refreshes the current_ratio materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.current_ratio AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('AssetsCurrent', 'LiabilitiesCurrent')
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'AssetsCurrent' THEN cf.value ELSE NULL END) AS current_assets,
                MAX(CASE WHEN cf.fact_name = 'LiabilitiesCurrent' THEN cf.value ELSE NULL END) AS current_liabilities
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('AssetsCurrent', 'LiabilitiesCurrent')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.current_assets,
            fv.current_liabilities,
            CASE
                WHEN fv.current_liabilities IS NOT NULL AND fv.current_liabilities <> 0 THEN fv.current_assets / fv.current_liabilities
                ELSE NULL
            END AS current_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.current_ratio created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.current_ratio;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.current_ratio refreshed successfully.")

            # Create indices
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_current_ratio_symbol ON financials.current_ratio (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_current_ratio_end_date ON financials.current_ratio (end_date);", return_df=False)
            print("✅ Indices on financials.current_ratio created successfully.")

    @Job(execution_order=3)
    def create_pe_ratio_view(self, refresh=False):
        """
        Creates or refreshes the price_to_earnings materialized view.
        Uses the market_caps view and finds the latest NetIncomeLoss before the market cap date.
        """
        # Ensure market_caps view exists first (dependency)
        self.create_market_cap_view(refresh=False) # Create if not exists, don't refresh here

        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.price_to_earnings AS
        WITH latest_net_income AS (
            -- Find the latest filed NetIncomeLoss for each symbol before or on a given date
            SELECT
                cf.symbol_id,
                d_filed.date AS filed_date, -- The date the fact was filed
                d_end.date AS period_end_date, -- The end date of the reporting period for the fact
                cf.value AS net_income,
                -- Assign a rank based on filed_date descending for each symbol
                ROW_NUMBER() OVER (PARTITION BY cf.symbol_id, d_filed.date ORDER BY cf.filed_date_id DESC, cf.end_date_id DESC) as rn
            FROM financials.company_facts cf
            JOIN metadata.dates d_filed ON cf.filed_date_id = d_filed.date_id
            JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
            WHERE cf.fact_name = 'NetIncomeLoss'
              AND cf.fiscal_period = 'Q4' -- Consider using annual net income (Q4 often represents full year) or TTM
              -- Alternative: Use TTM logic if quarterly data is reliable
        ),
        ranked_net_income AS (
             SELECT
                symbol_id,
                filed_date,
                period_end_date,
                net_income
             FROM latest_net_income
             WHERE rn = 1 -- Select the single latest filing for that symbol on that filed_date
        )
        SELECT
            mc.date AS price_date,
            mc.symbol,
            mc.price,
            mc.shares,
            ni.net_income,
            ni.period_end_date AS earnings_period_end_date,
            ni.filed_date AS earnings_filed_date,
            -- Calculate EPS using shares from market_caps and latest net_income
            CASE
                WHEN mc.shares IS NOT NULL AND mc.shares <> 0 THEN ni.net_income / mc.shares
                ELSE NULL
            END AS eps,
            -- Calculate P/E Ratio
            CASE
                WHEN mc.shares IS NOT NULL AND mc.shares <> 0 AND ni.net_income IS NOT NULL AND ni.net_income <> 0
                THEN mc.price / (ni.net_income / mc.shares)
                ELSE NULL
            END AS pe_ratio
        FROM financials.market_caps mc
        -- Find the most recent net income filed *before or on* the market cap's price date
        LEFT JOIN ranked_net_income ni ON mc.symbol = ni.symbol AND ni.filed_date <= mc.date
        -- Ensure we only join the single most recent earnings report available at the time of the price
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mc.symbol, mc.date ORDER BY ni.filed_date DESC, ni.period_end_date DESC) = 1
        ORDER BY mc.symbol, mc.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.price_to_earnings created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.price_to_earnings;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.price_to_earnings refreshed successfully.")

            # Create indices
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_pe_ratio_symbol ON financials.price_to_earnings (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_pe_ratio_price_date ON financials.price_to_earnings (price_date);", return_df=False)
            print("✅ Indices on financials.price_to_earnings created successfully.")

    @Job(execution_order=4)
    def create_fact_activity_view(self, refresh=False):
        """
        Creates or refreshes a materialized view summarizing the activity status of each fact_name.
        """
        # Note: This view is placed in the 'metadata' schema as it describes the facts themselves.
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS metadata.fact_activity_status AS
        WITH FactDates AS (
            SELECT
                fact_name,
                MIN(d_filed.date) AS first_filed_date,
                MAX(d_filed.date) AS last_filed_date,
                COUNT(*) AS total_reports,
                COUNT(DISTINCT symbol_id) AS distinct_symbols_reporting
            FROM financials.company_facts cf
            JOIN metadata.dates d_filed ON cf.filed_date_id = d_filed.date_id
            GROUP BY fact_name
        )
        SELECT
            fd.fact_name,
            fd.first_filed_date,
            fd.last_filed_date,
            fd.total_reports,
            fd.distinct_symbols_reporting,
            -- Define 'Active' if last reported within the last 2 years (adjust threshold as needed)
            CASE
                WHEN fd.last_filed_date >= (CURRENT_DATE - INTERVAL '2 years') THEN 'Active'
                ELSE 'Inactive'
            END AS activity_status,
            -- Calculate years since last report (integer part)
            EXTRACT(YEAR FROM AGE(CURRENT_DATE, fd.last_filed_date))::INTEGER AS years_since_last_report
        FROM FactDates fd
        ORDER BY fd.fact_name;
        """
        # Use the synchronous connector for schema changes/view creation
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view metadata.fact_activity_status created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY metadata.fact_activity_status;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view metadata.fact_activity_status refreshed successfully.")

            # Create indices for faster querying
            db_connector.run_query("CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_activity_status_fact_name ON metadata.fact_activity_status (fact_name);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_fact_activity_status_last_filed ON metadata.fact_activity_status (last_filed_date);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_fact_activity_status_status ON metadata.fact_activity_status (activity_status);", return_df=False)
            print("✅ Indices on metadata.fact_activity_status created successfully.")


if __name__ == "__main__":
    view_manager = CreateViewManager()
    # Refresh all views
    # Refresh all views in order
    view_manager.create_market_cap_view(refresh=True)
    view_manager.create_debt_to_equity_view(refresh=True)
    view_manager.create_current_ratio_view(refresh=True)
    view_manager.create_pe_ratio_view(refresh=True)
    view_manager.create_fact_activity_view(refresh=True)
