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
        WHERE fv.total_liabilities IS NOT NULL AND fv.total_equity IS NOT NULL -- Ensure both facts exist for the period
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
        WHERE fv.current_assets IS NOT NULL AND fv.current_liabilities IS NOT NULL -- Ensure both facts exist
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
        -- Use INNER JOIN to ensure we only include price dates where corresponding earnings exist
        INNER JOIN ranked_net_income ni ON mc.symbol = ni.symbol AND ni.filed_date <= mc.date
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

    # --- Additional Ratio Views based on Top 50 Facts ---

    @Job(execution_order=5)
    def create_return_on_assets_view(self, refresh=False):
        """Creates or refreshes the return_on_assets materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.return_on_assets AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('NetIncomeLoss', 'Assets')
              AND fiscal_period = 'Q4' -- Use annual data for consistency
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'NetIncomeLoss' THEN cf.value ELSE NULL END) AS net_income,
                MAX(CASE WHEN cf.fact_name = 'Assets' THEN cf.value ELSE NULL END) AS total_assets
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('NetIncomeLoss', 'Assets')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.net_income,
            fv.total_assets,
            -- Use average assets for ROA calculation (current period + previous period) / 2
            LAG(fv.total_assets) OVER (PARTITION BY s.symbol ORDER BY d.date) as prev_total_assets,
            CASE
                WHEN fv.total_assets IS NOT NULL AND LAG(fv.total_assets) OVER (PARTITION BY s.symbol ORDER BY d.date) IS NOT NULL
                 AND (fv.total_assets + LAG(fv.total_assets) OVER (PARTITION BY s.symbol ORDER BY d.date)) / 2 <> 0
                THEN fv.net_income / ((fv.total_assets + LAG(fv.total_assets) OVER (PARTITION BY s.symbol ORDER BY d.date)) / 2)
                ELSE NULL -- Handle cases with insufficient history or zero average assets
            END AS return_on_assets_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.net_income IS NOT NULL AND fv.total_assets IS NOT NULL -- Ensure both facts exist for the period
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.return_on_assets created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.return_on_assets;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.return_on_assets refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_roa_symbol ON financials.return_on_assets (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_roa_end_date ON financials.return_on_assets (end_date);", return_df=False)
            print("✅ Indices on financials.return_on_assets created successfully.")

    @Job(execution_order=6)
    def create_return_on_equity_view(self, refresh=False):
        """Creates or refreshes the return_on_equity materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.return_on_equity AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('NetIncomeLoss', 'StockholdersEquity')
              AND fiscal_period = 'Q4' -- Use annual data for consistency
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'NetIncomeLoss' THEN cf.value ELSE NULL END) AS net_income,
                MAX(CASE WHEN cf.fact_name = 'StockholdersEquity' THEN cf.value ELSE NULL END) AS total_equity
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('NetIncomeLoss', 'StockholdersEquity')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.net_income,
            fv.total_equity,
            -- Use average equity for ROE calculation (current period + previous period) / 2
            LAG(fv.total_equity) OVER (PARTITION BY s.symbol ORDER BY d.date) as prev_total_equity,
            CASE
                WHEN fv.total_equity IS NOT NULL AND LAG(fv.total_equity) OVER (PARTITION BY s.symbol ORDER BY d.date) IS NOT NULL
                 AND (fv.total_equity + LAG(fv.total_equity) OVER (PARTITION BY s.symbol ORDER BY d.date)) / 2 <> 0
                THEN fv.net_income / ((fv.total_equity + LAG(fv.total_equity) OVER (PARTITION BY s.symbol ORDER BY d.date)) / 2)
                ELSE NULL -- Handle cases with insufficient history or zero average equity
            END AS return_on_equity_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.net_income IS NOT NULL AND fv.total_equity IS NOT NULL -- Ensure both facts exist for the period
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.return_on_equity created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.return_on_equity;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.return_on_equity refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_roe_symbol ON financials.return_on_equity (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_roe_end_date ON financials.return_on_equity (end_date);", return_df=False)
            print("✅ Indices on financials.return_on_equity created successfully.")

    @Job(execution_order=7)
    def create_debt_to_assets_view(self, refresh=False):
        """Creates or refreshes the debt_to_assets materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.debt_to_assets AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('Liabilities', 'Assets')
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'Liabilities' THEN cf.value ELSE NULL END) AS total_liabilities,
                MAX(CASE WHEN cf.fact_name = 'Assets' THEN cf.value ELSE NULL END) AS total_assets
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('Liabilities', 'Assets')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.total_liabilities,
            fv.total_assets,
            CASE
                WHEN fv.total_assets IS NOT NULL AND fv.total_assets <> 0 THEN fv.total_liabilities / fv.total_assets
                ELSE NULL
            END AS debt_to_assets_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.total_liabilities IS NOT NULL AND fv.total_assets IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.debt_to_assets created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.debt_to_assets;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.debt_to_assets refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_dta_symbol ON financials.debt_to_assets (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_dta_end_date ON financials.debt_to_assets (end_date);", return_df=False)
            print("✅ Indices on financials.debt_to_assets created successfully.")

    @Job(execution_order=8)
    def create_equity_multiplier_view(self, refresh=False):
        """Creates or refreshes the equity_multiplier materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.equity_multiplier AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('Assets', 'StockholdersEquity')
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'Assets' THEN cf.value ELSE NULL END) AS total_assets,
                MAX(CASE WHEN cf.fact_name = 'StockholdersEquity' THEN cf.value ELSE NULL END) AS total_equity
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('Assets', 'StockholdersEquity')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.total_assets,
            fv.total_equity,
            CASE
                WHEN fv.total_equity IS NOT NULL AND fv.total_equity <> 0 THEN fv.total_assets / fv.total_equity
                ELSE NULL
            END AS equity_multiplier_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.total_assets IS NOT NULL AND fv.total_equity IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.equity_multiplier created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.equity_multiplier;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.equity_multiplier refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_em_symbol ON financials.equity_multiplier (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_em_end_date ON financials.equity_multiplier (end_date);", return_df=False)
            print("✅ Indices on financials.equity_multiplier created successfully.")

    @Job(execution_order=9)
    def create_cash_ratio_view(self, refresh=False):
        """Creates or refreshes the cash_ratio materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.cash_ratio AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('CashAndCashEquivalentsAtCarryingValue', 'LiabilitiesCurrent')
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'CashAndCashEquivalentsAtCarryingValue' THEN cf.value ELSE NULL END) AS cash_equivalents,
                MAX(CASE WHEN cf.fact_name = 'LiabilitiesCurrent' THEN cf.value ELSE NULL END) AS current_liabilities
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('CashAndCashEquivalentsAtCarryingValue', 'LiabilitiesCurrent')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.cash_equivalents,
            fv.current_liabilities,
            CASE
                WHEN fv.current_liabilities IS NOT NULL AND fv.current_liabilities <> 0 THEN fv.cash_equivalents / fv.current_liabilities
                ELSE NULL
            END AS cash_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.cash_equivalents IS NOT NULL AND fv.current_liabilities IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.cash_ratio created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.cash_ratio;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.cash_ratio refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_cashratio_symbol ON financials.cash_ratio (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_cashratio_end_date ON financials.cash_ratio (end_date);", return_df=False)
            print("✅ Indices on financials.cash_ratio created successfully.")

    @Job(execution_order=10)
    def create_working_capital_view(self, refresh=False):
        """Creates or refreshes the working_capital materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.working_capital AS
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
            (fv.current_assets - fv.current_liabilities) AS working_capital
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.current_assets IS NOT NULL AND fv.current_liabilities IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.working_capital created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.working_capital;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.working_capital refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_wc_symbol ON financials.working_capital (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_wc_end_date ON financials.working_capital (end_date);", return_df=False)
            print("✅ Indices on financials.working_capital created successfully.")

    @Job(execution_order=11)
    def create_interest_coverage_ratio_view(self, refresh=False):
        """Creates or refreshes the interest_coverage_ratio materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.interest_coverage_ratio AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('OperatingIncomeLoss', 'InterestExpense')
              AND fiscal_period = 'Q4' -- Use annual data for consistency
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'OperatingIncomeLoss' THEN cf.value ELSE NULL END) AS operating_income,
                MAX(CASE WHEN cf.fact_name = 'InterestExpense' THEN cf.value ELSE NULL END) AS interest_expense
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('OperatingIncomeLoss', 'InterestExpense')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.operating_income,
            fv.interest_expense,
            CASE
                WHEN fv.interest_expense IS NOT NULL AND fv.interest_expense <> 0 THEN fv.operating_income / fv.interest_expense
                ELSE NULL -- Handle zero or null interest expense
            END AS interest_coverage_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.operating_income IS NOT NULL AND fv.interest_expense IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.interest_coverage_ratio created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.interest_coverage_ratio;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.interest_coverage_ratio refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_icr_symbol ON financials.interest_coverage_ratio (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_icr_end_date ON financials.interest_coverage_ratio (end_date);", return_df=False)
            print("✅ Indices on financials.interest_coverage_ratio created successfully.")

    @Job(execution_order=12)
    def create_operating_cash_flow_ratio_view(self, refresh=False):
        """Creates or refreshes the operating_cash_flow_ratio materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.operating_cash_flow_ratio AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('NetCashProvidedByUsedInOperatingActivities', 'LiabilitiesCurrent')
              AND fiscal_period = 'Q4' -- Use annual data for consistency
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'NetCashProvidedByUsedInOperatingActivities' THEN cf.value ELSE NULL END) AS operating_cash_flow,
                MAX(CASE WHEN cf.fact_name = 'LiabilitiesCurrent' THEN cf.value ELSE NULL END) AS current_liabilities
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('NetCashProvidedByUsedInOperatingActivities', 'LiabilitiesCurrent')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.operating_cash_flow,
            fv.current_liabilities,
            CASE
                WHEN fv.current_liabilities IS NOT NULL AND fv.current_liabilities <> 0 THEN fv.operating_cash_flow / fv.current_liabilities
                ELSE NULL
            END AS operating_cash_flow_ratio
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.operating_cash_flow IS NOT NULL AND fv.current_liabilities IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.operating_cash_flow_ratio created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.operating_cash_flow_ratio;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.operating_cash_flow_ratio refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_ocfr_symbol ON financials.operating_cash_flow_ratio (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_ocfr_end_date ON financials.operating_cash_flow_ratio (end_date);", return_df=False)
            print("✅ Indices on financials.operating_cash_flow_ratio created successfully.")

    @Job(execution_order=13)
    def create_free_cash_flow_view(self, refresh=False):
        """Creates or refreshes the free_cash_flow materialized view."""
        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.free_cash_flow AS
        WITH latest_facts AS (
            SELECT
                symbol_id,
                end_date_id,
                MAX(filed_date_id) as latest_filed_date_id
            FROM financials.company_facts
            WHERE fact_name IN ('NetCashProvidedByUsedInOperatingActivities', 'PaymentsToAcquirePropertyPlantAndEquipment')
              AND fiscal_period = 'Q4' -- Use annual data for consistency
            GROUP BY symbol_id, end_date_id
        ),
        fact_values AS (
            SELECT
                lf.symbol_id,
                lf.end_date_id,
                MAX(CASE WHEN cf.fact_name = 'NetCashProvidedByUsedInOperatingActivities' THEN cf.value ELSE NULL END) AS operating_cash_flow,
                -- Capex is often reported as negative, FCF = CFO - Capex = CFO - (-Value) = CFO + Value
                MAX(CASE WHEN cf.fact_name = 'PaymentsToAcquirePropertyPlantAndEquipment' THEN cf.value ELSE NULL END) AS capex
            FROM latest_facts lf
            JOIN financials.company_facts cf
              ON lf.symbol_id = cf.symbol_id
             AND lf.end_date_id = cf.end_date_id
             AND lf.latest_filed_date_id = cf.filed_date_id
            WHERE cf.fact_name IN ('NetCashProvidedByUsedInOperatingActivities', 'PaymentsToAcquirePropertyPlantAndEquipment')
            GROUP BY lf.symbol_id, lf.end_date_id
        )
        SELECT
            s.symbol,
            d.date AS end_date,
            fv.operating_cash_flow,
            fv.capex,
            (fv.operating_cash_flow + fv.capex) AS free_cash_flow -- Adding because capex is negative
        FROM fact_values fv
        JOIN metadata.symbols s ON fv.symbol_id = s.symbol_id
        JOIN metadata.dates d ON fv.end_date_id = d.date_id
        WHERE fv.operating_cash_flow IS NOT NULL AND fv.capex IS NOT NULL -- Ensure both facts exist
        ORDER BY s.symbol, d.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.free_cash_flow created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.free_cash_flow;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.free_cash_flow refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_fcf_symbol ON financials.free_cash_flow (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_fcf_end_date ON financials.free_cash_flow (end_date);", return_df=False)
            print("✅ Indices on financials.free_cash_flow created successfully.")

    @Job(execution_order=14)
    def create_price_to_book_view(self, refresh=False):
        """
        Creates or refreshes the price_to_book materialized view.
        Uses the market_caps view and finds the latest StockholdersEquity before the market cap date.
        """
        # Ensure market_caps view exists first (dependency)
        self.create_market_cap_view(refresh=False)

        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.price_to_book AS
        WITH latest_equity AS (
            -- Find the latest filed StockholdersEquity for each symbol before or on a given date
            SELECT
                cf.symbol_id,
                d_filed.date AS filed_date,
                d_end.date AS period_end_date,
                cf.value AS total_equity,
                ROW_NUMBER() OVER (PARTITION BY cf.symbol_id, d_filed.date ORDER BY cf.filed_date_id DESC, cf.end_date_id DESC) as rn
            FROM financials.company_facts cf
            JOIN metadata.dates d_filed ON cf.filed_date_id = d_filed.date_id
            JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
            WHERE cf.fact_name = 'StockholdersEquity'
              -- AND cf.fiscal_period = 'Q4' -- Optional: Use only annual equity
        ),
        ranked_equity AS (
             SELECT
                symbol_id,
                filed_date,
                period_end_date,
                total_equity
             FROM latest_equity
             WHERE rn = 1
        )
        SELECT
            mc.date AS price_date,
            mc.symbol,
            mc.price,
            mc.shares,
            eq.total_equity,
            eq.period_end_date AS equity_period_end_date,
            eq.filed_date AS equity_filed_date,
            -- Calculate Book Value Per Share (BVPS)
            CASE
                WHEN mc.shares IS NOT NULL AND mc.shares <> 0 THEN eq.total_equity / mc.shares
                ELSE NULL
            END AS bvps,
            -- Calculate P/B Ratio
            CASE
                WHEN mc.shares IS NOT NULL AND mc.shares <> 0 AND eq.total_equity IS NOT NULL AND eq.total_equity <> 0
                THEN mc.price / (eq.total_equity / mc.shares)
                ELSE NULL
            END AS pb_ratio
        FROM financials.market_caps mc
        -- Use INNER JOIN to ensure we only include price dates where corresponding equity exists
        INNER JOIN ranked_equity eq ON mc.symbol = eq.symbol AND eq.filed_date <= mc.date
        -- Ensure we only join the single most recent equity report available at the time of the price
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mc.symbol, mc.date ORDER BY eq.filed_date DESC, eq.period_end_date DESC) = 1
        ORDER BY mc.symbol, mc.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.price_to_book created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.price_to_book;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.price_to_book refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_pb_ratio_symbol ON financials.price_to_book (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_pb_ratio_price_date ON financials.price_to_book (price_date);", return_df=False)
            print("✅ Indices on financials.price_to_book created successfully.")

    @Job(execution_order=15)
    def create_price_to_cash_flow_view(self, refresh=False):
        """
        Creates or refreshes the price_to_cash_flow materialized view.
        Uses the market_caps view and finds the latest NetCashProvidedByUsedInOperatingActivities before the market cap date.
        """
        # Ensure market_caps view exists first (dependency)
        self.create_market_cap_view(refresh=False)

        create_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS financials.price_to_cash_flow AS
        WITH latest_cash_flow AS (
            -- Find the latest filed Operating Cash Flow for each symbol before or on a given date
            SELECT
                cf.symbol_id,
                d_filed.date AS filed_date,
                d_end.date AS period_end_date,
                cf.value AS operating_cash_flow,
                ROW_NUMBER() OVER (PARTITION BY cf.symbol_id, d_filed.date ORDER BY cf.filed_date_id DESC, cf.end_date_id DESC) as rn
            FROM financials.company_facts cf
            JOIN metadata.dates d_filed ON cf.filed_date_id = d_filed.date_id
            JOIN metadata.dates d_end ON cf.end_date_id = d_end.date_id
            WHERE cf.fact_name = 'NetCashProvidedByUsedInOperatingActivities'
              AND cf.fiscal_period = 'Q4' -- Use annual OCF
        ),
        ranked_cash_flow AS (
             SELECT
                symbol_id,
                filed_date,
                period_end_date,
                operating_cash_flow
             FROM latest_cash_flow
             WHERE rn = 1
        )
        SELECT
            mc.date AS price_date,
            mc.symbol,
            mc.price,
            mc.shares,
            ocf.operating_cash_flow,
            ocf.period_end_date AS ocf_period_end_date,
            ocf.filed_date AS ocf_filed_date,
            -- Calculate Operating Cash Flow Per Share (CFPS)
            CASE
                WHEN mc.shares IS NOT NULL AND mc.shares <> 0 THEN ocf.operating_cash_flow / mc.shares
                ELSE NULL
            END AS cfps,
            -- Calculate P/CF Ratio
            CASE
                WHEN mc.shares IS NOT NULL AND mc.shares <> 0 AND ocf.operating_cash_flow IS NOT NULL AND ocf.operating_cash_flow <> 0
                THEN mc.price / (ocf.operating_cash_flow / mc.shares)
                ELSE NULL
            END AS pcf_ratio
        FROM financials.market_caps mc
        -- Use INNER JOIN to ensure we only include price dates where corresponding OCF exists
        INNER JOIN ranked_cash_flow ocf ON mc.symbol = ocf.symbol AND ocf.filed_date <= mc.date
        -- Ensure we only join the single most recent OCF report available at the time of the price
        QUALIFY ROW_NUMBER() OVER (PARTITION BY mc.symbol, mc.date ORDER BY ocf.filed_date DESC, ocf.period_end_date DESC) = 1
        ORDER BY mc.symbol, mc.date;
        """
        db_connector.run_query(create_query, return_df=False)
        print("✅ Materialized view financials.price_to_cash_flow created or already exists.")

        if refresh:
            refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY financials.price_to_cash_flow;"
            db_connector.run_query(refresh_query, return_df=False)
            print("✅ Materialized view financials.price_to_cash_flow refreshed successfully.")
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_pcf_ratio_symbol ON financials.price_to_cash_flow (symbol);", return_df=False)
            db_connector.run_query("CREATE INDEX IF NOT EXISTS idx_pcf_ratio_price_date ON financials.price_to_cash_flow (price_date);", return_df=False)
            print("✅ Indices on financials.price_to_cash_flow created successfully.")


if __name__ == "__main__":
    view_manager = CreateViewManager()
    # Refresh all views
    # Refresh all views in order (ensure dependencies like market_caps run first)
    view_manager.create_market_cap_view(refresh=True)           # 0
    view_manager.create_debt_to_equity_view(refresh=True)      # 1
    view_manager.create_current_ratio_view(refresh=True)       # 2
    view_manager.create_pe_ratio_view(refresh=True)            # 3
    view_manager.create_fact_activity_view(refresh=True)       # 4
    # New views based on top 50 facts
    view_manager.create_return_on_assets_view(refresh=True)    # 5
    view_manager.create_return_on_equity_view(refresh=True)    # 6
    view_manager.create_debt_to_assets_view(refresh=True)      # 7
    view_manager.create_equity_multiplier_view(refresh=True)   # 8
    view_manager.create_cash_ratio_view(refresh=True)          # 9
    view_manager.create_working_capital_view(refresh=True)     # 10
    view_manager.create_interest_coverage_ratio_view(refresh=True) # 11
    view_manager.create_operating_cash_flow_ratio_view(refresh=True) # 12
    view_manager.create_free_cash_flow_view(refresh=True)      # 13
    view_manager.create_price_to_book_view(refresh=True)       # 14
    view_manager.create_price_to_cash_flow_view(refresh=True)  # 15
