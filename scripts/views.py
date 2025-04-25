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


if __name__ == "__main__":
    view_manager = CreateViewManager()
    view_manager.create_market_cap_view(refresh=True)
