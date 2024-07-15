from database.database import db_connector

terminate_query = f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'edgar'
          AND pid <> pg_backend_pid();
        """


db_connector.run_query(terminate_query, return_df=False)
