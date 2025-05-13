from compare import Compare
from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG
import pandas as pd
import pyodbc
import snowflake.connector

class ViewComparer:
    def __init__(self):
        self.snowflake_config = SNOWFLAKE_CONFIG
        self.sql_server_config = SQL_SERVER_CONFIG
    
        # Snowflake connection
        self.sf_conn = snowflake.connector.connect(**self.snowflake_config)
        self.sf_cursor = self.sf_conn.cursor()
        # Ensure correct DB/Schema
        self.sf_cursor.execute("USE DATABASE MY_DB")
        self.sf_cursor.execute("USE SCHEMA MY_SCHEMA")

        # SQL Server connection string
        sql_conn_str = (
            f"DRIVER={self.sql_server_config['driver']};"
            f"SERVER={self.sql_server_config['server']};"
            f"DATABASE={self.sql_server_config['database']};"
            f"UID={self.sql_server_config['username']};"
            f"PWD={self.sql_server_config['password']}"
        )
        self.sql_conn = pyodbc.connect(sql_conn_str)
        self.sql_cursor = self.sql_conn.cursor()


    def get_snowflake_view_names(self):
        """Return list of fully-qualified views: DB.SCHEMA.VIEW_NAME"""
        query = """
            SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = 'MY_SCHEMA'
        """
        self.sf_cursor.execute(query)
        return [row[0] for row in self.sf_cursor.fetchall()]


    def get_sqlserver_view_names(self):
        """Return list of fully-qualified views: SCHEMA.VIEW_NAME"""
        query = """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
        """
        self.sql_cursor.execute(query)
        return [row[0] for row in self.sql_cursor.fetchall()]


    def fetch_view_data(self, view_name, platform):
        """SELECT * from view_name on the given platform into a DataFrame."""
        query = f"SELECT * FROM {view_name}"
        if platform == 'sqlserver':
            self.sql_cursor.execute(query)
            rows = self.sql_cursor.fetchall()
            cols = [col[0] for col in self.sql_cursor.description] if self.sql_cursor.description else []
            return pd.DataFrame.from_records(rows, columns=cols)

        elif platform == 'snowflake':
            # re-set context in case it changed
            self.sf_cursor.execute("USE DATABASE MY_DB")
            self.sf_cursor.execute("USE SCHEMA MY_SCHEMA")
            self.sf_cursor.execute(query)
            rows = self.sf_cursor.fetchall()
            cols = [col[0] for col in self.sf_cursor.description] if self.sf_cursor.description else []
            return pd.DataFrame.from_records(rows, columns=cols)


    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Sort both columns and rows for deterministic equality."""
        df_sorted_cols = df.sort_index(axis=1)
        df_sorted = (
            df_sorted_cols
            .sort_values(by=list(df_sorted_cols.columns), na_position='last')
            .reset_index(drop=True)
        )
        return df_sorted


    @staticmethod
    def normalize_name(full_name: str) -> str:
        """
        Strip schema (and any common prefixes) to get the base view name.
        E.g. 'dbo.CustomerView' or 'MY_SCHEMA.CUSTOMER_VIEW' → 'customer_view'
        """
        base = full_name.split('.')[-1]
        # Remove any platform prefixes if you have conventions
        for pfx in ('SFX_', 'MYSQL_', 'SQLSERVER_'):
            if base.upper().startswith(pfx):
                base = base[len(pfx):]
                break
        return base.lower()


    def compare_all_views(self):
        # 1) Load view lists
        sql_views = self.get_sqlserver_view_names()
        sf_views  = self.get_snowflake_view_names()

        print(f"🔍 Found {len(sql_views)} SQL Server views and {len(sf_views)} Snowflake views.\n")

        # 2) Build normalized lookup
        sql_map = {self.normalize_name(v): v for v in sql_views}
        sf_map  = {self.normalize_name(v): v for v in sf_views}

        common = set(sql_map).intersection(sf_map)
        print(f"🔍 {len(common)} common views to compare:\n   {common}\n")

        comparer = Compare()

        for name in common:
            try:
                sql_full = sql_map[name]
                sf_full  = sf_map[name]

                df_sql = self.fetch_view_data(sql_full,  'sqlserver')
                df_sf  = self.fetch_view_data(sf_full,   'snowflake')

                # normalize & compare
                df_sql_n = self.normalize_dataframe(df_sql)
                df_sf_n  = self.normalize_dataframe(df_sf)

                comparer.compare_results(
                 df_sf_n, df_sql_n, snowflake_name=sf_full, sqlserver_name=sql_full,entity_type='view'
                )

                # if df_sql_n.equals(df_sf_n):
                #     print(f"✅ View `{sf_full}` matches `{sql_full}`")
                # else:
                #     print(f"❌ View `{sf_full}` and `{sql_full}` differ")
                #     diff = pd.concat([df_sf_n, df_sql_n]).drop_duplicates(keep=False)
                #     print(diff)

            except Exception as e:
                print(f"⚠️ Error comparing view {name}: {e}")

        print("\n🔎 View comparison complete.")


if __name__ == "__main__":
    vc = ViewComparer()
    vc.compare_all_views()
