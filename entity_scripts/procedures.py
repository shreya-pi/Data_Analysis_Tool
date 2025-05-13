import json
import pandas as pd
import pyodbc
import snowflake.connector
from compare import Compare
from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG

class ProcedureComparer:
    def __init__(self):
        self.snowflake_config = SNOWFLAKE_CONFIG
        self.sql_server_config = SQL_SERVER_CONFIG
        self.json_path='./inputs/procedure_input.json'

        # Connect to Snowflake
        self.sf_conn = snowflake.connector.connect(**self.snowflake_config)
        self.sf_cursor = self.sf_conn.cursor()
        self.sf_cursor.execute("USE DATABASE MY_DB")
        self.sf_cursor.execute("USE SCHEMA MY_SCHEMA")

        # Connect to SQL Server
        sql_conn_str = (
            f"DRIVER={self.sql_server_config['driver']};"
            f"SERVER={self.sql_server_config['server']};"
            f"DATABASE={self.sql_server_config['database']};"
            f"UID={self.sql_server_config['username']};"
            f"PWD={self.sql_server_config['password']}"
        )
        self.sql_conn = pyodbc.connect(sql_conn_str)
        self.sql_cursor = self.sql_conn.cursor()

        # Load test cases
        with open(self.json_path, 'r') as f:
            self.procedure_tests = json.load(f)

    def fetch_procedure_result(self, query: str, platform: str) -> pd.DataFrame:
        if platform == 'sqlserver':
            self.sql_cursor.execute(query)
            result = self.sql_cursor.fetchall()
            columns = [desc[0] for desc in self.sql_cursor.description] if self.sql_cursor.description else []
            return pd.DataFrame.from_records(result, columns=columns)
        elif platform == 'snowflake':
            self.sf_cursor.execute("USE DATABASE MY_DB")
            self.sf_cursor.execute("USE SCHEMA MY_SCHEMA")
            self.sf_cursor.execute(query)
            result = self.sf_cursor.fetchall()
            columns = [desc[0] for desc in self.sf_cursor.description] if self.sf_cursor.description else []
            return pd.DataFrame.from_records(result, columns=columns)

    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_index(axis=1)
        df = df.sort_values(by=df.columns.tolist(), na_position='last').reset_index(drop=True)
        return df


# Not needed for procedures as they are provided in the JSON input
    # @staticmethod
    # def normalize_name(full_name: str) -> str:
    #     """
    #     Strip schema (and any common prefixes) to get the base view name.
    #     E.g. 'dbo.CustomerView' or 'MY_SCHEMA.CUSTOMER_VIEW' → 'customer_view'
    #     """
    #     base = full_name.split('.')[-1]
    #     # Remove any platform prefixes if you have conventions
    #     for pfx in ('SFX_', 'MYSQL_', 'SQLSERVER_'):
    #         if base.upper().startswith(pfx):
    #             base = base[len(pfx):]
    #             break
    #     return base.lower()



    def compare_all_procedures(self):
        comparer = Compare()
        print(f"🔍 Found {len(self.procedure_tests)} stored procedures to compare.\n")

        for proc_name, queries in self.procedure_tests.items():
            # try:
            sql_query = queries.get("sql_procedure_query")
            sf_query = queries.get("sf_procedure_query")

            if not sql_query or not sf_query:
                print(f"⚠️ Skipping `{proc_name}` due to missing queries.")
                continue

            df_sql = self.fetch_procedure_result(sql_query, 'sqlserver')
            df_sf = self.fetch_procedure_result(sf_query, 'snowflake')

            comparer.compare_results(
                df_sf, df_sql, snowflake_name=proc_name, sqlserver_name=proc_name, entity_type='procedure'
            )
                # self.compare_results(df_sf, df_sql, proc_name)
            # except Exception as e:
            #     print(f"⚠️ Error comparing procedure `{proc_name}`: {e}")

        print("\n🔍 Comparison completed.")

if __name__ == "__main__":
    comparer = ProcedureComparer()
    comparer.compare_all_procedures()
