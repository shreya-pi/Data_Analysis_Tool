from compare import Compare
from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG
import pandas as pd
import pyodbc
import snowflake.connector

class TableComparer:
    def __init__(self):
        self.snowflake_config = SNOWFLAKE_CONFIG
        self.sql_server_config = SQL_SERVER_CONFIG
    
        self.sf_conn = snowflake.connector.connect(**self.snowflake_config)
        self.sf_cursor = self.sf_conn.cursor()
        self.sf_cursor.execute("USE DATABASE MY_DB")
        self.sf_cursor.execute("USE SCHEMA MY_SCHEMA")


        sql_conn_str = f"DRIVER={self.sql_server_config['driver']};SERVER={self.sql_server_config['server']};DATABASE={self.sql_server_config['database']};UID={self.sql_server_config['username']};PWD={self.sql_server_config['password']}"
        self.sql_conn = pyodbc.connect(sql_conn_str)
        self.sql_cursor = self.sql_conn.cursor()



    def get_snowflake_table_names(self):
        query = """
                SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE='BASE TABLE'
            """
        self.sf_cursor.execute(query)
        return [row[0] for row in self.sf_cursor.fetchall()]
    

    def get_sqlserver_table_names(self):
        query = """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE='BASE TABLE'
        """
        self.sql_cursor.execute(query)
        return [row[0] for row in self.sql_cursor.fetchall()]
    

    def fetch_table_data(self, table_name, platform):        
        query = f"SELECT * FROM {table_name}"

        if platform == 'sqlserver':
            self.sql_cursor.execute(query)
            result = self.sql_cursor.fetchall()
            columns = [desc[0] for desc in self.sql_cursor.description] if self.sql_cursor.description else []
            df = pd.DataFrame.from_records(result, columns=columns)
            return df
        elif platform == 'snowflake':
            self.sf_cursor.execute("USE DATABASE MY_DB")
            self.sf_cursor.execute("USE SCHEMA MY_SCHEMA")
            self.sf_cursor.execute(query)
            result = self.sf_cursor.fetchall()
            columns = [desc[0] for desc in self.sf_cursor.description] if self.sf_cursor.description else []
            df = pd.DataFrame.from_records(result, columns=columns)
            return df




    def normalize_dataframe(self, df):
        # Sort columns and rows for consistent comparison
        df = df.sort_index(axis=1)
        df = df.sort_values(by=df.columns.tolist(), na_position='last').reset_index(drop=True)
        return df



    def compare_results(self, df1, df2, snowflake_name, sqlserver_name):
        df1 = self.normalize_dataframe(df1)
        df2 = self.normalize_dataframe(df2)

        if df1.equals(df2):
            print(f"✅ Table `{snowflake_name}` matches `{sqlserver_name}`")
        else:
            print(f"❌ Table `{snowflake_name}` and `{sqlserver_name}` differ")
            # Optional: print differences
            diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
            print("Differences:")
            print(diff)

    def normalize_name(self, full_name: str, platform: str) -> str:
        """
        Given a fully-qualified table name like 'dbo.customer' or
        'MY_SCHEMA.MYSQL_CUSTOMER', strip schema + platform prefixes
        and return e.g. 'customer'.
        """
        # 1. take the part after the last dot
        raw = full_name.split('.')[-1]
        # 2. strip known prefixes
        for pfx in ('MYSQL_', 'SQLSERVER_'):
            if raw.upper().startswith(pfx):
                raw = raw[len(pfx):]
                break
        # 3. lowercase
        return raw.lower()





    def compare_all_tables(self):

        sqlserver_tables = self.get_sqlserver_table_names()
        snowflake_tables = self.get_snowflake_table_names()
        print(f"🔍 Found {len(sqlserver_tables)} SQL Server tables and {len(snowflake_tables)} Snowflake tables.\n")
        # print(f"SQL Server Tables: {sqlserver_tables}\n \n")
        # print(f"Snowflake Tables: {snowflake_tables}\n")

                # build maps: normalized_name → original_full_name
        sql_map = {self.normalize_name(t,'sqlserver'): t for t in sqlserver_tables }
        sf_map  = {self.normalize_name(t,'snowflake'):  t for t in snowflake_tables  }

        # print(f"Normalized SQL Server Tables: {sql_map}\n")
        # print(f"Normalized Snowflake Tables: {sf_map}\n")

                # only compare those normalized names present in both platforms
        common_tables = set(sql_map).intersection(sf_map)
        # print(f"Common Tables: {common_tables}\n")

        print(f"🔍 Found {len(common_tables)} common tables to compare.\n")

        comparer = Compare()

        for table in common_tables:
            try:
                original_sql_name = sql_map[table]
                original_sf_name = sf_map[table]
        
                df_sql = self.fetch_table_data(original_sql_name, 'sqlserver')
                df_sf = self.fetch_table_data(original_sf_name, 'snowflake')

                comparer.compare_results(
                 df_sf, df_sql, snowflake_name=original_sf_name, sqlserver_name=original_sql_name,entity_type='table'
                )

                # self.compare_results(df_sf, df_sql, snowflake_name=original_sf_name, sqlserver_name=original_sql_name)

            except Exception as e:
                print(f"⚠️ Error comparing table {table}: {e}")
        return 


if __name__ == "__main__":
    comparer = TableComparer()
    comparer.compare_all_tables()