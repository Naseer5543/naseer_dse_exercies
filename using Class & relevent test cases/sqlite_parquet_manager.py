import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class SQLiteParquetManager:
    def __init__(self, db_file):
        self.db_file = db_file

    def create_table(self, table_name, columns):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Create the table using the provided columns
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        cursor.execute(create_table_query)

        conn.commit()
        conn.close()

    def insert_data(self, table_name, data):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Insert data into the table
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(data))})"
        cursor.execute(insert_query, data)

        conn.commit()
        conn.close()

    def retrieve_data_save_parquet(self, table_name,output_file):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Retrieve all data from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()

        
        
        # Assuming the data contains column names (replace with actual column names if needed)
        column_names = [description[0] for description in cursor.description]
        df = pd.DataFrame(data, columns=column_names)
        print(df.head(10))

        # Save the DataFrame to a Parquet file
        pq.write_table(pa.Table.from_pandas(df), output_file)
        if df.empty is False:
            print('data succesfully written to parquet format')
        conn.close()