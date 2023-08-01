import os
import sqlite3
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlite_parquet_manager import SQLiteParquetManager

# Fixture to provide a temporary database for testing
@pytest.fixture
def temp_db_file(tmpdir):
    db_file = os.path.join(tmpdir, 'test.db')
    yield db_file
    if os.path.exists(db_file):
        os.remove(db_file)

# Fixture to provide the SQLiteParquetManager instance for each test
@pytest.fixture
def manager(temp_db_file):
    return SQLiteParquetManager(temp_db_file)

# Test case for create_table method
def test_create_table(manager, temp_db_file):
    # Table 1
    table_name1 = "author"
    columns1 = ["A_ID int NOT NULL PRIMARY KEY", "Name varchar(100)"]

    # Create the table
    manager.create_table(table_name1, columns1)

    # Check if the table was created in the database
    conn = sqlite3.connect(temp_db_file)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name1}'")
    result = cursor.fetchone()
    print(result)
    conn.close()
    assert result[0] == table_name1

    # Table 2
    table_name2 = "books"
    columns2 = ["B_ID int NOT NULL PRIMARY KEY", "Name varchar(100)", "Price int NOT NULL", "A_ID int"]

    # Create the table
    manager.create_table(table_name2, columns2)

    # Check if the table was created in the database
    conn = sqlite3.connect(temp_db_file)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name2}'")
    result = cursor.fetchone()
    conn.close()

    assert result[0] == table_name2

# Test case for insert_data method
def test_insert_data(manager, temp_db_file):
    # Table 1
    table_name1 = "author"
    columns1 = ["A_ID int NOT NULL PRIMARY KEY", "Name varchar(100)"]
    manager.create_table(table_name1, columns1)

    # Insert data into the table
    data1 = (1, "Author 1")
    data2 = (2, "Author 2")
    manager.insert_data(table_name1, data1)
    manager.insert_data(table_name1, data2)

    # Retrieve data and check if the inserted data is present in the table
    conn = sqlite3.connect(temp_db_file)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name1}")
    retrieved_data = cursor.fetchall()
    conn.close()

    assert data1 in retrieved_data
    assert data2 in retrieved_data

    # Table 2
    table_name2 = "books"
    columns2 = ["B_ID int NOT NULL PRIMARY KEY", "Name varchar(100)", "Price int NOT NULL", "A_ID int"]
    manager.create_table(table_name2, columns2)

    # Insert data into the table
    data3 = (101, "Book 1", 50, 1)
    data4 = (102, "Book 2", 30, 2)
    manager.insert_data(table_name2, data3)
    manager.insert_data(table_name2, data4)

    # Retrieve data and check if the inserted data is present in the table
    conn = sqlite3.connect(temp_db_file)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name2}")
    retrieved_data = cursor.fetchall()
    conn.close()

    assert data3 in retrieved_data
    assert data4 in retrieved_data

# Test case for retrieve_data_save_parquet method
def test_retrieve_data_save_parquet(manager, temp_db_file, tmpdir):
    # Table 1
    table_name1 = "author"
    columns1 = ["A_ID int NOT NULL PRIMARY KEY", "Name varchar(100)"]
    manager.create_table(table_name1, columns1)

    # Insert data into the table
    data1 = (1, "Author 1")
    data2 = (2, "Author 2")
    manager.insert_data(table_name1, data1)
    manager.insert_data(table_name1, data2)

    # Table 2
    table_name2 = "books"
    columns2 = ["B_ID int NOT NULL PRIMARY KEY", "Name varchar(100)", "Price int NOT NULL", "A_ID int"]
    manager.create_table(table_name2, columns2)

    # Insert data into the table
    data3 = (101, "Book 1", 50, 1)
    data4 = (102, "Book 2", 30, 2)
    manager.insert_data(table_name2, data3)
    manager.insert_data(table_name2, data4)

    # Retrieve data and save as Parquet
    output_parquet_file = os.path.join(tmpdir, 'output_data.parquet')
    manager.retrieve_data_save_parquet(table_name1, output_parquet_file)

    # Read the saved Parquet file
    table = pq.read_table(output_parquet_file)
    df = table.to_pandas()

    # Check if the saved data matches the inserted data
    assert len(df) == 2
    assert df.loc[0].tolist() == list(data1)
    assert df.loc[1].tolist() == list(data2)
