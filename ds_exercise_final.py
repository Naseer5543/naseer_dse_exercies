import re
import pandas as pd
import random
import sqlite3
import pyarrow as pa
import pyarrow.parquet as pq

ddl_statement_1 = """
     CREATE TABLE author(
        A_ID int NOT NULL PRIMARY KEY,
        Name varchar(100)
    );
"""

ddl_statement_2 = """
 CREATE TABLE books(
        B_ID int NOT NULL PRIMARY KEY,
        Name varchar(100),
        Price int NOT NULL,
        A_ID int,
        FOREIGN KEY (A_ID) REFERENCES author(A_ID)
    );
    """
    
# Connect to the SQLite database/create one if not present
conn = sqlite3.connect('final_exercise.db')

# Create a cursor
cursor = conn.cursor()


# Execute the SQL query to select data from the table
cursor.execute(ddl_statement_1)
cursor.execute(ddl_statement_2)


authors_name =[(1, 'Jane Austen'), (2, 'J.K. Rowling'), (3, 'Leo Tolstoy'), (4, 'Mark Twain'), (5, 'Harper Lee'), (6, 'William Shakespeare'), (7, 'Agatha Christie'), (8, 'Stephen King'), (9, 'George Orwell'), (10, 'Charles Dickens')]

cursor.executemany("""
                   INSERT INTO author(A_ID,Name) VALUES(?,?)
                   """,authors_name)

books_data = [
    (101, 'Book 1', 25, 1),
    (102, 'Book 2', 30, 2),
    (103, 'Book 3', 20, 3),
    (104, 'Book 4', 15, 1),
    (105, 'Book 5', 28, 4),
    (106, 'Book 6', 22, 5),
    (107, 'Book 7', 35, 2),
    (108, 'Book 8', 18, 6),
    (109, 'Book 9', 40, 1),
    (110, 'Book 10', 27, 7),
]

cursor.executemany("""
                   INSERT INTO books(B_ID,Name,Price,A_ID) VALUES(?,?,?,?)
                   """,books_data)


query = """
        select  * from books;
"""

# Execute the SQL query to select data from the table
cursor.execute(query)

# Fetch all rows from the result
books = cursor.fetchall()

# Print the data
for row in books:
    print(row)
    
column_names = [description[0] for description in cursor.description]
books_df = pd.DataFrame(books, columns=column_names)
books_df.head()

pq.write_table(pa.Table.from_pandas(books_df), 'books.parquet')

query = """
        select  * from author;
"""

# Execute the SQL query to select data from the table
cursor.execute(query)

# Fetch all rows from the result
author = cursor.fetchall()

# Print the data
for row in author:
    print(row)
    
column_names = [description[0] for description in cursor.description]
author_df = pd.DataFrame(author, columns=column_names)
author_df.head()


pq.write_table(pa.Table.from_pandas(author_df), 'author.parquet')
