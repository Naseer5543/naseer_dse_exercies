import pytest
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

@pytest.fixture
def db_connection():
    # Connect to the test database
    conn = sqlite3.connect(':memory:')  # Use an in-memory database for testing
    yield conn
    # Close the connection after the tests
    conn.close()
    
@pytest.fixture
def create_test_tables(db_connection):
    cursor = db_connection.cursor()
    
    # Create the "author" table
    cursor.execute("""
        CREATE TABLE author(
            A_ID INTEGER PRIMARY KEY,
            Name VARCHAR(100)
        )
    """)
    
    # Create the "books" table with foreign key constraint
    cursor.execute("""
        CREATE TABLE books(
            B_ID INTEGER PRIMARY KEY,
            Name VARCHAR(100),
            Price INTEGER NOT NULL,
            A_ID INTEGER,
            FOREIGN KEY (A_ID) REFERENCES author(A_ID)
        )
    """)

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
    
    db_connection.commit()
    
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

    #writing author table data to parquet file
    pq.write_table(pa.Table.from_pandas(author_df), 'author.parquet')
    
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
    
    #writing books table data to parquet file
    pq.write_table(pa.Table.from_pandas(books_df), 'books.parquet')

    


@pytest.fixture
def dummy(db_connection, create_test_tables):
    pass

def test_author_table_has_data(db_connection, create_test_tables):
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM author")
    assert cursor.fetchone()[0] == 10  # Check if there are 2 authors in the table

def test_books_table_has_data(db_connection, create_test_tables):
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM books")
    assert cursor.fetchone()[0] == 10  # Check if there are 2 books in the table

def test_books_table_foreign_key_constraint(db_connection, create_test_tables):
    cursor = db_connection.cursor()
    with pytest.raises(sqlite3.IntegrityError):
        # Try inserting a book with an invalid A_ID
        cursor.execute("INSERT INTO books (B_ID,Name, Price, A_ID) VALUES (?,?, ?, ?)", (105,'Book 3', 25, 3))
        db_connection.commit()

def test_books_table_data_types(db_connection, create_test_tables):
    cursor = db_connection.cursor()
    cursor.execute("SELECT Price FROM books WHERE B_ID = 103")
    assert isinstance(cursor.fetchone()[0], int)  # Check if the Price is of integer data type
