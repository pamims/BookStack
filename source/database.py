import sqlite3
from typing import Callable, Union, Any
from functools import wraps

# Connection Handling Decorator

def db_connection(
        query_func: Callable[[sqlite3.Cursor, tuple[Any, ...]], Any]
) -> Callable[[str, tuple[Any, ...]], Any]:
    @wraps(query_func)
    def wrapper(db_path: str, *args: tuple[Any, ...]) -> Any:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        try:
            result = query_func(cursor, *args)
        except sqlite3.Error as error:
            connection.rollback()
            raise error
        finally:
            connection.commit()
            cursor.close()
            connection.close()

        return result
    return wrapper

# Table Creation Functions

@db_connection
def create_table_title(cursor: sqlite3.Cursor) -> None:
    """Create title table query."""
    cursor.execute(
        '''
        CREATE TABLE Title (
            ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL
        )
        '''
    )

@db_connection
def create_table_genre(cursor: sqlite3.Cursor) -> None:
    """Create genre table query."""
    cursor.execute(
        '''
        CREATE TABLE Genre (
            ID INTEGER PRIMARY KEY,
            Name TEXT UNIQUE NOT NULL
        )
        '''
    )

@db_connection
def create_table_format(cursor: sqlite3.Cursor) -> None:
    """Create format table query."""
    cursor.execute(
        '''
        CREATE TABLE Format (
            ID INTEGER PRIMARY KEY,
            Name TEXT UNIQUE NOT NULL
        )
        '''
    )

@db_connection
def create_table_publisher(cursor: sqlite3.Cursor) -> None:
    """Create publisher table query."""
    cursor.execute(
        '''
        CREATE TABLE Publisher (
            ID INTEGER PRIMARY KEY,
            Name TEXT UNIQUE NOT NULL
        )
        '''
    )

@db_connection
def create_table_condition(cursor: sqlite3.Cursor) -> None:
    """Create publisher table query."""
    cursor.execute(
        '''
        CREATE TABLE Condition (
            ID INTEGER PRIMARY KEY,
            Name TEXT UNIQUE NOT NULL,
            Description TEXT UNIQUE NOT NULL
        )
        '''
    )

@db_connection
def create_table_author(cursor: sqlite3.Cursor) -> None:
    """Create author table query."""
    cursor.execute(
        '''
        CREATE TABLE Author (
            ID INTEGER PRIMARY KEY,
            Prefix TEXT NULLABLE,
            First TEXT NOT NULL,
            Middle TEXT NULLABLE,
            Last TEXT NULLABLE,
            Suffix TEXT NULLABLE
        )
        '''
    )


# Insertion Functions

@db_connection
def insert_title(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into title table."""
    cursor.execute(
        '''
        INSERT INTO Title (Name)
        VALUES (?)
        ''', args
    )

@db_connection
def insert_genre(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into genre table."""
    cursor.execute(
        '''
        INSERT INTO Genre (Name)
        VALUES (?)
        ''', args
    )

@db_connection
def insert_format(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into format table."""
    cursor.execute(
        '''
        INSERT INTO Format (Name)
        VALUES (?)
        ''', args
    )

@db_connection
def insert_publisher(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into format table."""
    cursor.execute(
        '''
        INSERT INTO Publisher (Name)
        VALUES (?)
        ''', args
    )

@db_connection
def insert_condition(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into condition table."""
    cursor.execute(
        '''
        INSERT INTO Condition (Name, Description)
        VALUES (?, ?)
        ''', args
    )

@db_connection
def insert_author(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into author table."""
    cursor.execute(
        '''
        INSERT INTO Author (Prefix, First, Middle, Last, Suffix)
        VALUES (?, ?, ?, ?, ?)
        ''', args
    )

# @db_connection
# def create_table_authors(cursor: sqlite3.Cursor) -> None:
#     # Create Authors table
#     cursor.execute('''
#         CREATE TABLE Authors (
#             ID INTEGER PRIMARY KEY,
#             FirstName TEXT NOT NULL,
#             MiddleName TEXT,
#             LastName TEXT NOT NULL,
#             Suffix TEXT
#         )
#     ''')

# @db_connection
# def create_table_publishers(cursor: sqlite3.Cursor):
#     # Create Publishers table
#     cursor.execute('''
#         CREATE TABLE Publishers (
#             ID INTEGER PRIMARY KEY,
#             Name TEXT UNIQUE NOT NULL
#         )
#     ''')

# @db_connection
# def create_table_genrescategories(cursor: sqlite3.Cursor):
#     # Create GenresCategories table
#     cursor.execute('''
#         CREATE TABLE GenresCategories (
#             ID INTEGER PRIMARY KEY,
#             Name TEXT UNIQUE NOT NULL
#         )
#     ''')

# @db_connection
# def create_table_conditions(cursor: sqlite3.Cursor):
#     # Create Conditions table
#     cursor.execute('''
#         CREATE TABLE Conditions (
#             ID INTEGER PRIMARY KEY,
#             Name TEXT UNIQUE NOT NULL,
#             Description TEXT UNIQUE
#         )
#     ''')

# @db_connection
# def create_table_locations(cursor: sqlite3.Cursor):
#     # Create Locations table
#     cursor.execute('''
#         CREATE TABLE Locations (
#             ID INTEGER PRIMARY KEY,
#             Name TEXT UNIQUE NOT NULL,
#             Description TEXT UNIQUE
#         )
#     ''')

# @db_connection
# def create_table_books(cursor: sqlite3.Cursor):
#     # Create Books table
#     cursor.execute('''
#         CREATE TABLE Books (
#             ID              INTEGER PRIMARY KEY,
#             Title           TEXT    NOT NULL,
#             AuthorID        INTEGER NOT NULL,
#             PublisherID     INTEGER NOT NULL,
#             GenreID         INTEGER NOT NULL,
#             YearPublished   INTEGER,
#             Edition         INTEGER,
#             ConditionID     INTEGER NOT NULL,
#             Description     TEXT,
#             DateAcquired    TEXT,
#             Price           REAL,
#             LocationID      INTEGER NOT NULL,
#             ISBN            TEXT,
#             FOREIGN KEY (AuthorID) REFERENCES Authors(ID),
#             FOREIGN KEY (PublisherID) REFERENCES Publishers(ID),
#             FOREIGN KEY (GenreID) REFERENCES GenresCategories(ID),
#             FOREIGN KEY (ConditionID) REFERENCES Conditions(ID),
#             FOREIGN KEY (LocationID) REFERENCES Locations(ID)
#         )
#     ''')

# @db_connection
# def create_database(cursor: sqlite3.Cursor):

#     def call_unwrapped(func: Callable):
#         func.__wrapped__(cursor)

#     call_unwrapped(create_table_authors)
#     call_unwrapped(create_table_publishers)
#     call_unwrapped(create_table_genrescategories)
#     call_unwrapped(create_table_conditions)
#     call_unwrapped(create_table_locations)
#     call_unwrapped(create_table_books)


# def add_author(filename: str, first: str, middle: str, last: str, suffix: str):
#     params = (first, middle, last, suffix)
#     query = """
#             INSERT INTO Authors (FirstName, MiddleName, LastName, Suffix)
#             VALUES (?, ?, ?, ?)
#             """
#     __perform_query(filename, query, params)


# def add_publisher(filename: str, name: str):
#     params = (name, )
#     query = """
#             INSERT INTO Publishers (Name)
#             VALUES (?)
#             """
#     __perform_query(filename, query, params)


# def add_genrecategory(filename: str, name: str):
#     params = (name, )
#     query = """
#             INSERT INTO GenresCategories (Name)
#             VALUES (?)
#             """
#     __perform_query(filename, query, params)

# def add_condition(filename: str, name: str, description: str):
#     params = (name, description)
#     query = """
#             INSERT INTO Conditions (Name, Description)
#             VALUES (?, ?)
#             """
#     __perform_query(filename, query, params)

# def add_location(filename: str, name: str, description: str):
#     params = (name, description)
#     query = """
#             INSERT INTO Locations (Name, Description)
#             VALUES (?, ?)
#             """
#     __perform_query(filename, query, params)

# def add_book(
#         filename: str, title: str, authorid: int, publisherid: int, genreid: int,
#         yearpublished: str, edition: int, conditionid: int, description: str,
#         dateacquired: str, price: float, locationid: int, isbn: str
#         ):
#     params = (
#         title, authorid, publisherid,
#         genreid, yearpublished, edition,
#         conditionid, description, dateacquired,
#         price, locationid, isbn
#     )
#     query = """
#             INSERT INTO Books (
#                 Title, AuthorID, PublisherID,
#                 GenreID, YearPublished, Edition,
#                 ConditionID, Description, DateAcquired,
#                 Price, LocationID, ISBN
#             )
#             VALUES (
#                 ?, ?, ?, ?, ?, ?,
#                 ?, ?, ?, ?, ?, ?
#             )
#             """
#     __perform_query(filename, query, params)

# # Helpers
# def __perform_query(filename: str, query: str, params: tuple):
#     connection = sqlite3.connect(filename)
#     cursor = connection.cursor()

#     try:
#         cursor.execute(query, params)
#     except sqlite3.Error as error:
#         connection.rollback()
#         raise error
#     finally:
#         connection.commit()
#         connection.close()