import sqlite3
from typing import Callable, Any
from functools import wraps

def db_connection(query_func: Callable[[sqlite3.Cursor], Any]) -> Callable[[str], Any]:
    @wraps(query_func)
    def wrapper(filename: str) -> Any:
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()

        try:
            result = query_func(cursor)
        except sqlite3.Error as error:
            connection.rollback()
            raise error
        finally:
            connection.commit()
            cursor.close()
            connection.close()

        return result
    return wrapper

@db_connection
def create_table_authors(cursor: sqlite3.Cursor) -> None:
    # Create Authors table
    cursor.execute('''
        CREATE TABLE Authors (
            ID INTEGER PRIMARY KEY,
            FirstName TEXT NOT NULL,
            MiddleName TEXT,
            LastName TEXT NOT NULL,
            Suffix TEXT
        )
    ''')

@db_connection
def create_table_publishers(cursor: sqlite3.Cursor):
    # Create Publishers table
    cursor.execute('''
        CREATE TABLE Publishers (
            ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL
        )
    ''')

@db_connection
def create_table_genrescategories(cursor: sqlite3.Cursor):
    # Create GenresCategories table
    cursor.execute('''
        CREATE TABLE GenresCategories (
            ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL
        )
    ''')

@db_connection
def create_table_conditions(cursor: sqlite3.Cursor):
    # Create Conditions table
    cursor.execute('''
        CREATE TABLE Conditions (
            ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL,
            Description TEXT
        )
    ''')

@db_connection
def create_table_locations(cursor: sqlite3.Cursor):
    # Create Locations table
    cursor.execute('''
        CREATE TABLE Locations (
            ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL,
            Description TEXT
        )
    ''')

@db_connection
def create_table_books(cursor: sqlite3.Cursor):
    # Create Books table
    cursor.execute('''
        CREATE TABLE Books (
            ID              INTEGER PRIMARY KEY,
            Title           TEXT    NOT NULL,
            AuthorID        INTEGER NOT NULL,
            PublisherID     INTEGER NOT NULL,
            GenreID         INTEGER NOT NULL,
            YearPublished   INTEGER,
            Edition         INTEGER,
            ConditionID     INTEGER NOT NULL,
            Description     TEXT,
            DateAcquired    TEXT,
            Price           REAL,
            LocationID      INTEGER NOT NULL,
            ISBN            TEXT,
            FOREIGN KEY (AuthorID) REFERENCES Authors(ID),
            FOREIGN KEY (PublisherID) REFERENCES Publishers(ID),
            FOREIGN KEY (GenreID) REFERENCES GenresCategories(ID),
            FOREIGN KEY (ConditionID) REFERENCES Conditions(ID),
            FOREIGN KEY (LocationID) REFERENCES Locations(ID)
        )
    ''')

@db_connection
def create_database(cursor: sqlite3.Cursor):

    def call_unwrapped(func: Callable):
        func.__wrapped__(cursor)

    call_unwrapped(create_table_authors)
    call_unwrapped(create_table_publishers)
    call_unwrapped(create_table_genrescategories)
    call_unwrapped(create_table_conditions)
    call_unwrapped(create_table_locations)
    call_unwrapped(create_table_books)


def add_author(filename: str, first: str, middle: str, last: str, suffix: str):
    params = (first, middle, last, suffix)
    query = """
            INSERT INTO Authors (FirstName, MiddleName, LastName, Suffix)
            VALUES (?, ?, ?, ?)
            """
    __perform_query(filename, query, params)


def add_publisher(filename: str, name: str):
    params = (name, )
    query = """
            INSERT INTO Publishers (Name)
            VALUES (?)
            """
    __perform_query(filename, query, params)


def add_genrecategory(filename: str, name: str):
    params = (name, )
    query = """
            INSERT INTO GenresCategories (Name)
            VALUES (?)
            """
    __perform_query(filename, query, params)

def add_condition(filename: str, name: str, description: str):
    params = (name, description)
    query = """
            INSERT INTO Conditions (Name, Description)
            VALUES (?, ?)
            """
    __perform_query(filename, query, params)

def add_location(filename: str, name: str, description: str):
    params = (name, description)
    query = """
            INSERT INTO Locations (Name, Description)
            VALUES (?, ?)
            """
    __perform_query(filename, query, params)

def add_book(
        filename: str, title: str, authorid: int, publisherid: int, genreid: int,
        yearpublished: str, edition: int, conditionid: int, description: str,
        dateacquired: str, price: float, locationid: int, isbn: str
        ):
    params = (
        title, authorid, publisherid,
        genreid, yearpublished, edition,
        conditionid, description, dateacquired,
        price, locationid, isbn
    )
    query = """
            INSERT INTO Books (
                Title, AuthorID, PublisherID,
                GenreID, YearPublished, Edition,
                ConditionID, Description, DateAcquired,
                Price, LocationID, ISBN
            )
            VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )
            """
    __perform_query(filename, query, params)

# Helpers
def __perform_query(filename: str, query: str, params: tuple):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    try:
        cursor.execute(query, params)
    except sqlite3.Error as error:
        connection.rollback()
        raise error
    finally:
        connection.commit()
        connection.close()