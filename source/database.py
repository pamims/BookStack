"""Functions for working with project database."""
import sqlite3
from typing import Callable, Union, Any
from functools import wraps

# Connection Handling Decorator


def db_connection(
        query_func: Callable[[sqlite3.Cursor, tuple[Any, ...]], Any]
) -> Callable[[str, tuple[Any, ...]], Any]:
    """Wraps query functions with connection/commit to database file."""
    @wraps(query_func)
    def wrapper(db_path: str, *args: tuple[Any, ...]) -> Any:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")

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


@db_connection
def create_table_titleauthor(cursor: sqlite3.Cursor) -> None:
    """Create author table query."""
    cursor.execute(
        '''
        CREATE TABLE TitleAuthor (
            ID INTEGER PRIMARY KEY,
            TitleID INTEGER,
            AuthorID INTEGER,
            UNIQUE (TitleID, AuthorID),
            FOREIGN KEY (TitleID) REFERENCES Title(ID),
            FOREIGN KEY (AuthorID) REFERENCES Author(ID)
        )
        '''
    )


@db_connection
def create_table_work(cursor: sqlite3.Cursor) -> None:
    """Create work table query."""
    cursor.execute(
        '''
        CREATE TABLE Work (
            ID INTEGER PRIMARY KEY,
            TitleAuthorID INTEGER,
            GenreID INTEGER,
            UNIQUE (TitleAuthorID, GenreID),
            FOREIGN KEY (TitleAuthorID) REFERENCES TitleAuthor(ID),
            FOREIGN KEY (GenreID) REFERENCES Genre(ID)
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


@db_connection
def insert_titleauthor(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into author table."""
    cursor.execute(
        '''
        INSERT INTO TitleAuthor (TitleID, AuthorID)
        VALUES (?, ?)
        ''', args
    )


@db_connection
def insert_work(
    cursor: sqlite3.Cursor, *args: Union[str, int, float]
) -> None:
    """Insert record into work table."""
    cursor.execute(
        '''
        INSERT INTO Work (TitleAuthorID, GenreID)
        VALUES (?, ?)
        ''', args
    )
