import sqlite3

def create_database(filename):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

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

    # Create Publishers table
    cursor.execute('''
        CREATE TABLE Publishers (
            ID INTEGER PRIMARY KEY,
            Name TEXT NOT NULL
        )
    ''')

    # Create GenresCategories table
    cursor.execute('''
        CREATE TABLE GenresCategories (
            ID INTEGER PRIMARY KEY,
            Name TEXT
        )
    ''')

    # Create Conditions table
    cursor.execute('''
        CREATE TABLE Conditions (
            ID INTEGER PRIMARY KEY,
            Name TEXT,
            Description TEXT
        )
    ''')

    # Create Locations table
    cursor.execute('''
        CREATE TABLE Locations (
            ID INTEGER PRIMARY KEY,
            Name TEXT,
            Description TEXT
        )
    ''')

    # Create Books table
    cursor.execute('''
        CREATE TABLE Books (
            ID              INTEGER PRIMARY KEY,
            Title           TEXT    NOT NULL,
            AuthorID        INTEGER NOT NULL,
            PublisherID     INTEGER NOT NULL,
            GenreID         INTEGER NOT NULL,
            YearPublished   INTEGER,
            Edition         TEXT,
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

    connection.commit()
    connection.close()

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