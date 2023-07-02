import unittest
import os
import sqlite3
from source import database
from typing import Callable, Iterable, Optional


### BASE CLASSES ###
class BaseDatabaseModuleTestCase(unittest.TestCase):
    """Base test case for all database tests."""
    db_path = 'test_book_stack_database.db'
    db_required_tables = {
        'Authors': ['ID', 'FirstName', 'MiddleName', 'LastName', 'Suffix'],
        'Publishers': ['ID', 'Name'],
        'GenresCategories': ['ID', 'Name'],
        'Conditions': ['ID', 'Name', 'Description'],
        'Locations': ['ID', 'Name', 'Description'],
        'Books': [
            'ID', 'Title', 'AuthorID', 'PublisherID',
            'GenreID', 'YearPublished', 'Edition',
            'ConditionID', 'Description', 'DateAcquired',
            'Price', 'LocationID', 'ISBN'
        ]
    }

    # These are class members because I want to use them in setUpClass and
    # tearDownClass -- the connection only needs established once per testcase
    connection: sqlite3.Connection = None
    cursor: sqlite3.Cursor = None

    def tearDown(self) -> None:
        # Clean the database after each test
        self.removeAllDatabaseTables()

    @classmethod
    def setUpClass(cls) -> None:
        """Builds database, establishes connection, and sets cursor."""
        #print(f"\nsetUpClass() called: {cls.__name__}\n")
        cls.openDatabaseConnection()

    @classmethod
    def tearDownClass(cls) -> None:
        """Closes database connection and removes test database file."""
        #print(f"\ntearDownClass() called: {cls.__name__}\n")
        cls.closeDatabaseConnection()
        cls.removeDatabaseFile()

    @classmethod
    def openDatabaseConnection(cls) -> None:
        cls.connection = sqlite3.connect(cls.db_path)
        cls.cursor = cls.connection.cursor()

    @classmethod
    def closeDatabaseConnection(cls) -> None:
        if cls.connection is not None:
            cls.cursor.close()
            cls.connection.close()
            cls.connection = None
            cls.cursor = None

    @classmethod
    def removeDatabaseFile(cls) -> None:
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    @classmethod
    def removeAllDatabaseTables(cls) -> None:
        user_defined_tables = cls.getDatabaseTableNames()
        for table in user_defined_tables:
            query = f"DROP TABLE IF EXISTS {table}"
            cls.cursor.execute(query)
        cls.connection.commit()

    @classmethod
    def validateCursor(cls, msg: str = None) -> None:
        if cls.cursor is None:
            if msg is None:
                msg = "No database connection."
            raise cls.NoDatabaseConnectionError(msg)

    @classmethod
    def validateTableName(cls, table_name: str, db_schema: Iterable,
                          msg: str = None) -> None:
        if table_name not in db_schema:
            if msg is None:
                valid_list = ', '.join(list(db_schema))
                msg = (
                    f"Invalid table name: '{table_name}'"
                    f"Valid names include: {valid_list}"
                )
            raise cls.DatabaseTableError(msg)

    @classmethod
    def getDatabaseTableNames(cls) -> list[str]:
        """Get the table names from the database."""
        cls.validateCursor("Cannot get table names. No database connection.")
        cls.cursor.execute(
            '''
            SELECT name FROM sqlite_master WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            '''
        )
        result = cls.cursor.fetchall()
        table_names = [row[0] for row in result]
        return table_names

    @classmethod
    def getTableColumnNames(cls, table_name: str) -> list[str]:
        """Get the column names from a specific table in the database."""
        valid_list = ', '.join(list(cls.db_required_tables))
        cls.validateTableName(
            table_name, cls.db_required_tables,
            f"Cannot retrieve columns for '{table_name} table.\n"
            f"Valid names include: {valid_list}\n"
            f"Invalid table name: {table_name}\n"
        )
        table_names = cls.getDatabaseTableNames()
        if table_name not in table_names:
            return [] # no columns in table that doesn't exist
        cls.cursor.execute(f"PRAGMA table_info({table_name})")
        result = cls.cursor.fetchall()
        column_names = [row[1] for row in result]
        return column_names

    @classmethod
    def getFirstValidRecordID(cls, table_name: str):
        valid_list = ', '.join(list(cls.db_required_tables))
        cls.validateTableName(
            table_name, cls.db_required_tables,
            f"Cannot retrieve columns for '{table_name} table.\n"
            f"Valid names include: {valid_list}"
            f"Invalid table name: {table_name}\n"
        )
        cls.validateCursor("Could not get Record ID. No database connection.")
        cls.cursor.execute(f"SELECT ID FROM {table_name} ORDER BY ID LIMIT 1")
        result = cls.cursor.fetchone()
        if result is not None:
            result = int(result[0])
        return result

    class DatabaseError(Exception):
        def __init__(self, msg: str = None):
            super().__init__(msg)
        def __str__(self):
            msg = super().args[0]
            name = type(self).__name__
            msg = name + f": {msg}" if type(msg) is str else ""
            return msg

    class NoDatabaseConnectionError(DatabaseError):
        pass
    class DatabaseTableError(DatabaseError):
        pass

    def assertCorrectRecordInsertion(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_list: list[tuple[Optional[str]]]
    ) -> None:
        """Asserts correct table insertion, including ID AUTOINCREMENT."""
        self.validateTableName(table_name, self.db_required_tables)
        self.validateCursor()
        for params in params_list:
            args = (self.db_path, ) + params
            try:
                insert_row_func(*args)
            except Exception as e:
                self.fail(f"Error during '{table_name}' insertion: {str(e)}")

        self.cursor.execute(f"SELECT * FROM {table_name}")
        id = 0
        for params in params_list:
            id += 1 # sqlite database autoincrement id's start at 1
            expected_record = (id, ) + params
            record = self.cursor.fetchone()
            if record is None:
                self.fail(f"No record found in the '{table_name}' table.")
            num_fields = len(record)
            num_expected = len(expected_record)
            mismatch = set(record).symmetric_difference(set(expected_record))
            if num_fields != num_expected:
                self.fail(
                    f"Number of fields mismatch. Expected {num_expected}, "
                    f"Received {num_fields}. Mismatched items: {mismatch}"
                )
            if record != expected_record:
                self.fail(
                    f"Expected: {expected_record}, Received {record}. "
                    f"Mismatched items: {mismatch}"
                )

    def assertNotNullTableConstraints(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_list: list[tuple[Optional[str]]]
    ) -> None:
        """Asserts NOT NULL constraint detected when adding record."""
        # get the list of the column names
        try:
            column_names = self.getTableColumnNames(table_name)
        except self.DatabaseError as e: # could raise either one
            self.fail(f"Could not assert NOT NULL constraints: {str(e)}")
        for params in params_list:
            # make sure only one None is in the parameters
            assert params.count(None) == 1, (
                "To check NOT NULL constraints properly, every parameter "
                "list must contain None exactly once. The particular "
                "value being tested should be set to None."
            )
            # make sure the parameters were programmed in correctly
            assert len(params) + 1 == len(column_names), (
                "Number of parameters for the insertion call does not match "
                "the number of columns that are supposed to be in the table."
            )
            # get the name of the column being tested
            col_index = params.index(None) + 1 # accounts for ID column
            column_tested = column_names[col_index]
            # get the args to send to the insert_row_func
            args = (self.db_path, ) + params
            # generate the failure message
            msg = f"{table_name} {column_tested} NOT NULL constraint fails."
            with self.assertRaises(sqlite3.IntegrityError, msg = msg) as ctx:
                insert_row_func(*args)
            error_msg = str(ctx.exception)
            self.assertIn("NOT NULL constraint failed", error_msg, msg)

### TEST CASES ###

class TestCreateDatabaseModuleFunction(BaseDatabaseModuleTestCase):

    def test_create_database_function(self):
        database.create_database(self.db_path)
        existing_tables = self.getDatabaseTableNames()
        db_schema = {}
        for table_name in existing_tables:
            column_names = self.getTableColumnNames(table_name)
            db_schema[table_name] = column_names
        self.assertDictEqual(
            self.db_required_tables, db_schema,
            "\n\nFAILURE: Database schema is incorrect!"
        )

class TestAuthorsTable(BaseDatabaseModuleTestCase):
    table_name = 'Authors'

    def setUp(self):
        database.create_table_authors(self.db_path)

    def test_add_author_creates_valid_record(self):
        """Verifies add_author() creates a valid record."""
        self.assertCorrectRecordInsertion(
            self.table_name, database.add_author,
            [
                ('FIRST', 'M', 'LAST', 'SUFFIX'),
                ('first', 'm', 'last', 'suffix'),
                ('Bob', None, 'Anderson', None)
            ]
        )

    def test_author_table_not_null_constraints(self):
        """Test not null constraints in Authors table."""
        self.assertNotNullTableConstraints(
            self.table_name, database.add_author,
            [
                (None, 'MiddleName', 'LastName', 'Suffix'),
                ('FirstName', 'MiddleName', None, 'Suffix')
            ]
        )

class TestPublishersTable(BaseDatabaseModuleTestCase):
    table_name = 'Publishers'

    def setUp(self):
        database.create_table_publishers(self.db_path)

    def test_add_publisher_creates_valid_record(self):
        """Verifies add_publisher() creates a valid record."""
        self.assertCorrectRecordInsertion(
            self.table_name, database.add_publisher, [('NAME', ), ('name', )]
        )

    def test_publisher_table_not_null_constraints(self):
        """Test not null constraints in Publishers table."""
        self.assertNotNullTableConstraints(
            self.table_name, database.add_publisher, [(None, )]
        )

class TestGenresCategoriesTable(BaseDatabaseModuleTestCase):
    table_name = 'GenresCategories'

    def setUp(self):
        database.create_table_genrescategories(self.db_path)

    def test_add_genrecategory_creates_valid_record(self):
        """Verifies add_genrecategory() creates a valid record."""
        self.assertCorrectRecordInsertion(
            self.table_name, database.add_genrecategory,
            [('NAME', ), ('name', )]
        )

    def test_genrecategory_table_not_null_constraints(self):
        """Test not null constraints in GenresCategories table."""
        self.assertNotNullTableConstraints(
            self.table_name, database.add_genrecategory, [(None, )]
        )

class TestConditionsTable(BaseDatabaseModuleTestCase):
    table_name = 'Conditions'

    def setUp(self):
        database.create_table_conditions(self.db_path)

    def test_add_condition_creates_valid_record(self):
        """Verifies add_condition() creates a valid record."""
        self.assertCorrectRecordInsertion(
            self.table_name, database.add_condition,
            [
                ('NAME', 'DESCRIPTION'),
                ('name', None),
            ]
        )

    def test_condition_table_not_null_constraints(self):
        """Test not null constraints in Conditions table."""
        self.assertNotNullTableConstraints(
            self.table_name, database.add_condition, [(None, 'Hello')]
        )

class TestLocationsTable(BaseDatabaseModuleTestCase):
    table_name = 'Locations'

    def setUp(self):
        database.create_table_locations(self.db_path)

    def test_add_location_creates_valid_record(self):
        """Verifies add_location() creates a valid record."""
        self.assertCorrectRecordInsertion(
            self.table_name, database.add_location,
            [
                ('NAME', 'DESCRIPTION'),
                ('name', None)
            ]
        )

    def test_location_table_not_null_constraints(self):
        self.assertNotNullTableConstraints(
            self.table_name, database.add_location, [(None, 'Description')]
        )

class TestBooksTable(BaseDatabaseModuleTestCase):
    table_name = 'Books'

    def setUp(self):
        database.create_database(self.db_path)
        database.add_author(
            self.db_path, "AuthorFirst", "AuthorMiddle",
            "AuthorLast", "AuthorSuffix"
        )
        database.add_publisher(self.db_path, "PublisherName")
        database.add_genrecategory(self.db_path, "GenreCategoryName")
        database.add_condition(self.db_path, "ConditionName",
                               "ConditionDescription"
        )
        database.add_location(self.db_path, "LocationName",
                              "LocationDescription"
        )

    def test_add_book_creates_valid_record(self):
        """Verifies add_book() creates a valid record."""
        AuthorID = self.getFirstValidRecordID('Authors')
        PublisherID = self.getFirstValidRecordID('Publishers')
        GenreID = self.getFirstValidRecordID('GenresCategories')
        ConditionID = self.getFirstValidRecordID('Conditions')
        LocationID = self.getFirstValidRecordID('Locations')

        Title = 'Title'
        YearPublished = 2000
        Edition = 1
        DateAcquired = '2020-01-01'
        Description = 'Description'
        Price = 9.99
        isbn = 'ISBN'

        params = (
            Title, AuthorID, PublisherID,
            GenreID, YearPublished, Edition,
            ConditionID, Description, DateAcquired,
            Price, LocationID, isbn
        )
        self.assertCorrectRecordInsertion(
            self.table_name, database.add_book, [params, params]
        )

    def test_books_table_foreign_key_constraints(self):
        """Test foreign key constraints in Books table."""
        parameters = (
            "Title", 999, 999, 999, 2000, 1, 999,
            "Description", "2000-01-01", 9.99, 999, "ISBN"
        )
        # Turn foreign key functionality on
        self.cursor.execute("PRAGMA foreign_keys = ON")
        with self.assertRaises(sqlite3.IntegrityError) as context:
            self.cursor.execute(
                '''
                INSERT INTO Books (Title, AuthorID, PublisherID, GenreID,
                YearPublished, Edition, ConditionID, Description, DateAcquired,
                Price, LocationID, ISBN)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                parameters
            )
        error_message = str(context.exception)
        self.assertIn(
            "FOREIGN KEY constraint failed", error_message,
            "AuthorID, PublisherID, GenreID, ConditionID, and LocationID "
            "should have FOREIGN KEY constraints"
        )

    def test_book_table_not_null_constraints(self):

        AuthorID = self.getFirstValidRecordID('Authors')
        PublisherID = self.getFirstValidRecordID('Publishers')
        GenreID = self.getFirstValidRecordID('GenresCategories')
        ConditionID = self.getFirstValidRecordID('Conditions')
        LocationID = self.getFirstValidRecordID('Locations')

        YearPublished = 2000
        Edition = 1
        DateAcquired = '2020-01-01'
        Description = 'Description'
        Price = 9.99
        isbn = 'ISBN'

        self.assertNotNullTableConstraints(
            self.table_name, database.add_book,
            [
                (None, AuthorID, PublisherID,
                GenreID, YearPublished, Edition,
                ConditionID, Description, DateAcquired,
                Price, LocationID, isbn),
                ('Title', None, PublisherID,
                GenreID, YearPublished, Edition,
                ConditionID, Description, DateAcquired,
                Price, LocationID, isbn)
            ]
        )

### ENTRY POINT ###
if __name__ == '__main__':
    unittest.main()