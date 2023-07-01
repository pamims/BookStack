import unittest
import os
import sqlite3
from source import database
from typing import Callable, Iterable


### BASE CLASSES ###
class BaseDatabaseTestCase(unittest.TestCase):
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

    connection = None
    cursor = None

    def __init__(self, methodName: str = "runtest") -> None:
        super().__init__(methodName)

    @classmethod
    def openDatabaseConnection(cls) -> None:
        cls.connection = sqlite3.connect(cls.db_path)
        cls.cursor = cls.connection.cursor()
        if not os.path.exists(cls.db_path):
            raise FileNotFoundError(f"Database file '{cls.db_path}' not found.")

    @classmethod
    def closeDatabaseConnection(cls) -> None:
        if cls.connection is not None:
            cls.connection.close()
            cls.connection = None
            cls.cursor = None
        cls.removeDatabaseFile()

    @classmethod
    def removeDatabaseFile(cls):
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def removeAllDatabaseTables(self):
        user_defined_tables = self.getDatabaseTableNames()
        for table in user_defined_tables:
            query = f"DROP TABLE IF EXISTS {table}"
            self.cursor.execute(query)
        self.connection.commit()

    def validateCursor(self, msg: str = None) -> None:
        if self.cursor is None:
            if msg is None:
                msg = "No database connection."
            raise self.NoDatabaseConnectionError(msg)

    def validateTableName(self, table_name: str, db_schema: Iterable,
                          msg: str = None) -> None:
        if table_name not in db_schema:
            if msg is None:
                valid_list = ', '.join(list(db_schema))
                msg = (
                    f"Invalid table name: '{table_name}'"
                    f"Valid names include: {valid_list}"
                )
            raise self.DatabaseTableError(msg)

    def getDatabaseTableNames(self) -> list[str]:
        """Get the table names from the database."""
        self.validateCursor("Cannot get table names. No database connection.")
        self.cursor.execute(
            '''
            SELECT name FROM sqlite_master WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            '''
        )
        result = self.cursor.fetchall()
        table_names = [row[0] for row in result]
        return table_names

    def getTableColumnNames(self, table_name: str) -> list[str]:
        """Get the column names from a specific table in the database."""
        valid_list = ', '.join(list(self.db_required_tables))
        self.validateTableName(
            table_name, self.db_required_tables,
            f"Cannot retrieve columns for '{table_name} table.\n"
            f"Valid names include: {valid_list}\n"
            f"Invalid table name: {table_name}\n"
        )
        table_names = self.getDatabaseTableNames()
        if table_name not in table_names:
            return []
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        result = self.cursor.fetchall()
        column_names = [row[1] for row in result]
        return column_names

    def getFirstValidRecordID(self, table_name: str):
        valid_list = ', '.join(list(self.db_required_tables))
        self.validateTableName(
            table_name, self.db_required_tables,
            f"Cannot retrieve columns for '{table_name} table.\n"
            f"Valid names include: {valid_list}"
            f"Invalid table name: {table_name}\n"
        )
        self.validateCursor("Could not get Record ID. No database connection.")
        self.cursor.execute(f"SELECT ID FROM {table_name} ORDER BY ID LIMIT 1")
        result = self.cursor.fetchone()
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

    def assertFileExists(self, file_path: str, msg: str = None) -> None:
        if not os.path.exists(file_path):
            if msg is None:
                msg = f"File {file_path} does not exist."
            self.fail(msg)

    def assertCorrectRecordInsertion(
            self, insert_row_func: Callable,
            params_list: list[tuple], table_name: str
    ) -> None:
        """Asserts correct table insertion, including ID AUTOINCREMENT."""
        for params in params_list:
            args = (self.db_path, ) + params
            try:
                insert_row_func(*args)
            except Exception as e:
                self.fail(f"Error during '{table_name}' insertion: {str(e)}")

        self.validateTableName(table_name, self.db_required_tables)
        self.validateCursor()
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
            self, insert_row_func: Callable,
            params_list: list[tuple], table_name: str
    ) -> None:
        """Asserts NOT NULL constraint detected when adding record."""
        # get the list of the column names
        try:
            column_names = self.getTableColumnNames(table_name)
        except self.DatabaseError as e:
            self.fail(f"Could not assert NOT NULL constraints: {str(e)}")
        for params in params_list:
            # make sure only one None is in the parameters
            assert params.count(None), (
                "To check NOT NULL constraints properly, every parameter "
                "list must contain None exactly once. The particular "
                "value being tested should be set to None."
            )
            # get the name of the column being tested
            column_tested = column_names[params.index(None)]
            # get the args to send to the insert_row_func
            args = (self.db_path, ) + params
            # generate the failure message
            msg = f"{table_name} {column_tested} NOT NULL constraint fails."
            error_msg = None
            try:
                insert_row_func(*args)
            except sqlite3.IntegrityError as e:
                error_msg = str(e)
                if "NOT NULL constraint failed" not in error_msg:
                    self.fail(msg)
            if error_msg is None:
                self.fail(msg)


class SetUpDatabaseConnectionTestCase(BaseDatabaseTestCase):

    @classmethod
    def setUpClass(cls):
        """Builds database, establishes connection, and sets cursor."""
        # print(f"\nsetUpClass() called: {cls.__name__}\n")
        cls.openDatabaseConnection()

    @classmethod
    def tearDownClass(cls):
        """Closes database connection and removes test database file."""
        # print(f"\ntearDownClass() called: {cls.__name__}\n")
        cls.closeDatabaseConnection()

class SetUpDatabaseTableSchemaTestCase(SetUpDatabaseConnectionTestCase):

    def setUp(self):
        raise NotImplementedError("Create table(s) necessary for test case.")

    def tearDown(self):
        self.removeAllDatabaseTables()


### TEST CASES ###

class TestCreateDatabaseModuleFunction(SetUpDatabaseConnectionTestCase):

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

class TestAuthorsTable(SetUpDatabaseTableSchemaTestCase):
    table_name = 'Authors'

    def setUp(self):
        database.create_table_authors(self.db_path)

    def test_add_author_creates_valid_record(self):
        """Verifies add_author() creates a valid record."""
        self.assertCorrectRecordInsertion(
            database.add_author,
            [
                ('FIRST', 'M', 'LAST', 'SUFFIX'),
                ('first', 'm', 'last', 'suffix'),
                ('Bob', None, 'Anderson', None)
            ],
            self.table_name
        )

    def test_author_table_not_null_constraints(self):
        """Test not null constraints in Authors table."""
        self.assertNotNullTableConstraints(
            database.add_author,
            [
                (None, 'MiddleName', 'LastName', 'Suffix'),
                ('FirstName', 'MiddleName', None, 'Suffix')
            ],
            self.table_name
        )

class TestPublishersTable(SetUpDatabaseTableSchemaTestCase):
    table_name = 'Publishers'

    def setUp(self):
        database.create_table_publishers(self.db_path)

    def test_add_publisher_creates_valid_record(self):
        """Verifies add_publisher() creates a valid record."""
        self.assertCorrectRecordInsertion(
            database.add_publisher, [('NAME', ), ('name', )], self.table_name
        )

    def test_publisher_table_not_null_constraints(self):
        """Test not null constraints in Publishers table."""
        self.assertNotNullTableConstraints(
            database.add_publisher, [(None, )], self.table_name
        )

class TestGenresCategoriesTable(SetUpDatabaseTableSchemaTestCase):
    table_name = 'GenresCategories'

    def setUp(self):
        database.create_table_genrescategories(self.db_path)

    def test_add_genrecategory_creates_valid_record(self):
        """Verifies add_genrecategory() creates a valid record."""
        self.assertCorrectRecordInsertion(
            database.add_genrecategory, [('NAME', ), ('name', )], self.table_name
        )

    def test_genrecategory_table_not_null_constraints(self):
        """Test not null constraints in GenresCategories table."""
        self.assertNotNullTableConstraints(
            database.add_genrecategory, [(None, )], self.table_name
        )

class TestConditionsTable(SetUpDatabaseTableSchemaTestCase):
    table_name = 'Conditions'

    def setUp(self):
        database.create_table_conditions(self.db_path)

    def test_add_condition_creates_valid_record(self):
        """Verifies add_condition() creates a valid record."""
        self.assertCorrectRecordInsertion(
            database.add_condition,
            [
                ('NAME', 'DESCRIPTION'),
                ('name', None),
            ],
            self.table_name
        )

    def test_condition_table_not_null_constraints(self):
        """Test not null constraints in Conditions table."""
        self.assertNotNullTableConstraints(
            database.add_condition, [(None, 'Hello')], self.table_name
        )

class TestLocationsTable(SetUpDatabaseTableSchemaTestCase):
    table_name = 'Locations'

    def setUp(self):
        database.create_table_locations(self.db_path)

    def test_add_location_creates_valid_record(self):
        """Verifies add_location() creates a valid record."""
        self.assertCorrectRecordInsertion(
            database.add_location,
            [
                ('NAME', 'DESCRIPTION'),
                ('name', None)
            ],
            self.table_name
        )

    def test_location_table_not_null_constraints(self):
        self.assertNotNullTableConstraints(
            database.add_location, [(None, 'Description')], self.table_name
        )

class TestBooksTable(SetUpDatabaseTableSchemaTestCase):
    table_name = 'Books'

    def setUp(self):
        database.create_database(self.db_path)
        database.add_author(self.db_path, "AuthorFirst", "AuthorMiddle", "AuthorLast", "AuthorSuffix")
        database.add_publisher(self.db_path, "PublisherName")
        database.add_genrecategory(self.db_path, "GenreCategoryName")
        database.add_condition(self.db_path, "ConditionName", "ConditionDescription")
        database.add_location(self.db_path, "LocationName", "LocationDescription")

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
            database.add_book, [params, params], self.table_name
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
            "AuthorID, PublisherID, GenreID, ConditionID, and LocationID should have FOREIGN KEY constraints"
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
            database.add_book,
            [
                (None, AuthorID, PublisherID,
                GenreID, YearPublished, Edition,
                ConditionID, Description, DateAcquired,
                Price, LocationID, isbn),
                ('Title', None, PublisherID,
                GenreID, YearPublished, Edition,
                ConditionID, Description, DateAcquired,
                Price, LocationID, isbn)
            ],
            self.table_name
        )

### ENTRY POINT ###
if __name__ == '__main__':
    unittest.main()