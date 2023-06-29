import unittest
import os
import sqlite3
from source import database
from typing import Callable

DB_PATH = 'test_book_stack_database.db'
DB_REQUIRED_TABLES = {
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



# Test Cases

class TestDatabaseHasAttributes(unittest.TestCase):
    """Verifies all necessary function names, classes, and variables exist in database.py"""

    def test_create_database_function_is_defined(self):
        """Verifies create_database exists in database.py"""
        self.assertDatabaseHasAttribute('create_database')

    def test_add_author_function_is_defined(self):
        """Verifies add_author exists in database.py"""
        self.assertDatabaseHasAttribute('add_author')

    def test_add_publisher_function_is_defined(self):
        """Verifies add_publisher exists in database.py"""
        self.assertDatabaseHasAttribute('add_publisher')

    def test_add_genrecategory_function_is_defined(self):
        """Verifies add_genrecategory exists in database.py"""
        self.assertDatabaseHasAttribute('add_genrecategory')

    def test_add_condition_function_is_defined(self):
        """Verifies add_condition exists in database.py"""
        self.assertDatabaseHasAttribute('add_condition')

    def test_add_location_function_is_defined(self):
        """Verifies add_location exists in database.py"""
        self.assertDatabaseHasAttribute('add_location')

    def assertDatabaseHasAttribute(self, attr: str):
        """Helper function that asserts attr is in database.py"""
        self.assertTrue(
            hasattr(database, attr),
            f"Attribute '{attr}' not found in database.py"
        )




class _BaseTestDatabaseCase(unittest.TestCase):
    """Base class responsible for setup and teardown of tests requiring database file connection."""
    connection = None
    cursor = None

    def setUp(self):
        """Builds database, establishes connection, and sets cursor."""
        try:
            database.create_database(DB_PATH)
            self.connection = sqlite3.connect(DB_PATH)
            self.cursor = self.connection.cursor()
        except:
            print("Critical error: failed to establish database connection. Continuing tests.")

    def tearDown(self):
        """Closes database connection and removes test database file."""
        if self.connection is not None:
            self.connection.close()
            self.cursor = None
            self.connection = None
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

    def getUserDefinedTableNames(self):
        """Get the names of the user-defined tables from a specified table in the database."""
        self.cursor.execute(
            '''
            SELECT name FROM sqlite_master WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            '''
        )
        user_defined_tables = self.cursor.fetchall()
        user_defined_tables = [row[0] for row in user_defined_tables]
        return user_defined_tables




class TestDatabaseFileConnection(_BaseTestDatabaseCase):
    """Class responsible for ensuring database file is created and can establish a connection."""

    def test_database_file_is_created(self):
        """Verifies the database file exists."""
        self.assertTrue(
            os.path.exists(DB_PATH),
            f"File {DB_PATH} was not created."
        )

    def test_database_connection_is_established(self):
        """Verifies database can connect."""
        try:
            connection = sqlite3.connect(DB_PATH)
            cursor = connection.cursor()
            cursor.close()
            connection.close()
        except sqlite3.Error as e:
            self.fail(f"SQLite error: {str(e)}")



class TestDatabaseTableCreation(_BaseTestDatabaseCase):
    """TestCase for verifying proper database construction."""

    def test_db_tables_match_required_tables(self):
        """Verify only the required tables exist in the database."""
        user_defined_tables = self.getUserDefinedTableNames()
        self.assertCountEqual(
            user_defined_tables,
            DB_REQUIRED_TABLES,
            f"Table mismatch: requires {user_defined_tables}, has {DB_REQUIRED_TABLES.keys()}"
        )

    def test_authors_table_is_created(self):
        """Verifies the Authors table is created."""
        self.assertTableExists('Authors')

    def test_publishers_table_is_created(self):
        """Verifies the Publishers table is created."""
        self.assertTableExists('Publishers')

    def test_genrescategories_table_is_created(self):
        """Verifies the GenresCategories table is created."""
        self.assertTableExists('GenresCategories')

    def test_conditions_table_is_created(self):
        """Verifies the Conditions table is created."""
        self.assertTableExists('Conditions')

    def test_locations_table_is_created(self):
        """Verifies the Locations table is created."""
        self.assertTableExists('Locations')

    def test_books_table_is_created(self):
        """Verifies the Books table is created."""
        self.assertTableExists('Books')

    def assertTableExists(self, table: str):
        """Assert that the specified table exists."""
        assert table in DB_REQUIRED_TABLES, f"{table} not found in required tables definition"
        tables = self.getUserDefinedTableNames()
        self.assertIn(
            table, tables,
            f"Expected '{table}' table in required tables. Table not found."
        )




class TestDatabaseTableColumnCreation(_BaseTestDatabaseCase):
    """TestCase for verifying proper database construction."""

    def test_authors_table_columns_match_required_columns(self):
        """Verifies the structure of the Authors table."""
        self.assertTableColumnsMatchRequiredColumns('Authors')

    def test_publishers_table_columns_match_required_columns(self):
        """Verifies the structure of the Columns table."""
        self.assertTableColumnsMatchRequiredColumns('Publishers')

    def test_genrescategories_table_columns_match_required_columns(self):
        """Verifies the structure of the GenresCategories table."""
        self.assertTableColumnsMatchRequiredColumns('GenresCategories')

    def test_conditions_table_columns_match_required_columns(self):
        """Verifies the structure of the Conditions table."""
        self.assertTableColumnsMatchRequiredColumns('Conditions')

    def test_locations_table_columns_match_required_columns(self):
        """Verifies the structure of the Locations table."""
        self.assertTableColumnsMatchRequiredColumns('Locations')

    def test_books_table_columns_match_required_columns(self):
        """Verifies the structure of the Books table."""
        self.assertTableColumnsMatchRequiredColumns('Books')

    def assertTableColumnsMatchRequiredColumns(self, table: str):
        """Assert that the columns in the specified table match the required schema."""
        assert table in DB_REQUIRED_TABLES, f"{table} not found in required tables definition"
        columns = self.getColumnNamesFromTable(table)
        required_columns = DB_REQUIRED_TABLES[table]
        self.assertEqual(
            columns,
            required_columns,
            f"{table} columns mismatch: requires {required_columns}, has {columns}"
        )

    def getColumnNamesFromTable(self, table: str):
        """Get the names of the columns from a specified table in the database."""
        assert table in DB_REQUIRED_TABLES, f"{table} not found in required tables definition"
        tables = self.getUserDefinedTableNames()
        if table in tables:
            self.cursor.execute(f"PRAGMA table_info({table})")
            column_definitions = self.cursor.fetchall()
            columns = [column[1] for column in column_definitions]
        else:
            columns = []
        return columns




class TestDatabaseConstraintFunctionality(_BaseTestDatabaseCase):
    """TestCase for verifying proper database construction."""

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

    def test_author_table_not_null_constraints(self):
        """Test not null constraints in Authors table."""
        self.assertAddRecordNotNullColumnConstraints(
            database.add_author,
            [
                (None, 'MiddleName', 'LastName', 'Suffix'),
                ('FirstName', 'MiddleName', None, 'Suffix')
            ],
            'Authors'
        )

    def test_publisher_table_not_null_constraints(self):
        """Test not null constraints in Publishers table."""
        self.assertAddRecordNotNullColumnConstraints(
            database.add_publisher, [(None, )], 'Publishers'
        )

    def test_genrecategory_table_not_null_constraints(self):
        """Test not null constraints in GenresCategories table."""
        self.assertAddRecordNotNullColumnConstraints(
            database.add_genrecategory, [(None, )], 'GenresCategories'
        )

    def test_condition_table_not_null_constraints(self):
        """Test not null constraints in Conditions table."""
        self.assertAddRecordNotNullColumnConstraints(
            database.add_condition, [(None, 'Hello')], 'Conditions'
        )

    def test_location_table_not_null_constraints(self):
        self.assertAddRecordNotNullColumnConstraints(
            database.add_location, [(None, 'Description')]
        )

    def assertAddRecordNotNullColumnConstraints(self, func: Callable, params_list: list, table: str):
        """Asserts NOT NULL constraint detected when adding record."""
        for params in params_list:

            args = (DB_PATH, ) + params
            index = next((i for (i, value) in enumerate(params) if value is None), None)
            message = f"Element index {index} in '{table}' row should have NOT NULL constraint: {params}"

            with self.assertRaises(sqlite3.IntegrityError, msg = message) as context:
                func(*args)
            error_message = str(context.exception)
            self.assertIn("NOT NULL constraint failed", error_message, message)




class TestDatabaseInsertionFunctionality(_BaseTestDatabaseCase):
    """Testcase for verifying records inserted into tables properly."""
    def test_add_author_creates_valid_record(self):
        """Verifies add_author() creates a valid record."""
        self.assertAddRecordFunction(
            database.add_author,
            [
                ('FIRST', 'M', 'LAST', 'SUFFIX'),
                ('first', 'm', 'last', 'suffix'),
                ('Bob', None, 'Anderson', None)
            ],
            'Authors'
        )

    def test_add_publisher_creates_valid_record(self):
        """Verifies add_publisher() creates a valid record."""
        self.assertAddRecordFunction(
            database.add_publisher, [('NAME', ), ('name', )], 'Publishers'
        )

    def test_add_genrecategory_creates_valid_record(self):
        """Verifies add_genrecategory() creates a valid record."""
        self.assertAddRecordFunction(
            database.add_genrecategory, [('NAME', ), ('name', )], 'GenresCategories'
        )

    def test_add_condition_creates_valid_record(self):
        """Verifies add_condition() creates a valid record."""
        self.assertAddRecordFunction(
            database.add_condition,
            [
                ('NAME', 'DESCRIPTION'),
                ('name', None),
            ],
            'Conditions'
        )

    def test_add_location_creates_valid_record(self):
        """Verifies add_location() creates a valid record."""
        self.assertAddRecordFunction(
            database.add_location,
            [
                ('NAME', 'DESCRIPTION'),
                ('name', None)
            ],
            'Locations'
        )

    def assertAddRecordFunction(self, func: Callable, params_list: list, table: str):
        """Asserts records are added to database."""
        for params in params_list:
            args = (DB_PATH, ) + params
            func(*args)

        assert table in DB_REQUIRED_TABLES, f"'{table}' is not a valid table name."

        self.cursor.execute(f"SELECT * FROM {table}")

        id = 0
        for params in params_list:
            id += 1
            expected_record = (id, ) + params
            record = self.cursor.fetchone()
            self.assertIsNotNone(
                record,
                f"No record found in the '{table}' table."
            )
            self.assertEqual(
                len(record), len(expected_record),
                f"Invalid number of fields."
            )
            self.assertEqual(
                record, expected_record,
                f"Expected: {expected_record}, Received {record}"
            )


# Entry Point

if __name__ == '__main__':
    unittest.main()