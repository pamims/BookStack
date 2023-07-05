"""Test cases for database.py"""

import inspect
import os
import sqlite3
import sys
from typing import Any, Callable, Iterable, Optional, Tuple
import unittest
from source import database


class BaseDatabaseModuleTestCase(unittest.TestCase):
    """Base test case for all database tests."""
    db_path = 'test_book_stack_database.db'
    db_required_tables = {
        'Title': ['ID', 'Name'],
        'Author': ['ID', 'Prefix', 'First', 'Middle', 'Last', 'Suffix'],
        'Genre': ['ID', 'Name'],
        'TitleAuthor': ['ID', 'TitleID', 'AuthorID'],
        'Work': ['ID', 'TitleAuthorID', 'GenreID'],

        'Format': ['ID', 'Name'],
        'Publisher': ['ID', 'Name'],
        'Publication': ['ID', 'WorkID', 'FormatID', 'PublisherID', 'ISBN'],

        'Condition': ['ID', 'Name', 'Description'],
        'Location': ['ID', 'LocationID', 'Name', 'Description'],
        'Book': ['ID', 'PublicationID', 'ConditionID', 'LocationID']
    }

    # These are [class members] because I want to use them in setUpClass and
    # tearDownClass -- the connection only needs established once per TestCase
    connection: sqlite3.Connection = None
    cursor: sqlite3.Cursor = None

    def tearDown(self) -> None:
        """Clean the database after each test"""
        self.remove_all_database_tables()

    @classmethod
    def setUpClass(cls) -> None:
        """Builds database, establishes connection, and sets cursor."""
        cls.open_database_connection()

    @classmethod
    def tearDownClass(cls) -> None:
        """Closes database connection and removes test database file."""
        cls.close_database_connection()
        cls.remove_database_file()

    @classmethod
    def open_database_connection(cls) -> None:
        """Opens database and gets cursor."""
        cls.connection = sqlite3.connect(cls.db_path)
        cls.cursor = cls.connection.cursor()

    @classmethod
    def close_database_connection(cls) -> None:
        """Closes database and sets connection and cursor to None."""
        if cls.connection is not None:
            cls.cursor.close()
            cls.connection.close()
            cls.connection = None
            cls.cursor = None

    @classmethod
    def remove_database_file(cls) -> None:
        """Finds and removes test database file as cleanup."""
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    @classmethod
    def remove_all_database_tables(cls) -> None:
        """Drops all tables in the database."""
        user_defined_tables = cls.get_database_table_names()
        for table in user_defined_tables:
            cls.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cls.connection.commit()

    @classmethod
    def validate_cursor(cls, msg: str = None) -> None:
        """Errors when there is no cursor object."""
        if cls.cursor is None:
            if msg is None:
                msg = "No cursor object exists."
            raise cls.NoDatabaseConnectionError(msg)

    @classmethod
    def validate_table_name(
        cls, table_name: str, db_schema: Iterable, msg: str = None
    ) -> None:
        """Ensures the table name is in the given iterable."""
        if table_name not in db_schema:
            if msg is None:
                msg = (
                    f"Invalid table name: '{table_name}'"
                    f"Valid names include: {', '.join(list(db_schema))}"
                )
            raise cls.DatabaseTableError(msg)

    @classmethod
    def get_database_table_names(cls) -> Optional[list[str]]:
        """Get the table names from the database."""
        cls.validate_cursor("Cannot get table names. No database connection.")
        cls.cursor.execute(
            '''
            SELECT name FROM sqlite_master WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            '''
        )
        result = [row[0] for row in cls.cursor.fetchall()]
        return result

    @classmethod
    def get_table_column_names(cls, table_name: str) -> Optional[list[str]]:
        """Get the column names from a specific table in the database."""
        cls.validate_table_name(
            table_name, cls.db_required_tables,
            f"Cannot retrieve columns for '{table_name}' table.\n"
            f"Valid names include: {', '.join(list(cls.db_required_tables))}\n"
            f"Invalid table name: {table_name}\n"
        )
        table_names = cls.get_database_table_names()
        if table_name not in table_names:
            return []  # no columns in table that doesn't exist
        cls.cursor.execute(f"PRAGMA table_info({table_name})")
        result = cls.cursor.fetchall()
        # if result is not None: just return the empty list
        result = [row[1] for row in result]
        return result

    @classmethod
    def get_valid_record_id(cls, table_name: str) -> Optional[list[int]]:
        """Returns all ID (primary key) column values from a table."""
        cls.validate_cursor(
            "Could not get Record IDs. No database connection."
        )
        cls.validate_table_name(
            table_name, cls.db_required_tables,
            f"Cannot retrieve columns for '{table_name}' table.\n"
            f"Valid names include: {', '.join(list(cls.db_required_tables))}\n"
            f"Invalid table name: {table_name}\n"
        )
        table_names = cls.get_database_table_names()
        cls.validate_table_name(
            table_name, table_names,
            f"Cannot retrieve columns for '{table_name}' table.\n"
            f"Tables in database: {', '.join(list(table_names))}\n"
            f"Table name given: {table_name}\n"
        )
        cls.cursor.execute(f"SELECT ID FROM {table_name} ORDER BY ID")
        result = cls.cursor.fetchall()
        # if result is not None: just return the empty list
        result = [int(item[0]) for item in result]
        return result

    class DatabaseError(Exception):
        """
        Parent class for database-related errors in BaseDatabaseModuleTestCase.
        """
        def __init__(self, msg: str = None):
            super().__init__(msg)

        def __str__(self) -> str:
            msg = super().args[0]
            name = type(self).__name__
            msg = name + f": {msg}" if isinstance(msg, str) else ""
            return msg

    class NoDatabaseConnectionError(DatabaseError):
        """Exception raised when no database connection is found."""

    class DatabaseTableError(DatabaseError):
        """Exception raised when there is a problem finding a table."""

    def assert_record_insertion_correct(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_tuple: Tuple[Tuple[Optional[str]], ...]
    ) -> None:
        """
        Asserts correct table insertion. Includes: Record exists,
        ID AUTOINCREMENT, Number of fields, Correct values
        """
        self.validate_table_name(table_name, self.db_required_tables)
        self.validate_cursor()

        for params in params_tuple:
            try:
                insert_row_func(self.db_path, *params)
            except sqlite3.Error as exc:
                self.fail(f"Error during '{table_name}' insertion: {str(exc)}")

        self.cursor.execute(f"SELECT * FROM {table_name}")
        expected_id = 0
        for params in params_tuple:
            assert len(params) == len(set(params)), (
                "Cannot assert correct record insertion. Cannot verify "
                "correct order of parameters because multiple parameters have "
                "the same value. Ensure different values are being passed for "
                "each parameter."
            )
            expected_id += 1  # sqlite database autoincrement id's start at 1
            expected_record = (expected_id, ) + params
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

    def assert_not_null_table_constraints(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_tuple: Tuple[Tuple[Optional[str]], ...]
    ) -> None:
        """Asserts NOT NULL constraint detected when adding record."""
        try:
            column_names = self.get_table_column_names(table_name)
        except self.DatabaseError as exc:  # could raise either one
            self.fail(f"Could not assert NOT NULL constraints: {str(exc)}")

        for params in params_tuple:
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
            col_index = params.index(None) + 1  # accounts for ID column
            column_tested = column_names[col_index]
            # generate the failure message
            msg = f"{table_name} [{column_tested}] NOT NULL constraint fails."
            with self.assertRaises(sqlite3.IntegrityError, msg=msg) as ctx:
                insert_row_func(self.db_path, *params)
            error_msg = str(ctx.exception)
            self.assertIn("NOT NULL constraint failed", error_msg, msg)

    def assert_foreign_key_constraints(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_tuple: Tuple[Tuple[Optional[str]], ...]
    ) -> None:
        """Asserts FOREIGN KEY constraint detected when adding record."""
        assert len(params_tuple) == 1, (
            f"Only send one set of parameters at a time to check FOREIGN KEY "
            f"constraint. FK tests must be performed individually.\nTable: "
            f"{table_name}\nParemeter sets:\n"
            f"{'; '.join(map(str, params) for params in params_tuple)}"
        )
        params = params_tuple[0]
        with self.assertRaises(
            sqlite3.IntegrityError,
            msg=f"No Foreign Key violation error in table {table_name}."
        ) as ctx:
            insert_row_func(self.db_path, *params)
        error_msg = str(ctx.exception)
        self.assertIn(
            "FOREIGN KEY constraint failed", error_msg,
            f"Could not verify Foreign Key constraint for table {table_name} "
            f"due to some other error:\n{error_msg}"
        )

    def assert_unique_table_constraint(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_tuple: Tuple[Tuple[Optional[str]], Tuple[Optional[str]]],
            num_items: int = 1
    ) -> None:
        """Asserts UNIQUE constraint detected when adding record."""
        assert len(params_tuple) == 2, (
            "Wrong number of parameter sets. To assert unique table "
            "constraints, exactly two parameter sets must be given.\nSets "
            "Given:\n"
            f"{'; '.join(map(str, params_tuple)) if params_tuple else 'None'}"
        )
        for params in params_tuple:
            assert len(params) == len(set(params)), (
                f"Ensure no two parameters in a single record are the same. "
                f"Received: {params}, "
                f"{', '.join(' == '.join([str(p)] * 2) for p in set(params))}"
            )
        # params_tuple[0], params_tuple[1] = params_tuple
        try:
            column_names = self.get_table_column_names(table_name)
        except self.DatabaseError as exc:  # could raise either one
            self.fail(f"Could not assert UNIQUE constraints: {str(exc)}")
        # make sure the parameters were programmed in correctly
        a_len = len(params_tuple[0])
        assert (
            a_len + 1 == len(column_names) and a_len == len(params_tuple[1])
        ), (
            "Number of parameters for the insertion call does not match "
            "the number of columns that are supposed to be in the table."
        )
        # make sure exactly num_item item(s) in each parameter list is the same
        matched_items = [
            (a, b) for a, b in zip(params_tuple[0], params_tuple[1]) if a == b
        ]
        assert len(matched_items) == num_items, (
            f"To check UNIQUE constraints properly, the parameter lists must "
            f"contain exactly {num_items} item(s) in common. The particular "
            f"value being tested should be the common item.\nMatching items: "
            f"{'None' if len(matched_items) == 0 else matched_items}, \n"
            f"First set: {params_tuple[0]}\nSecond set: {params_tuple[1]}\n"
            f"{'; '.join(f'{a} != {b}' for a, b in zip(*params_tuple))}"
        )
        # get the indices of each match
        column_indices = [
            params_tuple[0].index(match[0]) + 1 for match in matched_items
        ]
        columns_tested = tuple(column_names[i] for i in column_indices)
        msg = f"{table_name} [{columns_tested}] UNIQUE constraint fails."
        with self.assertRaises(sqlite3.IntegrityError, msg=msg) as ctx:
            insert_row_func(self.db_path, *params_tuple[0])
            insert_row_func(self.db_path, *params_tuple[1])
        error_msg = str(ctx.exception)
        self.assertIn("UNIQUE constraint failed", error_msg, msg)

# ## TEST CASES ## #


class BaseTableTestCase(BaseDatabaseModuleTestCase):
    """
    Base class for test cases where a single table must exist before the test
    can be performed. This class allows all child class tests to be updated
    from a single location. Ex. if an insertion function is renamed, it only
    needs to be renamed in the db_insert_functions dictionary.

    __init__, self.insert_function --
    The __init__ function detects which insert function the child class needs
    by looking at the value of table_name and retrieving the function from the
    db_insert_functions dictionary. The value is then stored in
    self.insert_function.

    setUp --
    The setUp function similarly decides which create_table_* function to call
    at the beginning of each test.
    """
    db_insert_functions = {
        'Title': database.insert_title,
        'Author': database.insert_author,
        'Genre': database.insert_genre,
        'TitleAuthor': database.insert_titleauthor,
        'Work': database.insert_work,

        'Format': database.insert_format,
        'Publisher': database.insert_publisher,
        'Publication': None,

        'Condition': database.insert_condition,
        'Location': None,
        'Book': None
    }
    db_create_table_functions = {
        'Title': database.create_table_title,
        'Author': database.create_table_author,
        'Genre': database.create_table_genre,
        'TitleAuthor': database.create_table_titleauthor,
        'Work': database.create_table_work,

        'Format': database.create_table_format,
        'Publisher': database.create_table_publisher,
        'Publication': None,

        'Condition': database.create_table_condition,
        'Location': None,
        'Book': None
    }
    table_name = None

    def __init__(self, methodName: str = "runTest"):
        """
        Sets the create_table_function to be called during setUp. Function will
        be a failure function if a problem occurs with retrieving the function.
        The function is based on the defined table_name and the
        create_table_functions dictionary.
        """
        super().__init__(methodName)
        self.insert_function = self.get_table_function_from_dictionary(
            self.table_name, self.db_insert_functions
        )

    def setUp(self) -> None:
        """Create necessary table for testing based on table_name."""
        create_table = self.get_table_function_from_dictionary(
            self.table_name, self.db_create_table_functions
        )
        create_table(self.db_path)

    def get_table_function_from_dictionary(
            self: BaseDatabaseModuleTestCase, table_name: str,
            dictionary: dict[str, Callable[[str, tuple[Any, ...]], Any]]
    ) -> Callable[[str, tuple[Any, ...]], Any]:
        """Retrieves function from a db_ dictionary based on table_name"""
        self.validate_table_name(
            table_name, dictionary,
            f"Cannot locate function. Table '{table_name}' is not defined "
            f"in the provided dictionary. Available keys are: "
            f"{', '.join(dictionary)}"
        )
        func: Callable[[str, tuple[Any, ...]], Any] = (
            dictionary[table_name]
        )
        if not (inspect.isfunction(func) or inspect.ismethod(func)):
            if func is None:
                msg = (
                    f"'{table_name}' table function has not been added to the "
                    f"provided dictionary. Verify function exists and add it "
                    f"to the appropriate dictionary."
                )
            else:
                msg = (
                    f"'{table_name}' table function is not a function or "
                    f"method. Verify correct function definition."
                )
            self.fail(msg)
        return func


class TitleTableTestCase(BaseTableTestCase):
    """
    Tests for validating Title table function. Titles must be inserted
    correctly with correct auto-incrementing ID's. Title name should be
    NOT NULL, and it should NOT have UNIQUE constraint.
    """
    table_name = 'Title'

    def test_title_insert_record_creation(self) -> None:
        """Verifies insert_publisher() creates a valid record."""
        params_tuple = ((f'Name{i}', ) for i in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_title_name_not_unique_constraint(self) -> None:
        """Verifies no unique constraint on title Name field."""
        params_tuple = (('Name', ) for _ in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_title_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on title Name field."""
        params_tuple = ((None, ), )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )


class GenreTableTestCase(BaseTableTestCase):
    """
    Tests for validating Genre table function. Genres must be inserted
    correctly with correct auto-incrementing ID's. Title name should
    be UNIQUE and NOT NULL.
    """
    table_name = 'Genre'

    def test_genre_insert_record_creation(self) -> None:
        """Verifies insert_genre() creates a valid record."""
        params_tuple = ((f'Name{i}', ) for i in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_genre_name_unique_constraint(self) -> None:
        """Verifies unique constraint on the genre name field."""
        params_tuple = (('Name', ), ('Name', ))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_genre_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on genre name field."""
        params_tuple = ((None, ), )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )


class FormatTableTestCase(BaseTableTestCase):
    """
    Tests for validating Format table function. Formats must be inserted
    correctly with correct auto-incrementing ID's. Format name should
    be UNIQUE and NOT NULL.
    """
    table_name = 'Format'

    def test_format_insert_record_creation(self) -> None:
        """Verifies insert_format() creates a valid record."""
        params_tuple = ((f'Name{i}', ) for i in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_format_name_unique_constraint(self) -> None:
        """Verifies unique constraint on the format name field."""
        params_tuple = (('Name', ), ('Name', ))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_format_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on format name field."""
        params_tuple = ((None, ), )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )


class PublisherTableTestCase(BaseTableTestCase):
    """
    Tests for validating Publisher table function. Publishers must be inserted
    correctly with correct auto-incrementing ID's. Publisher name should
    be UNIQUE and NOT NULL.
    """
    table_name = 'Publisher'

    def test_publisher_insert_record_creation(self) -> None:
        """Verifies insert_publisher() creates a valid record."""
        params_tuple = ((f'Name{i}', ) for i in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_publisher_name_unique_constraint(self) -> None:
        """Verifies unique constraint on the publisher name field."""
        params_tuple = (('Name', ), ('Name', ))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_publisher_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on publisher name field."""
        params_tuple = ((None, ), )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )


class ConditionTableTestCase(BaseTableTestCase):
    """
    Tests for validating Condition table function. Conditions must be inserted
    correctly with auto-incrementing ID's. Condition names and descriptions
    must be NOT NULL and UNIQUE.
    """
    table_name = 'Condition'

    def test_condition_insert_record_function(self) -> None:
        """Verifies insert_condition() creates a valid record."""
        params_tuple = ((f'Name{i}', f'Description{i}') for i in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_condition_name_unique_constraint(self) -> None:
        """Verifies unique constraint on condition name field."""
        params_tuple = (('Name', 'Description1'), ('Name', 'Description2'))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_condition_description_unique_constraint(self) -> None:
        """Verifies unique constraint on condition description field."""
        params_tuple = (('Name1', 'Description'), ('Name2', 'Description'))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_condition_name_and_description_not_null_constraints(self) -> None:
        """
        Verifies not null constraints on condition name and description fields.
        """
        params_tuple = ((None, 'Description'), ('Name', None))
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )


class AuthorTableTestCase(BaseTableTestCase):
    """
    Tests for validating Author table function. Authors must be inserted
    correctly with auto-incrementing ID's. Author First[Name] must be NOT NULL.
    Author Prefix, Middle, Last, and Suffix must be NULLABLE.
    """
    table_name = 'Author'

    def test_author_insert_record_creation(self) -> None:
        """Verifies insert_author() creates a valid record."""
        params_tuple = (
            (f'Prefix{i}', f'First{i}', f'Middle{i}', f'Last{i}', f'Suffix{i}')
            for i in range(1, 10)
        )
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_first_not_null_constraint(self) -> None:
        """Verifies not null constraint on author first field."""
        params_tuple = (('Prefix', None, 'Middle', 'Last', 'Suffix'), )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_prefix_nullable_constraint(self) -> None:
        """Verifies author prefix field nullable."""
        params_tuple = ((None, 'First', 'Middle', 'Last', 'Suffix'), )
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_middle_nullable_constraint(self) -> None:
        """Verifies author middle field nullable."""
        params_tuple = (('Prefix', 'First', None, 'Last', 'Suffix'), )
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_last_nullable_constraint(self) -> None:
        """Verifies author last field nullable."""
        params_tuple = (('Prefix', 'First', 'Middle', None, 'Suffix'), )
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_suffix_nullable_constraint(self) -> None:
        """Verifies author suffix field nullable."""
        params_tuple = (('Prefix', 'First', 'Middle', 'Last', None), )
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )


class BaseDependentTableTestCase(BaseTableTestCase):
    """
    Base class for test cases where multiple tables must exist prior to running
    tests.
    """
    referenced_table_names: tuple[str] = ()

    def __init__(self, methodName: str = "runTest"):
        """
        Gets the necessary create_table_* and insert_* functions to set up the
        test environment.
        """
        super().__init__(methodName)
        self.insert_funcs = tuple(
            self.get_table_function_from_dictionary(
                name, self.db_insert_functions
            ) for name in self.referenced_table_names
        )
        self.create_table_funcs = tuple(
            self.get_table_function_from_dictionary(
                name, self.db_create_table_functions
            ) for name in self.referenced_table_names
        )

    def setUp(self):
        """Creates and populates the necessary tables."""
        # Create the test table.
        super().setUp()
        for create_table_func in self.create_table_funcs:
            create_table_func(self.db_path)
        # Make 5 insertions in each table.
        for i in range(1, 6):
            for table_name, insert_func in zip(
                self.referenced_table_names, self.insert_funcs
            ):
                # Need to know the column names to generate input.
                column_names = self.get_table_column_names(table_name)
                params = (
                    i if 'ID' in column_name else column_name + str(i)
                    for column_name in column_names[1:]  # don't want ID column
                )
                try:
                    insert_func(self.db_path, *params)
                except sqlite3.IntegrityError as exc:
                    err_msg = str(exc)
                    if "FOREIGN KEY constraint failed" in err_msg:
                        self.fail(
                            "Check the order that table names are defined in "
                            "the referenced_table_names tuple. Referenced "
                            "tables must be listed before dependent tables."
                        )
                    else:
                        self.fail(str(exc))
                except sqlite3.Error as exc:
                    self.fail(str(exc))


class TitleAuthorTableTestCase(BaseDependentTableTestCase):
    """
    Tests for validating TitleAuthor table function. TitleAuthors must be
    inserted correctly with auto-incrementing ID's. Each (TitleID, AuthorID)
    combination must be unique, and they must have FOREIGN KEY constraints.
    """
    table_name = 'TitleAuthor'
    referenced_table_names = ('Title', 'Author')

    def test_titleauthor_insert_record_creation(self) -> None:
        """Verifies insert_titleauthor() creates a valid record."""
        title_ids = self.get_valid_record_id('Title')
        author_ids = self.get_valid_record_id('Author')
        author_ids = author_ids[-1:] + author_ids[:-1]
        params_list = tuple(zip(title_ids, author_ids))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_list
        )

    def test_titleauthor_authorid_titleid_unique_constraint(self) -> None:
        """Verifies the unique constraint of TitleID, AuthorID pairs."""
        title_ids = self.get_valid_record_id('Title')
        author_ids = self.get_valid_record_id('Author')
        params_list = next(
            ((t, a), ) * 2 for t in title_ids for a in author_ids if t != a
        )
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_list, 2
        )

    def test_titleauthor_authorid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of authorid."""
        title_ids = self.get_valid_record_id('Title')
        author_ids = self.get_valid_record_id('Author')
        ids = set(title_ids).union(author_ids)
        invalid_id = next(
            i for i in range(1, len(ids) + 2) if i not in ids
        )
        params_list = ((title_ids[0], invalid_id), )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )

    def test_titleauthor_titleid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of titleid."""
        title_ids = self.get_valid_record_id('Title')
        author_ids = self.get_valid_record_id('Author')
        ids = set(title_ids).union(author_ids)
        invalid_id = next(
            i for i in range(1, len(ids) + 2) if i not in ids
        )
        params_list = ((invalid_id, author_ids[0]), )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )


class WorkTableTestCase(BaseDependentTableTestCase):
    """
    Tests for validating Work table function. Work must be inserted correctly
    with auto-incrementing ID's. Each (TitleAuthorID, GenreID) combination must
    be unique, and they must have FOREIGN KEY constraints.
    """

    table_name = 'Work'
    referenced_table_names = ('Title', 'Author', 'TitleAuthor', 'Genre')

    def test_work_insert_record_createion(self) -> None:
        """Verifies insert_work() creates a valid record."""
        ta_ids = self.get_valid_record_id('TitleAuthor')
        genre_ids = self.get_valid_record_id('Genre')
        genre_ids = genre_ids[-1:] + genre_ids[:-1]
        params_list = tuple(zip(ta_ids, genre_ids))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_list
        )

    def test_work_titleauthorid_genreid_unique_constraint(self) -> None:
        """Verifies the unique constraint of TitleAuthorID, GenreID pairs."""
        ta_ids = self.get_valid_record_id('TitleAuthor')
        genre_ids = self.get_valid_record_id('Genre')
        params_list = next(
            ((ta, g), ) * 2 for ta in ta_ids for g in genre_ids if ta != g
        )
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_list, 2
        )

    def test_work_genreid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of genreid."""
        ta_ids = self.get_valid_record_id('TitleAuthor')
        genre_ids = self.get_valid_record_id('Genre')
        ids = set(ta_ids).union(genre_ids)
        invalid_id = next(
            i for i in range(1, len(ids) + 2) if i not in ids
        )
        params_list = ((ta_ids[0], invalid_id), )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )

    def test_work_titleauthorid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of titleauthorid."""
        ta_ids = self.get_valid_record_id('TitleAuthor')
        genre_ids = self.get_valid_record_id('Genre')
        ids = set(ta_ids).union(genre_ids)
        invalid_id = next(
            i for i in range(1, len(ids) + 2) if i not in ids
        )
        params_list = ((invalid_id, genre_ids[0]), )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )


def __disjoint_union(*iterables: Iterable) -> set[str]:
    """
    Takes an arbitrary number of iterables and subtracts their intersection
    from their union. This is specifically used to assert that all of the
    dictionary keys match and provide a warning if something is wrong with one
    of the dictionary definitions -- if disjoint union is 0, assertion passes.
    """
    union = set().union(*iterables)
    intersection = set(iterables[0]).intersection(*iterables[1:])
    disjoint_union = union.symmetric_difference(intersection)
    return disjoint_union


__classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
__db_dictionaries = [
    m
    for (_, class_obj) in __classes
    for m in inspect.getmembers(class_obj)
    if m[0].startswith('db_') and isinstance(m[1], dict)
]
__dict_names = {d[0] for d in __db_dictionaries}
__db_dictionaries = [d[1] for d in __db_dictionaries]

__mismatched_dictionary_items = __disjoint_union(*__db_dictionaries)

assert len(__mismatched_dictionary_items) == 0, (
    f"""
    Keys representing database tables are mismatched across locations.
    Verify keys are correct.\nMismatched keys: {__mismatched_dictionary_items}
    \nDictionary names: {__dict_names}
    """
)

# ## ENTRY POINT ## #

if __name__ == '__main__':
    unittest.main()
