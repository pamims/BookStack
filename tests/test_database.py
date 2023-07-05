import unittest
import os
import sqlite3
from source import database
from typing import Callable, Iterable, Optional, Any

### BASE CLASSES ###
class BaseDatabaseModuleTestCase(unittest.TestCase):
    """Base test case for all database tests."""
    db_path = 'test_book_stack_database.db'
    db_required_tables = {
        'Title'     : ['ID', 'Name'],
        'Author'    : ['ID', 'Prefix', 'First', 'Middle', 'Last', 'Suffix'],
        'Genre'     : ['ID', 'Name'],
        'TitleAuthor' : ['ID', 'TitleID', 'AuthorID'],
        'Work'      : ['ID', 'TitleAuthorID', 'GenreID'],

        'Format'    : ['ID', 'Name'],
        'Publisher' : ['ID', 'Name'],
        'Publication' : ['ID', 'WorkID', 'FormatID', 'PublisherID', 'ISBN'],

        'Condition' : ['ID', 'Name', 'Description'],
        'Location'  : ['ID', 'LocationID', 'Name', 'Description'],
        'Book'      : ['ID', 'PublicationID', 'ConditionID', 'LocationID']
    }

    # These are [class members] because I want to use them in setUpClass and
    # tearDownClass -- the connection only needs established once per TestCase
    connection: sqlite3.Connection = None
    cursor: sqlite3.Cursor = None

    def tearDown(self) -> None:
        # Clean the database after each test
        self.removeAllDatabaseTables()

    @classmethod
    def setUpClass(cls) -> None:
        """Builds database, establishes connection, and sets cursor."""
        cls.openDatabaseConnection()

    @classmethod
    def tearDownClass(cls) -> None:
        """Closes database connection and removes test database file."""
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
    def validateTableName(
        cls, table_name: str, db_schema: Iterable, msg: str = None
    ) -> None:
        if table_name not in db_schema:
            if msg is None:
                valid_list = ', '.join(list(db_schema))
                msg = (
                    f"Invalid table name: '{table_name}'"
                    f"Valid names include: {valid_list}"
                )
            raise cls.DatabaseTableError(msg)

    @classmethod
    def getDatabaseTableNames(cls) -> Optional[list[str]]:
        """Get the table names from the database."""
        cls.validateCursor("Cannot get table names. No database connection.")
        cls.cursor.execute(
            '''
            SELECT name FROM sqlite_master WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            '''
        )
        result = cls.cursor.fetchall()
        if result is not None:
            result = [row[0] for row in result]
        return result

    @classmethod
    def getTableColumnNames(cls, table_name: str) -> Optional[list[str]]:
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
        if result is not None:
            result = [row[1] for row in result]
        return result

    @classmethod
    def getValidRecordIDs(cls, table_name: str) -> Optional[list[int]]:
        cls.validateCursor("Could not get Record IDs. No database connection.")
        valid_list_str = ', '.join(list(cls.db_required_tables))
        cls.validateTableName(
            table_name, cls.db_required_tables,
            f"Cannot retrieve columns for '{table_name}' table.\n"
            f"Valid names include: {valid_list_str}\n"
            f"Invalid table name: {table_name}\n"
        )
        table_names = cls.getDatabaseTableNames()
        table_list_str = ', '.join(list(table_names))
        cls.validateTableName(
            table_name, table_names,
            f"Cannot retrieve columns for '{table_name}' table.\n"
            f"Tables in database: {table_list_str}\n"
            f"Table name given: {table_name}\n"
        )
        cls.cursor.execute(f"SELECT ID FROM {table_name} ORDER BY ID")
        result = cls.cursor.fetchall()
        if result is not None:
            result = [int(item[0]) for item in result]
        return result

    class DatabaseError(Exception):
        def __init__(self, msg: str = None):
            super().__init__(msg)
        def __str__(self) -> str:
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
            params_tuple: tuple[tuple[Optional[str]]]
    ) -> None:
        """
        Asserts correct table insertion. Includes:
        Record exists
        ID AUTOINCREMENT
        Number of fields
        Correct values
        """
        self.validateTableName(table_name, self.db_required_tables)
        self.validateCursor()
        for params in params_tuple:
            try:
                insert_row_func(self.db_path, *params)
            except Exception as e:
                self.fail(f"Error during '{table_name}' insertion: {str(e)}")

        self.cursor.execute(f"SELECT * FROM {table_name}")
        id = 0
        for params in params_tuple:
            assert len(params) == len(set(params)), (
                "Cannot assert correct record insertion. Cannot verify "
                "correct order of parameters because multiple parameters have "
                "the same value. Ensure different values are being passed for "
                "each parameter."
            )
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
            params_tuple: tuple[tuple[Optional[str]]]
    ) -> None:
        """Asserts NOT NULL constraint detected when adding record."""
        try:
            column_names = self.getTableColumnNames(table_name)
        except self.DatabaseError as e: # could raise either one
            self.fail(f"Could not assert NOT NULL constraints: {str(e)}")
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
            col_index = params.index(None) + 1 # accounts for ID column
            column_tested = column_names[col_index]
            # generate the failure message
            msg = f"{table_name} [{column_tested}] NOT NULL constraint fails."
            with self.assertRaises(sqlite3.IntegrityError, msg = msg) as ctx:
                insert_row_func(self.db_path, *params)
            error_msg = str(ctx.exception)
            self.assertIn("NOT NULL constraint failed", error_msg, msg)

    def assertForeignKeyTableConstraint(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_tuple: tuple[tuple[Optional[str]]]
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
            msg = f"No Foreign Key violation error in table {table_name}."
        ) as ctx:
            insert_row_func(self.db_path, *params)
        error_msg = str(ctx.exception)
        self.assertIn(
            "FOREIGN KEY constraint failed", error_msg,
            f"Could not verify Foreign Key constraint for table {table_name} "
            f"due to some other error:\n{error_msg}"
        )


    def assertUniqueTableConstraint(
            self, table_name: str, insert_row_func: Callable[..., None],
            params_tuple: tuple[tuple[Optional[str]], tuple[Optional[str]]],
            num_items: int = 1
    ) -> None:
        """Asserts NOT NULL constraint detected when adding record."""
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
        a_params, b_params = params_tuple
        try:
            column_names = self.getTableColumnNames(table_name)
        except self.DatabaseError as e: # could raise either one
            self.fail(f"Could not assert NOT NULL constraints: {str(e)}")
        # make sure the parameters were programmed in correctly
        a_len = len(a_params)
        assert a_len + 1 == len(column_names) and a_len == len(b_params), (
            "Number of parameters for the insertion call does not match "
            "the number of columns that are supposed to be in the table."
        )
        # make sure exactly num_item item(s) in each parameter list is the same
        matched_items = []
        param_pairs = tuple(zip(a_params, b_params))
        for a, b in param_pairs:
            if a == b:
                matched_items.append((a, b))
        assert len(matched_items) == num_items, (
            f"To check UNIQUE constraints properly, the parameter lists must "
            f"contain exactly {num_items} item(s) in common. The particular "
            f"value being tested should be the common item.\nMatching items: "
            f"{'None' if len(matched_items) == 0 else matched_items}, \n"
            f"First set: {a_params}\nSecond set: {b_params}\n"
            f"{'; '.join(f'{a} != {b}' for a, b in param_pairs)}"
        )
        # get the indices of each match
        column_indices = [
            a_params.index(match[0]) + 1 for match in matched_items
        ]
        columns_tested = tuple(column_names[i] for i in column_indices)
        msg = f"{table_name} [{columns_tested}] UNIQUE constraint fails."
        with self.assertRaises(sqlite3.IntegrityError, msg = msg) as ctx:
            insert_row_func(self.db_path, *a_params)
            insert_row_func(self.db_path, *b_params)
        error_msg = str(ctx.exception)
        self.assertIn("UNIQUE constraint failed", error_msg, msg)

### TEST CASES ###

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
        'Title'     : database.insert_title,
        'Author'    : database.insert_author,
        'Genre'     : database.insert_genre,
        'TitleAuthor' : database.insert_titleauthor,
        'Work'      : None,

        'Format'    : database.insert_format,
        'Publisher' : database.insert_publisher,
        'Publication' : None,

        'Condition' : database.insert_condition,
        'Location'  : None,
        'Book'      : None
    }
    db_create_table_functions = {
        'Title'     : database.create_table_title,
        'Author'    : database.create_table_author,
        'Genre'     : database.create_table_genre,
        'TitleAuthor' : database.create_table_titleauthor,
        'Work'      : None,

        'Format'    : database.create_table_format,
        'Publisher' : database.create_table_publisher,
        'Publication' : None,

        'Condition' : database.create_table_condition,
        'Location'  : None,
        'Book'      : None
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
        self.insert_function = self.getTableFunctionFromDictionary(
            self.table_name, self.db_insert_functions
        )

    def setUp(self) -> None:
        """Create necessary table for testing based on table_name."""
        create_table = self.getTableFunctionFromDictionary(
            self.table_name, self.db_create_table_functions
        )
        create_table(self.db_path)

    def getTableFunctionFromDictionary(
            self: BaseDatabaseModuleTestCase, table_name: str,
            dictionary: dict[str, Callable[[str, tuple[Any, ...]], Any]]
    ) -> Callable[[str, tuple[Any, ...]], Any]:
        if table_name not in dictionary:
            self.fail(
                f"Cannot locate function. '{table_name}' is not defined in "
                f"the provided dictionary. Available keys are: "
                f"{', '.join(dictionary)}"
            )
        func: Callable[[str, tuple[Any, ...]], Any] = (
            dictionary[table_name]
        )
        if func is None:
            self.fail(
                f"'{table_name}' table function has not been added to the "
                f"provided dictionary. Verify function exists and add it to "
                f"the appropriate dictionary."
        )
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
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_title_name_not_unique_constraint(self) -> None:
        """Verifies no unique constraint on title Name field."""
        params_tuple = ((f'Name', ) for _ in range(1, 10))
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_title_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on title Name field."""
        params_tuple = ((None, ), )
        self.assertNotNullTableConstraints(
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
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_genre_name_unique_constraint(self) -> None:
        """Verifies unique constraint on the genre name field."""
        params_tuple = (('Name', ), ('Name', ))
        self.assertUniqueTableConstraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_genre_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on genre name field."""
        params_tuple = ((None, ), )
        self.assertNotNullTableConstraints(
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
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_format_name_unique_constraint(self) -> None:
        """Verifies unique constraint on the format name field."""
        params_tuple = (('Name', ), ('Name', ))
        self.assertUniqueTableConstraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_format_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on format name field."""
        params_tuple = ((None, ), )
        self.assertNotNullTableConstraints(
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
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_publisher_name_unique_constraint(self) -> None:
        """Verifies unique constraint on the publisher name field."""
        params_tuple = (('Name', ), ('Name', ))
        self.assertUniqueTableConstraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_publisher_name_not_null_constraint(self) -> None:
        """Verifies not null constraint on publisher name field."""
        params_tuple = ((None, ), )
        self.assertNotNullTableConstraints(
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
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_condition_name_unique_constraint(self) -> None:
        """Verifies unique constraint on condition name field."""
        params_tuple = (('Name', 'Description1'), ('Name', 'Description2'))
        self.assertUniqueTableConstraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_condition_description_unique_constraint(self) -> None:
        """Verifies unique constraint on condition description field."""
        params_tuple = (('Name1', 'Description'), ('Name2', 'Description'))
        self.assertUniqueTableConstraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_condition_name_and_description_not_null_constraints(self) -> None:
        """
        Verifies not null constraints on condition name and description fields.
        """
        params_tuple = ((None, 'Description'), ('Name', None))
        self.assertNotNullTableConstraints(
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
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_first_not_null_constraint(self) -> None:
        """Verifies not null constraint on author first field."""
        params_tuple = (('Prefix', None, 'Middle', 'Last', 'Suffix'), )
        self.assertNotNullTableConstraints(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_prefix_nullable_constraint(self) -> None:
        """Verifies author prefix field nullable."""
        params_tuple = ((None, 'First', 'Middle', 'Last', 'Suffix'), )
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_middle_nullable_constraint(self) -> None:
        """Verifies author middle field nullable."""
        params_tuple = (('Prefix', 'First', None, 'Last', 'Suffix'), )
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_last_nullable_constraint(self) -> None:
        """Verifies author last field nullable."""
        params_tuple = (('Prefix', 'First', 'Middle', None, 'Suffix'), )
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_tuple
        )

    def test_author_suffix_nullable_constraint(self) -> None:
        """Verifies author suffix field nullable."""
        params_tuple = (('Prefix', 'First', 'Middle', 'Last', None), )
        self.assertCorrectRecordInsertion(
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
            self.getTableFunctionFromDictionary(
                name, self.db_insert_functions
            ) for name in self.referenced_table_names
        )
        self.create_table_funcs = tuple(
            self.getTableFunctionFromDictionary(
                name, self.db_create_table_functions
            ) for name in self.referenced_table_names
        )


    def setUp(self):
        """Creates and populates the necessary tables."""
        # Create the test table.
        super().setUp()
        # Turn foreign keys on.
        self.cursor.execute("PRAGMA foreign_keys = ON")
        # Make all the tables -- establish test environment.
        for create_table_func in self.create_table_funcs:
            create_table_func(self.db_path)
        # Make 5 insertions in each table.
        for i in range(1, 6):
            for table_name, insert_func in zip(
                self.referenced_table_names, self.insert_funcs
            ):
                # Need to know the column names to generate input.
                column_names = self.getTableColumnNames(table_name)
                params = (
                    i if 'ID' in column_name else column_name + str(i)
                    for column_name in column_names[1:] # don't want ID column
                )
                try:
                    insert_func(self.db_path, *params)
                except sqlite3.IntegrityError as e:
                    err_msg = str(e)
                    if "FOREIGN KEY constraint failed" in err_msg:
                        self.fail(
                            "Check the order that table names are defined in "
                            "the referenced_table_names tuple. Referenced "
                            "tables must be listed before dependent tables."
                        )
                    else:
                        self.fail(str(e))
                except Exception as e:
                    self.fail(str(e))


class TitleAuthorTableTestCase(BaseDependentTableTestCase):
    """
    Tests for validating TitleAuthor table function. TitleAuthors must be
    inserted correctly with auto-incrementing ID's. Each (TitleID, AuthorID)
    combination must be unique, and they must have FOREIGN KEY constraints.
    """
    table_name = 'TitleAuthor'
    referenced_table_names = ('Title', 'Author')

    def test_titleauthor_insert_record_creation(self) -> None:
        """Verifies insert_titleauthor() creates a valid record"""
        title_ids = self.getValidRecordIDs('Title')
        author_ids = self.getValidRecordIDs('Author')
        author_ids = author_ids[-1:] + author_ids[:-1]
        params_list = tuple(zip(title_ids, author_ids))
        self.assertCorrectRecordInsertion(
            self.table_name, self.insert_function, params_list
        )

    def test_titleauthor_authorid_titleid_unique_constraint(self) -> None:
        """Verifies the unique constraint of TitleID, AuthorID pairs"""
        title_ids = self.getValidRecordIDs('Title')
        author_ids = self.getValidRecordIDs('Author')
        params_list = next(
            ((t, a), ) * 2 for t in title_ids for a in author_ids if t != a
        )
        #params_list = (param, param)
        self.assertUniqueTableConstraint(
            self.table_name, self.insert_function, params_list, 2
        )

    def test_titleauthor_authorid_foreign_key_constraint(self) -> None:
        title_ids = self.getValidRecordIDs('Title')
        author_ids = self.getValidRecordIDs('Author')
        ids = set(title_ids).union(author_ids)
        invalid_id = next(
            i for i in range(1, len(ids) + 2) if i not in ids
        )
        params_list = ((title_ids[0], invalid_id), )
        self.assertForeignKeyTableConstraint(
            self.table_name, self.insert_function, params_list
        )

    def test_titleauthor_titleid_foreign_key_constraint(self) -> None:
        title_ids = self.getValidRecordIDs('Title')
        author_ids = self.getValidRecordIDs('Author')
        ids = set(title_ids).union(author_ids)
        invalid_id = next(
            i for i in range(1, len(ids) + 2) if i not in ids
        )
        params_list = ((invalid_id, author_ids[0]), )
        self.assertForeignKeyTableConstraint(
            self.table_name, self.insert_function, params_list
        )



class WorkTableTestCase(BaseDependentTableTestCase):
    """
    Tests for validating Work table function. Work must be inserted correctly
    with auto-incrementing ID's. Each (TitleAuthorID, GenreID) combination must
    be unique, and they must have FOREIGN KEY constraints.
    """




    #def setUp(self):









# class TestAuthorsTable(BaseDatabaseModuleTestCase):
#     table_name = 'Authors'

#     def setUp(self):
#         database.create_table_authors(self.db_path)

#     def test_add_author_creates_valid_record(self):
#         """Verifies add_author() creates a valid record."""
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_author,
#             [
#                 ('FIRST', 'M', 'LAST', 'SUFFIX'),
#                 ('first', 'm', 'last', 'suffix'),
#                 ('Bob', None, 'Anderson', None)
#             ]
#         )

#     def test_author_table_not_null_constraints(self):
#         """Test not null constraints in Authors table."""
#         self.assertNotNullTableConstraints(
#             self.table_name, database.add_author,
#             [
#                 (None, 'MiddleName', 'LastName', 'Suffix'),
#                 ('FirstName', 'MiddleName', None, 'Suffix')
#             ]
#         )

# class TestPublishersTable(BaseDatabaseModuleTestCase):
#     table_name = 'Publishers'

#     def setUp(self):
#         database.create_table_publishers(self.db_path)

#     def test_add_publisher_creates_valid_record(self):
#         """Verifies add_publisher() creates a valid record."""
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_publisher, [('NAME', ), ('name', )]
#         )

#     def test_publisher_table_not_null_constraints(self):
#         """Test not null constraints in Publishers table."""
#         self.assertNotNullTableConstraints(
#             self.table_name, database.add_publisher, [(None, )]
#         )

#     def test_publisher_table_name_unique_constraint(self):
#         """Test unique constraints on Publishers Name column."""
#         self.assertUniqueTableConstraint(
#             self.table_name, database.add_publisher, ('NAME', ), ('NAME', )
#         )

# class TestGenresCategoriesTable(BaseDatabaseModuleTestCase):
#     table_name = 'GenresCategories'

#     def setUp(self):
#         database.create_table_genrescategories(self.db_path)

#     def test_add_genrecategory_creates_valid_record(self):
#         """Verifies add_genrecategory() creates a valid record."""
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_genrecategory,
#             [('NAME', ), ('name', )]
#         )

#     def test_genrecategory_table_not_null_constraints(self):
#         """Test not null constraints in GenresCategories table."""
#         self.assertNotNullTableConstraints(
#             self.table_name, database.add_genrecategory, [(None, )]
#         )

#     def test_genrecategory_table_name_unique_constraint(self):
#         """Test unique constraints on GenresCategories Name column."""
#         self.assertUniqueTableConstraint(
#             self.table_name, database.add_genrecategory, ('NAME', ), ('NAME', )
#         )

# class TestConditionsTable(BaseDatabaseModuleTestCase):
#     table_name = 'Conditions'

#     def setUp(self):
#         database.create_table_conditions(self.db_path)

#     def test_add_condition_creates_valid_record(self):
#         """Verifies add_condition() creates a valid record."""
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_condition,
#             [
#                 ('NAME', 'DESCRIPTION'),
#                 ('name', None),
#             ]
#         )

#     def test_condition_table_not_null_constraints(self):
#         """Test not null constraints in Conditions table."""
#         self.assertNotNullTableConstraints(
#             self.table_name, database.add_condition, [(None, 'Hello')]
#         )

#     def test_condition_table_name_unique_constraint(self):
#         """Test unique constraints on Conditions Name column."""
#         self.assertUniqueTableConstraint(
#             self.table_name, database.add_condition,
#             ('NAME', 'description a'), ('NAME', 'description b')
#         )

#     def test_condition_table_description_unique_constraint(self):
#         """Test unique constraints on Conditions Description column."""
#         self.assertUniqueTableConstraint(
#             self.table_name, database.add_condition,
#             ('Name a', 'DESCRIPTION'), ('name b', 'DESCRIPTION')
#         )

# class TestLocationsTable(BaseDatabaseModuleTestCase):
#     table_name = 'Locations'

#     def setUp(self):
#         database.create_table_locations(self.db_path)

#     def test_add_location_creates_valid_record(self):
#         """Verifies add_location() creates a valid record."""
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_location,
#             [
#                 ('NAME', 'DESCRIPTION'),
#                 ('name', None)
#             ]
#         )

#     def test_location_table_not_null_constraints(self):
#         self.assertNotNullTableConstraints(
#             self.table_name, database.add_location, [(None, 'Description')]
#         )

#     def test_location_table_name_unique_constraint(self):
#         """
#         Test unique constraints on Conditions Name column.
#         """
#         self.assertUniqueTableConstraint(
#             self.table_name, database.add_location,
#             ('NAME', 'description a'), ('NAME', 'description b')
#         )

#     def test_location_table_description_unique_constraint(self):
#         """
#         Test unique constraints on Conditions Description column.
#         """
#         self.assertUniqueTableConstraint(
#             self.table_name, database.add_location,
#             ('Name a', 'DESCRIPTION'), ('name b', 'DESCRIPTION')
#         )

# class TestISBNTable(BaseDatabaseModuleTestCase):
#     table_name = 'ISBN'

#     def setUp(self) -> None:
#         """Creates ISBN table and all of its parent tables."""
#         self.createParentTables()
#         database.create_table_isbns(self.db_path)

#     @classmethod
#     def createParentTables(cls):
#         """Creates each ISBN table parent table."""
#         database.create_table_authors(cls.db_path)
#         database.create_table_publishers(cls.db_path)
#         database.create_table_genrescategories(cls.db_path)

#     @classmethod
#     def addNParentRows(cls, n: int) -> None:
#         """Inserts n completely unique rows to each ISBN table parent table"""
#         assert n > 1, (
#             "TestISBNsTable.addParentRows() must insert >= 1 row. Set num >= 1"
#         )
#         for i in range(1, n + 1):
#             database.add_author(
#                 cls.db_path, f"AFirst{i}", f"AMid{i}", f"ALast{i}", f"ASuf{i}"
#             )
#             database.add_publisher(cls.db_path, f"PName{i}")
#             database.add_genrecategory(cls.db_path, f"GCName{i}")

#     @classmethod
#     def getNInsertionParams(cls, n: int = 1) -> tuple[list[int | str]]:
#         """
#         Generates tuple of n lists containing insertion parameters for the
#         ISBNs table. Inserts n sets of parent table records, providing n unique
#         IDs, to allow each parameter list to be completely unique.
#         """
#         assert n > 1, (
#             "TestBooksTable.getNInsertionParameterLists() must return >= 1 "
#             "list. Set num_rows >= 1"
#         )

#         cls.addNParentRows(n)
#         TitleIDs = cls.getValidRecordIDs('Titles')
#         AuthorIDs = cls.getValidRecordIDs('Authors')
#         PublisherIDs = cls.getValidRecordIDs('Publishers')
#         GenreIDs = cls.getValidRecordIDs('GenresCategories')

#         test_params_list = []
#         for i in range(0, n):
#             AuthorID = AuthorIDs[i]
#             PublisherID = PublisherIDs[i]
#             GenreID = GenreIDs[i]

#             count = i + 1
#             params = [
#                 f'ISBN {count}', f'Title {count}', AuthorID, PublisherID,
#                 2000 + count, count, GenreID, f'Desc{1}'
#             ]
#             test_params_list.append(params)

#         return tuple(test_params_list)


#     def test_add_isbn_creates_valid_record(self):
#         """Verifies add_isbn() creates a valid record."""
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_isbn,
#             [
#                 ('ISBN', 'Title', AuthorID, PublisherID, 2000, 1, GenreID, 'desc'),
#                 ('name', None)
#             ]
#         )

# class TestBooksTable(BaseDatabaseModuleTestCase):
#     table_name = 'Books'

#     def setUp(self) -> None:
#         """Creates Books table and all of its parent tables."""
#         database.create_database(self.db_path)

#     def addParentRows(self, num: int = 1) -> None:
#         """Inserts n records into each of the Books table's parent tables."""
#         assert num > 1, (
#             "TestBooksTable.addParentRows() must insert >= 1 row. Set num >= 1"
#         )
#         for i in range(1, num + 1):
#             database.add_condition(self.db_path, f"CName{i}", f"CDesc{i}")
#             database.add_location(self.db_path, f"LName{i}", f"LDesc{i}")

#     def getNInsertionParams(
#             self, num_rows: int = 1
#         ) -> tuple[tuple[int | str | float]]:
#         """
#         Generates tuple of n lists containing insertion parameters for the
#         books table. Inserts n sets of parent table records to allow each
#         parameter list to be completely unique.
#         """
#         assert num_rows > 1, (
#             "TestBooksTable.getNInsertionParameterLists() must return >= 1 "
#             "list. Set num_rows >= 1"
#         )
#         self.addParentRows(num_rows)
#         test_params_list = []
#         for i in range(0, num_rows):
#             ConditionID = self.getValidRecordIDs('Conditions')[i]
#             LocationID = self.getValidRecordIDs('Locations')[i]

#             count = i + 1
#             year = 2000 + count
#             params = (
#                 f'Title {count}', AuthorID, PublisherID, GenreID, year, count,
#                 ConditionID, f'Desc {count}', f'{year}-01-01', count + 0.99,
#                 LocationID, f'ISBN{count}'
#             )
#             test_params_list.append(params)
#         return tuple(test_params_list)


#     def test_add_book_creates_valid_record(self):
#         """Verifies add_book() creates a valid record."""
#         param_lists = self.getNInsertionParameterLists(1)

#         params = (
#             Title, AuthorID, PublisherID,
#             GenreID, YearPublished, Edition,
#             ConditionID, Description, DateAcquired,
#             Price, LocationID, isbn
#         )
#         self.assertCorrectRecordInsertion(
#             self.table_name, database.add_book, [params, params]
#         )

#     def test_books_table_foreign_key_constraints(self):
#         """Test foreign key constraints in Books table."""
#         parameters = (
#             "Title", 999, 999, 999, 2000, 1, 999,
#             "Description", "2000-01-01", 9.99, 999, "ISBN"
#         )
#         # Turn foreign key functionality on
#         self.cursor.execute("PRAGMA foreign_keys = ON")
#         with self.assertRaises(sqlite3.IntegrityError) as context:
#             self.cursor.execute(
#                 '''
#                 INSERT INTO Books (Title, AuthorID, PublisherID, GenreID,
#                 YearPublished, Edition, ConditionID, Description, DateAcquired,
#                 Price, LocationID, ISBN)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 ''',
#                 parameters
#             )
#         error_message = str(context.exception)
#         self.assertIn(
#             "FOREIGN KEY constraint failed", error_message,
#             "AuthorID, PublisherID, GenreID, ConditionID, and LocationID "
#             "should have FOREIGN KEY constraints"
#         )

#     def test_book_table_not_null_constraints(self):

#         AuthorID = self.getValidRecordIDs('Authors')[0]
#         PublisherID = self.getValidRecordIDs('Publishers')[0]
#         GenreID = self.getValidRecordIDs('GenresCategories')[0]
#         ConditionID = self.getValidRecordIDs('Conditions')[0]
#         LocationID = self.getValidRecordIDs('Locations')[0]

#         YearPublished = 2000
#         Edition = 1
#         DateAcquired = '2020-01-01'
#         Description = 'Description'
#         Price = 9.99
#         isbn = 'ISBN'

#         self.assertNotNullTableConstraints(
#             self.table_name, database.add_book,
#             [
#                 (None, AuthorID, PublisherID,
#                 GenreID, YearPublished, Edition,
#                 ConditionID, Description, DateAcquired,
#                 Price, LocationID, isbn),
#                 ('Title', None, PublisherID,
#                 GenreID, YearPublished, Edition,
#                 ConditionID, Description, DateAcquired,
#                 Price, LocationID, isbn)
#             ]
#         )

#     def test_book_table_isbn_unique_constraints(self):
#         """Test unique constraints on Conditions Name and Description columns."""
#         # self.assertUniqueTableConstraint(
#         #     self.table_name, database.add_publisher,
#         #     ('Title', None, PublisherID, GenreID, YearPublished, Edition,
#         #     ConditionID, Description, DateAcquired, Price, LocationID, isbn
#         #     ),
#         #     ('Title', None, PublisherID, GenreID, YearPublished, Edition,
#         #     ConditionID, Description, DateAcquired, Price, LocationID, isbn
#         #     )
#         # )
#     # ISBN

# class TestCreateDatabaseModuleFunction(BaseDatabaseModuleTestCase):

#     def test_create_database_function(self):
#         database.create_database(self.db_path)
#         existing_tables = self.getDatabaseTableNames()
#         db_schema = {}
#         for table_name in existing_tables:
#             column_names = self.getTableColumnNames(table_name)
#             db_schema[table_name] = column_names
#         self.assertDictEqual(
#             self.db_required_tables, db_schema,
#             "\n\nFAILURE: Database schema is incorrect!"
#         )

def __disjoint_union(*iterables: Iterable) -> set[str]:
    """
    Takes an arbitrary number of iterables and subtracts their intersection
    from their union. This is specifically used to assert that all of the
    dictionary keys match and provide a warning if something is wrong with one
    of the dictionary definitions.
    """
    union = set().union(*iterables)
    intersection = set(iterables[0]).intersection(*iterables[1:])
    disjoint_union = union.symmetric_difference(intersection)
    return disjoint_union

import inspect, sys


__classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
__db_dictionaries = [
    m
    for (_, class_obj) in __classes
    for m in inspect.getmembers(class_obj)
    if m[0].startswith('db_') and type(m[1]) == dict
]
__dict_names = set([d[0] for d in __db_dictionaries])
__db_dictionaries = [d[1] for d in __db_dictionaries]

__mismatched_dictionary_items = __disjoint_union(*__db_dictionaries)

assert len(__mismatched_dictionary_items) == 0, (
    f"""
    Keys representing database tables are mismatched across locations.
    Verify keys are correct.\nMismatched keys: {__mismatched_dictionary_items}
    \nDictionary names: {__dict_names}
    """
)

### ENTRY POINT ###

if __name__ == '__main__':
    unittest.main()