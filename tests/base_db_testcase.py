"""Test cases for database.py"""

import os
import sqlite3
from typing import Callable, Iterable, Optional, Tuple
import unittest

from tests import db_test_config


class BaseDatabaseModuleTestCase(unittest.TestCase):
    """Base test case for all database tests."""
    db_path = 'test_book_stack_database.db'
    db_required_tables = db_test_config.DB_DICT_SCHEMA

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
