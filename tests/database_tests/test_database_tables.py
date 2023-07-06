"""Test cases for tables that don't depend on referenced tables."""

import inspect
from typing import Any, Callable

from tests.database_tests import DB_DICT_INSERT_FUNCS
from tests.database_tests import DB_DICT_CREATE_TABLE_FUNCS
from tests.database_tests.base_db_testcase import BaseDatabaseModuleTestCase


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
    db_insert_functions = DB_DICT_INSERT_FUNCS
    db_create_table_functions = DB_DICT_CREATE_TABLE_FUNCS
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


class LocationTableTestCase(BaseTableTestCase):
    """
    Tests for validating Location table function. Locations must be inserted
    correctly with auto-incrementing ID's. Location names must be NOT NULL and
    UNIQUE. Descriptions must be NULLABLE.
    """
    table_name = 'Location'

    def test_location_insert_record_function(self) -> None:
        """Verifies insert_location() creates a valid record."""
        params_tuple = ((f'Name{i}', f'Description{i}') for i in range(1, 10))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )

    def test_location_name_unique_constraint(self) -> None:
        """Verifies unique constraint on location name field."""
        params_tuple = (('Name', 'Description1'), ('Name', 'Description2'))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_tuple
        )

    def test_location_name_not_null_constraint(self) -> None:
        """
        Verifies not null constraints on condition name and description fields.
        """
        params_tuple = ((None, 'Description'), )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_tuple
        )

    def test_location_description_nullable(self) -> None:
        """Verifies location description is nullable."""
        params_tuple = (('Name', None), )
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_tuple
        )
