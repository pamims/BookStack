"""Test cases for tables that depend on referenced tables."""

import sqlite3

from tests.test_database_tables import BaseTableTestCase


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


class PublicationTableTestCase(BaseDependentTableTestCase):
    """
    Tests for validating Publication table function. Publication must be
    inserted correctly with auto-incrementing ID's.
    (WorkID, FormatID, PublisherID) must be UNIQUE
    ISBN must be UNIQUE
    WorkID, FormatID, PublisherID must be FOREIGN KEYs
    WorkID, FormatID must be NOT NULL
    PulbisherID, ISBN must be NULLABLE
    """
