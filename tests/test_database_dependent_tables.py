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
        title_ids = self.get_valid_record_ids('Title')
        author_ids = self.get_valid_record_ids('Author')
        author_ids = author_ids[-1:] + author_ids[:-1]
        params_list = tuple(zip(title_ids, author_ids))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_list
        )

    def test_titleauthor_authorid_titleid_unique_constraint(self) -> None:
        """Verifies the unique constraint of TitleID, AuthorID pairs."""
        title_ids = self.get_valid_record_ids('Title')
        author_ids = self.get_valid_record_ids('Author')
        params_list = next(
            ((t, a), ) * 2 for t in title_ids for a in author_ids if t != a
        )
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_list, 2
        )

    def test_titleauthor_authorid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of authorid."""
        title_ids = self.get_valid_record_ids('Title')
        author_ids = self.get_valid_record_ids('Author')
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
        title_ids = self.get_valid_record_ids('Title')
        author_ids = self.get_valid_record_ids('Author')
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

    def test_work_insert_record_creation(self) -> None:
        """Verifies insert_work() creates a valid record."""
        ta_ids = self.get_valid_record_ids('TitleAuthor')
        genre_ids = self.get_valid_record_ids('Genre')
        genre_ids = genre_ids[-1:] + genre_ids[:-1]
        params_list = tuple(zip(ta_ids, genre_ids))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_list
        )

    def test_work_titleauthorid_genreid_unique_constraint(self) -> None:
        """Verifies the unique constraint of TitleAuthorID, GenreID pairs."""
        ta_ids = self.get_valid_record_ids('TitleAuthor')
        genre_ids = self.get_valid_record_ids('Genre')
        params_list = next(
            ((ta, g), ) * 2 for ta in ta_ids for g in genre_ids if ta != g
        )
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_list, 2
        )

    def test_work_genreid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of genreid."""
        ta_ids = self.get_valid_record_ids('TitleAuthor')
        genre_ids = self.get_valid_record_ids('Genre')
        ids = set(ta_ids).union(genre_ids)
        invalid_id = next(i for i in range(1, len(ids) + 2) if i not in ids)
        params_list = ((ta_ids[0], invalid_id), )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )

    def test_work_titleauthorid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key constraint of titleauthorid."""
        ta_ids = self.get_valid_record_ids('TitleAuthor')
        genre_ids = self.get_valid_record_ids('Genre')
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

    table_name = 'Publication'
    referenced_table_names = (
        'Title', 'Author', 'TitleAuthor',
        'Genre', 'Work', 'Format', 'Publisher'
    )

    def test_publication_insert_record_creation(self) -> None:
        """Verifies insert_publication() creates a valid record."""
        work_ids = self.get_valid_record_ids('Work')
        fmt_ids = self.get_valid_record_ids('Format')
        fmt_ids = fmt_ids[-1:] + fmt_ids[:-1]
        pub_ids = self.get_valid_record_ids('Publisher')
        pub_ids = pub_ids[-2:] + pub_ids[:-2]
        isbns = (f'ISBN-{i}' for i in range(1, len(pub_ids) + 2))
        params_list = tuple(zip(work_ids, fmt_ids, pub_ids, isbns))
        self.assert_record_insertion_correct(
            self.table_name, self.insert_function, params_list
        )

    def test_publication_workid_fmtid_pubid_unique_constraint(self) -> None:
        """Verifies the unique constraint of (WorkID, FormatID, PublisherID)"""
        w_ids = self.get_valid_record_ids('Work')
        f_ids = self.get_valid_record_ids('Format')
        p_ids = self.get_valid_record_ids('Publisher')
        unique_list = next(
            ((w, f, p), ) * 2
            for w in w_ids for f in f_ids for p in p_ids
            if w != f and f != p and p != w
        )
        isbn_list = tuple((f'ISBN-{i}', ) for i in range(1, 3))
        params_list = (unique_list[i] + isbn_list[i] for i in range(2))
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_list, 3
        )

    def test_publication_isbn_unique_constraint(self) -> None:
        """Verifies the unique constraint of ISBN."""
        w_ids = self.get_valid_record_ids('Work')
        f_ids = self.get_valid_record_ids('Format')
        f_ids = f_ids[-1:] + f_ids[:-1]
        p_ids = self.get_valid_record_ids('Publisher')
        p_ids = p_ids[-2:] + p_ids[:-2]
        params_list = tuple(
            (w, f, p, "ISBN-0") for w, f, p in (w_ids, f_ids, p_ids)
        )
        self.assert_unique_table_constraint(
            self.table_name, self.insert_function, params_list
        )

    def test_publication_workid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key contraint of workid."""
        w_ids = self.get_valid_record_ids('Work')
        f_ids = self.get_valid_record_ids('Format')
        p_ids = self.get_valid_record_ids('Publisher')
        ids = set(w_ids).union(f_ids, p_ids)
        invalid_id = next(i for i in range(1, len(ids) + 2) if i not in ids)
        params_list = next(
            ((invalid_id, f, p, 'ISBN-1'), ) for f, p in (f_ids, p_ids)
        )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )

    def test_publication_formatid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key contraint of formatid."""
        w_ids = self.get_valid_record_ids('Work')
        f_ids = self.get_valid_record_ids('Format')
        p_ids = self.get_valid_record_ids('Publisher')
        ids = set(w_ids).union(f_ids, p_ids)
        invalid_id = next(i for i in range(1, len(ids) + 2) if i not in ids)
        params_list = next(
            ((w, invalid_id, p, 'ISBN-1'), ) for w, p in (w_ids, p_ids)
        )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )

    def test_publication_publisherid_foreign_key_constraint(self) -> None:
        """Verifies the foreign key contraint of publisherid."""
        w_ids = self.get_valid_record_ids('Work')
        f_ids = self.get_valid_record_ids('Format')
        p_ids = self.get_valid_record_ids('Publisher')
        ids = set(w_ids).union(f_ids, p_ids)
        invalid_id = next(i for i in range(1, len(ids) + 2) if i not in ids)
        params_list = next(
            ((w, f, invalid_id, 'ISBN-1'), ) for w, f in (w_ids, f_ids)
        )
        self.assert_foreign_key_constraints(
            self.table_name, self.insert_function, params_list
        )

    def test_publication_workid_formatid_not_null_constraints(self) -> None:
        """Verifies the not null constraints of workid and publisherid"""
        w_ids = self.get_valid_record_ids('Work')
        f_ids = self.get_valid_record_ids('Format')
        p_ids = self.get_valid_record_ids('Publisher')
        params_list = (
            (None, f_ids[0], p_ids[0], 'ISBN1'),
            (w_ids[1], f_ids[1], None, 'ISBN2')
        )
        self.assert_not_null_table_constraints(
            self.table_name, self.insert_function, params_list
        )
