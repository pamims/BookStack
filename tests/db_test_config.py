"""Config file holds dictionaries used in TestCase classes."""

import inspect
import sys

from typing import Iterable
from source import database

DB_DICT_SCHEMA = {
        'Title': ['ID', 'Name'],
        'Author': ['ID', 'Prefix', 'First', 'Middle', 'Last', 'Suffix'],
        'Genre': ['ID', 'GenreID', 'Name'],
        'TitleAuthor': ['ID', 'TitleID', 'AuthorID'],
        'Work': ['ID', 'TitleAuthorID', 'GenreID'],

        'Format': ['ID', 'Name'],
        'Publisher': ['ID', 'Name'],
        'Publication': ['ID', 'WorkID', 'FormatID', 'PublisherID', 'ISBN'],

        'Condition': ['ID', 'Name', 'Description'],
        'Location': ['ID', 'LocationID', 'Name', 'Description'],
        'Book': ['ID', 'PublicationID', 'ConditionID', 'LocationID']
    }

DB_DICT_INSERT_FUNCS = {
        'Title': database.insert_title,
        'Author': database.insert_author,
        'Genre': database.insert_genre,
        'TitleAuthor': database.insert_titleauthor,
        'Work': database.insert_work,

        'Format': database.insert_format,
        'Publisher': database.insert_publisher,
        'Publication': database.insert_publication,

        'Condition': database.insert_condition,
        'Location': database.insert_location,
        'Book': database.insert_book
    }

DB_DICT_CREATE_TABLE_FUNCS = {
        'Title': database.create_table_title,
        'Author': database.create_table_author,
        'Genre': database.create_table_genre,
        'TitleAuthor': database.create_table_titleauthor,
        'Work': database.create_table_work,

        'Format': database.create_table_format,
        'Publisher': database.create_table_publisher,
        'Publication': database.create_table_publication,

        'Condition': database.create_table_condition,
        'Location': database.create_table_location,
        'Book': database.create_table_book
    }


def __disjoint_union(*iterables: Iterable) -> set[str]:
    """
    Takes an arbitrary number of iterables and subtracts their intersection
    from their union. This is specifically used to assert that all of the
    dictionary keys match and provide a warning if something is wrong with one
    of the dictionary definitions -- if disjoint union is 0, assertion passes.
    """
    if len(iterables) == 0:
        return {}  # return an empty sets
    union = set().union(*iterables)
    intersection = set(iterables[0]).intersection(*iterables[1:])
    disjoint_union = union.symmetric_difference(intersection)
    return disjoint_union


# ## Assert dictionaries match ## #
__members = inspect.getmembers(
    sys.modules[__name__], lambda x: isinstance(x, dict)
)
__db_dictionaries = [m[1] for m in __members if m[0].startswith('DB_DICT_')]

__mismatched_dictionary_items = __disjoint_union(*__db_dictionaries)
assert len(__mismatched_dictionary_items) == 0, (
    f"""
    Keys representing database tables are mismatched across locations.
    Verify keys are correct.\nMismatched keys: {__mismatched_dictionary_items}
    """
)
