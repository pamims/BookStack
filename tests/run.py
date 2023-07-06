"""Program runs the test suite."""

import unittest

from tests import (test_database_tables, test_database_dependent_tables)


def run_all_tests():
    """Loads all the tests and runs them."""
    loader = unittest.TestLoader()
    test_suite = loader.loadTestsFromModule(test_database_tables)
    test_suite.addTests(
        loader.loadTestsFromModule(
            test_database_dependent_tables
        )
    )

    unittest.TextTestRunner(verbosity=1).run(test_suite)


if __name__ == '__main__':
    run_all_tests()
