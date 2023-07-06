"""Initiates test runs using python -m tests"""
import unittest

import tests
from tests.database_tests import test_database_tables
from tests.database_tests import test_database_tables_dependent


def run_all_tests():
    """Loads all the tests and runs them."""
    loader = unittest.TestLoader()
    test_suite = loader.loadTestsFromModule(test_database_tables)
    test_suite.addTests(
        loader.loadTestsFromModule(
            test_database_tables_dependent
        )
    )

    runner = unittest.TextTestRunner(verbosity=tests.TESTRUNNER_VERBOSITY)
    runner.run(test_suite)


# ## TEST ENTRYPOINT ## #
if __name__ == '__main__':
    run_all_tests()
