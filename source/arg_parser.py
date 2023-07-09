"""Code for the argument parser."""

from argparse import ArgumentParser, _SubParsersAction

URL = 'github'


class BookyParser():
    """The argument parser for the booky application."""
    def __init__(self):
        self.parser = self._create_parser()

    @staticmethod
    def _create_parser() -> ArgumentParser:
        """Builds the parser."""
        parser = ArgumentParser(
            prog='booky',
            description='A book management application.',
            epilog=f'For more information, visit the website at {URL}'
        )

        BookyParser._add_booky_commands(parser)

        return parser

    @staticmethod
    def _add_booky_commands(parser: ArgumentParser) -> None:
        """Adds commands to the booky parser."""
        sub_parsers = parser.add_subparsers(
            title='Database Commands',
            description='Commands for working with your book database.',
            prog='booky',
            dest='command',
            help='aquire and discard book records or explore your database.',
            metavar='COMMAND'
        )

        database_parent_command = BookyParser._make_database_command_parent()

        BookyParser._add_grab_command(sub_parsers, database_parent_command)
        BookyParser._add_toss_command(sub_parsers, database_parent_command)
        BookyParser._add_find_command(sub_parsers, database_parent_command)

    @staticmethod
    def _make_database_command_parent() -> ArgumentParser:
        parser = ArgumentParser(add_help=False)
        parser.add_argument(
            '-a', '--author',
            help='the author of the book.',
            metavar='AUTHOR',
            dest='author'
        )
        parser.add_argument(
            '-t', '--title',
            help='the title of the book.',
            metavar='TITLE',
            dest='title'
        )
        parser.add_argument(
            '-i', '--isbn',
            help='the isbn-10 of the book.',
            metavar='ISBN-10',
            dest='isbn_10'
        )
        parser.add_argument(
            '-I', '-ISBN',
            help='the isbn-13 of the book.',
            metavar='ISBN-13',
            dest='isbn_13'
        )
        return parser

    @staticmethod
    def _add_grab_command(
            sub_parsers: _SubParsersAction,
            parent: ArgumentParser
            # pylint: disable=unsubscriptable-object
    ) -> None:
        """Adds the grab command to booky's parser."""
        parser = sub_parsers.add_parser(
            name='grab',
            help='add a record to the book database.',
            prog='booky grab',
            description='Adds a record to the database.',
            epilog=(
                'At least one argument needs to be included in this command.'
            ),
            parents=[parent]
        )
        return parser

    @staticmethod
    def _add_toss_command(
            sub_parsers: _SubParsersAction,
            parent: ArgumentParser
            # pylint: disable=unsubscriptable-object
    ) -> None:
        """Adds the toss command to booky's parser."""
        parser = sub_parsers.add_parser(
            name='toss',
            help='remove a record from the book database.',
            prog='booky toss',
            description='Removes a record from the database.',
            epilog=(
                'At least one argument needs to be included in this command.'
            ),
            parents=[parent]
        )
        return parser

    @staticmethod
    def _add_find_command(
            sub_parsers: _SubParsersAction,
            parent: ArgumentParser
            # pylint: disable=unsubscriptable-object
    ) -> None:
        """Adds the search command to booky's parser."""
        parser = sub_parsers.add_parser(
            name='find',
            help='locate a record in the book database.',
            prog='booky find',
            description='Finds a record in the database.',
            epilog=(
                'At least one argument needs to be included in this command.'
            ),
            parents=[parent]
        )
        return parser

    def parse_args(self):
        """Handles arguments."""
        args = self.parser.parse_args()

        args_dict = vars(args)

        for key, val in args_dict.items():
            print(f'{key}: {val}')
