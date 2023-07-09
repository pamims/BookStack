"""Grabs arguments and runs the program."""

from source.arg_parser import BookyParser

if __name__ == '__main__':
    booky = BookyParser()
    booky.parse_args()
