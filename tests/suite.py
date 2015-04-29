"""Module for loading all tests, so we can run them from setup.py"""
import sys
from os.path import dirname

# this module has to stand alone, so we can't use tests.py2
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest


def load_tests():
    return unittest.defaultTestLoader.discover(dirname(__file__))
