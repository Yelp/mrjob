"""Module for loading all tests, so we can run them from setup.py"""
import os

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


def load_tests():
    return unittest.defaultTestLoader.discover(os.path.dirname(__file__))
