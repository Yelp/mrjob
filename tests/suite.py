"""Module for loading all tests, so we can run them from setup.py"""
import os

from tests.py2 import unittest

def load_tests():
    return unittest.defaultTestLoader.discover(os.path.dirname(__file__))
