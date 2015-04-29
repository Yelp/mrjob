"""Compatilibity layer for test code. Handles the following issues:

Need to use unittest2 rather than unittest in Python 2.6 only.

mock is built in to Python 3.

Don't import directly from unittest/mock; use this module instead.
"""
import sys

# unittest2 is a backport of unittest in Python 2.7
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

TestCase = unittest.TestCase
TestCase  # quiet pyflakes

# mock is built into unittest in Python 3
if sys.version_info < (3, 0):
    import mock
else:
    from unittest import mock

MagicMock = mock.MagicMock
MagicMock

Mock = mock.Mock
Mock

call = mock.call
call

patch = mock.patch
patch

skipIf = unittest.skipIf
skipIf
