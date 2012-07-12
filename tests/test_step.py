try:
    from unittest2 import TestCase
    TestCase  # silency pyflakes
except ImportError:
    from unittest import TestCase

from mrjob.step import MRJobStep


def identity_mapper(k, v):
    yield k, v


def identity_reducer(k, vals):
    for v in vals:
        yield k, v


class MRJobStepInitTestCase(TestCase):

    ### Basic behavior ###

    def test_nothing_specified(self):
        self.assertRaises(ValueError, MRJobStep)

    def _test_explicit(self, m=False, c=False, r=False, **kwargs):
        s = MRJobStep(**kwargs)
        self.assertEqual(s.has_explicit_mapper, m)
        self.assertEqual(s.has_explicit_combiner, c)
        self.assertEqual(s.has_explicit_reducer, r)

    # normal

    def test_explicit_mapper(self):
        self._test_explicit(mapper=identity_mapper, m=True)

    def test_explicit_combiner(self):
        self._test_explicit(combiner=identity_reducer, c=True)

    def test_explicit_reducer(self):
        self._test_explicit(reducer=identity_reducer, r=True)

    # final

    def test_explicit_mapper_final(self):
        self._test_explicit(mapper_final=identity_mapper, m=True)

    def test_explicit_combiner_final(self):
        self._test_explicit(combiner_final=identity_reducer, c=True)

    def test_explicit_reducer_final(self):
        self._test_explicit(reducer_final=identity_reducer, r=True)

    # init

    def test_explicit_mapper_init(self):
        self._test_explicit(mapper_init=identity_mapper, m=True)

    def test_explicit_combiner_init(self):
        self._test_explicit(combiner_init=identity_reducer, c=True)

    def test_explicit_reducer_init(self):
        self._test_explicit(reducer_init=identity_reducer, r=True)

    # cmd

    def test_explicit_mapper_cmd(self):
        self._test_explicit(mapper_cmd='cat', m=True)

    def test_explicit_combiner_cmd(self):
        self._test_explicit(combiner_cmd='cat', c=True)

    def test_explicit_reducer_cmd(self):
        self._test_explicit(reducer_cmd='cat', r=True)

    # filter

    def test_explicit_mapper_filter(self):
        self._test_explicit(mapper_filter='cat', m=True)

    def test_explicit_combiner_filter(self):
        # combiners aren't currently allowed to have filters
        self.assertRaises(TypeError, MRJobStep, combiner_filter='cat')

    def test_explicit_reducer_filter(self):
        self._test_explicit(reducer_filter='cat', r=True)

    ### Conflicts ###

    def _test_conflict(self, **kwargs):
        self.assertRaises(ValueError, MRJobStep, **kwargs)

    def test_conflict_mapper(self):
        self._test_conflict(mapper_cmd='cat', mapper=identity_mapper)

    def test_conflict_mapper_init(self):
        self._test_conflict(mapper_cmd='cat', mapper_init=identity_mapper)

    def test_conflict_mapper_final(self):
        self._test_conflict(mapper_cmd='cat', mapper_final=identity_mapper)
