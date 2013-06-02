#!/usr/bin/python3
"""Counts the frequencies of words in a document, and doubles the count just
for kicks. Works with Python 2.7 and Python 3.2+.

Usage:

    python -m mrjob.launch wfc.py -r local <input files>
"""
from __future__ import print_function

import argparse
import itertools
import json
import re
import sys


WORD_RE = re.compile(r"[\w']+")


# Tasks are defined as functions that take an input file object and an output
# file object. In most cases, each line in the input file is of the form:
# key\tvalue\n
# This behavior may be different depending on things you set up in Hadoop and
# mrjob, but it is the default, so don't worry about it.
# **probable exception: first mapper gets raw lines from input file. I could
# be wrong about this though, haven't tested with actual Hadoop yet.**
# Output lines should be written in the same way.


def _write(stdout, key, value):
    stdout.write('%s\t%s\n' % (key, value))


def _group_by_key(in_file, sep='\t'):
    """Turn this:
        ['x\ta', 'x\tb', 'y\tc']
    into this:
        [('x', ['a', 'b']), ('y', ['c'])]
    """
    group_key = lambda line: line.split(sep, 1)[0]
    return itertools.groupby(in_file, key=group_key)


def lines_to_word_occurrences(in_file, stdout):
    """For each line of input, output (word, 1) for each word in the line"""
    for line in in_file:
        for word in WORD_RE.findall(line):
            _write(stdout, word, 1)


def sum_word_occurrences(in_file, stdout):
    """Group input lines by key and output (key, sum(values))"""
    for word, lines in _group_by_key(in_file):
        value = sum(int(line.split('\t', 1)[1]) for line in lines)
        _write(stdout, word, value)


def multiply_value_by_2(in_file, stdout):
    """Emit (key, 2*value) for each line in in_file"""
    for line in in_file:
        key, value = line.split('\t', 1)
        _write(stdout, key, 2 * int(value))


def _run_task(task, paths, stdin, stdout):
    """Run *task* for each file in *paths*. Use stdin if '-' is an arg or there
    are no args.
    """
    for path in (paths or ['-']):
        if path == '-':
            task(stdin, stdout)
        else:
            with open(path, 'r') as f:
                task(f, stdout)


def main(argv, stdin, stdout, stderr):
    p = argparse.ArgumentParser()
    p.add_argument('--steps', default=False, action='store_true')
    p.add_argument('--mapper', default=False, action='store_true')
    p.add_argument('--reducer', default=False, action='store_true')
    p.add_argument('--step-num', default=None, type=int)
    p.add_argument('files', nargs='*')

    opts = p.parse_args(argv)
    args = opts.files
    
    # --steps behavior. This job has 2 steps, the first with a mapper and
    # reducer and the second with only a mapper. They are all 'script' steps,
    # meaning that they are run by invoking this file with --step-num=X and
    # [--mapper|--reducer].
    # The output of --steps tells mrjob what steps the job has.
    if opts.steps:
        if any((opts.mapper, opts.reducer, opts.step_num)):
            print('--steps is mutually exclusive with all other options.',
                  file=stderr)
        print(
            json.dumps([
                {'type': 'streaming',
                 'mapper': {'type': 'script'},
                 'reducer': {'type': 'script'}},
                {'type': 'streaming',
                 'mapper': {'type': 'script'}}]),
            file=stdout)
        return 0

    # --step-num is required if --steps not present
    if opts.step_num is None:
        print('You must specify --step-num if not using --steps.',
              file=stderr)
        return 1

    # likewise for [--mapper|--reducer]
    if ((opts.mapper and opts.reducer) or
        (not opts.mapper and not opts.reducer)):
        print (
            'You must specify exactly one of either --mapper or --reducer'
            ' if not using --steps.',
            file=stderr)
        return 1

    # decide which mapper to run based on --step-num
    if opts.mapper:
        if opts.step_num == 0:
            _run_task(lines_to_word_occurrences, args, stdin, stdout)
            return 0
        elif opts.step_num == 1:
            _run_task(multiply_value_by_2, args, stdin, stdout)
            return 0
        else:
            print('There is no step %d mapper!' % opts.step_num, file=stderr)
            return 1

    # run reducer if --step-num is correct
    if opts.reducer:
        if opts.step_num == 0:
            _run_task(sum_word_occurrences, args, stdin, stdout)
            return 0
        else:
            print('There is no step %d reducer!' % opts.step_num, file=stderr)
            return 1

    raise Exception("How did we get here???")


if __name__ == '__main__':
    # invoke with sys.argv, etc. Test cases might use different values.
    sys.exit(main(None, sys.stdin, sys.stdout, sys.stderr))
