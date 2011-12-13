"""Convert testify decorators to unittest2. This assumes:

- You use 4 spaces for indentation
- Imports look like: from testify import <one thing>
- You use a subset of testify asserts (easy to add to)
- You don't use testify.run() (would be easy to add)

This also doesn't handle blockquotes correctly.

And doubtless some other stuff.
"""
from cStringIO import StringIO
from collections import defaultdict
from optparse import OptionParser
import re
import sys


CLASS_RE = re.compile(r'class\s+(\w+)\((.*)\):')

DEF_RE = re.compile(r'\s*def (\w+)')

TESTIFY_IMPORT_RE = re.compile(r'^from testify import (\w+)$')

ASSERT_RE = re.compile(r'assert_\w+')

DECORATOR_MAP = {
    '@setup': 'setUp',
    '@teardown': 'tearDown',
    '@class_setup': 'setUpClass',
    '@class_teardown': 'tearDownClass',
}


ASSERT_MAP = {
    'assert_equal': 'assertEqual',
    'assert_gt': 'assertGreater',
    'assert_gte': 'assertGreaterEqual',
    'assert_in': 'assertIn',
    'assert_lt': 'assertLess',
    'assert_lte': 'assertLessEqual',
    'assert_not_equal': 'assertNotEqual',
    'assert_not_in': 'assertNotIn',
    'assert_not_reached': 'fail',
    'assert_raises': 'assertRaises',
}

TESTIFY_IMPORTS_TO_KEEP = set(['TestCase'])


def main():
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()

    if args:
        for path in args:
            with open(path) as input:
                if options.inline:
                    output = StringIO()
                else:
                    output = sys.stdout
                process_file(input, output)

            if options.inline:
                with open(path, 'w') as output_file:
                    output_file.write(output.getvalue())
    else:
        process_file(sys.stdin, sys.stdout)


def make_option_parser():
    usage = '%prog [options] [files]'
    description = ('Translate testify -> unittest2')
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        '-i', '--inline', dest='inline', default=False, action='store_true',
        help='Overwrite existing files')

    return option_parser


def process_file(input, output):
    # lines for the current class we're in
    current_class_lines = None

    for line in input:
        if line.strip() and not line.startswith(' '):
            if current_class_lines is not None:
                process_class(current_class_lines, output)
                current_class_lines = None

            if CLASS_RE.match(line):
                current_class_lines = []

        if current_class_lines is None:
            # imports of asserts, etc. need not be saved
            m = TESTIFY_IMPORT_RE.match(line)
            if m:
                name = m.group(1)
                if name in TESTIFY_IMPORTS_TO_KEEP:
                    output.write('from unittest2 import %s\n' % name)
                # otherwise just delete it
            else:
                output.write(line)
        else:
            current_class_lines.append(line)

    if current_class_lines is not None:
        process_class(current_class_lines, output)


def process_class(lines, output):
    m = CLASS_RE.match(lines[0])
    assert m
    class_name, parents = m.groups()

    setup_methods = defaultdict(list)
    method_type = None
    # find setup and teardown methods
    for line in lines:
        if line.strip() in DECORATOR_MAP:
            method_type = DECORATOR_MAP[line.strip()]
            continue

        if method_type:
            m = DEF_RE.match(line)
            if m:
                method_name = m.group(1)
                setup_methods[method_type].append(method_name)
                method_type = None

    # output class
    output.write(lines[0])  # class definition

    # setup/teardown methods
    for method_type, method_names in sorted(setup_methods.iteritems()):
        output.write('\n')

        if 'Class' in method_type:
            output.write('    @classmethod\n')
            cls_or_self = 'cls'
        else:
            cls_or_self = 'self'
        
        output.write('    def %s(%s):\n' % (method_type, cls_or_self))

        if parents != 'TestCase':
            output.write('        super(%s, %s).%s()\n' %
                         (class_name, cls_or_self, method_type))

        if 'tearDown' in method_type:
            method_names = reversed(method_names)

        for method_name in method_names:
            output.write('        %s.%s()\n' % (cls_or_self, method_name))

    # print lines of the class, with a wee bit of filtering
    for line in lines[1:]:

        # strip out decorators
        if line.strip() in DECORATOR_MAP:
            if 'class' in line:
                output.write('    @classmethod\n')
            continue

        # translate asserts. leave asserts we don't recognize as-is
        def translate_assert(m):
            name = m.group(0)
            if name in ASSERT_MAP:
                return 'self.%s' % ASSERT_MAP[name]
            else:
                return name
            
        line = ASSERT_RE.sub(translate_assert, line)
        output.write(line)


if __name__ == '__main__':
    main()
