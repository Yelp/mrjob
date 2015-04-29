# Copyright 2009-2013 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys

import mrjob

try:
    from setuptools import setup
    setup  # quiet "redefinition of unused ..." warning from pyflakes
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'install_requires': [
            'filechunkio',
            'PyYAML',
            'simplejson>=2.0.9',
        ],
        'provides': ['mrjob'],
        'test_suite': 'tests.suite.load_tests',
        'zip_safe': False,  # so that we can bootstrap mrjob
    }

    # mock is included in Python 3 as unittest.mock
    if sys.version_info < (3, 0):
        setuptools_kwargs['tests_require'] = ['mock']

        # unittest2 is a backport of unittest from Python 2.7
        if sys.version_info < (2, 7):
            setuptools_kwargs['tests_require'].append('unittest2')

    # boto
    if sys.version_info < (3, 0):
        # Officially, the 0.4.x series of mrjob supports boto back to v2.2.0
        setuptools_kwargs['install_requires'].append('boto>=2.2.0')
    else:
        # boto didn't support Python 3 until v2.32.1. Since Python 3 support
        # is new as of mrjob v0.4.5, there's no backward compatibility issue,
        # so we might as well require the latest version of boto.
        setuptools_kwargs['install_requires'].append('boto>=2.38.0')

except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}

setup(
    author='David Marin',
    author_email='dm@davidmarin.org',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Distributed Computing',
    ],
    description='Python MapReduce framework',
    license='Apache',
    long_description=open('README.rst').read(),
    name='mrjob',
    packages=[
        'mrjob',
        'mrjob.examples',
        'mrjob.examples.bash_wrap',
        'mrjob.examples.mr_postfix_bounce',
        'mrjob.examples.mr_travelling_salesman',
        'mrjob.fs',
        'mrjob.tools',
        'mrjob.tools.emr',
    ],
    package_data={
        'mrjob': ['bootstrap/*.sh'],
        'mrjob.examples.bash_wrap': ['*.sh'],
        'mrjob.examples.mr_postfix_bounce': ['*.json'],
        'mrjob.examples.mr_travelling_salesman': ['example_graphs/*.json'],
    },
    scripts=['bin/mrjob'],
    url='http://github.com/Yelp/mrjob',
    version=mrjob.__version__,
    **setuptools_kwargs
)
