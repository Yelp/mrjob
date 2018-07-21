# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017 Yelp
# Copyright 2018 Yelp
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
        'extras_require': {
            # highly recommended, but requires a compiler
            'ujson': ['ujson'],
        },
        'install_requires': [
            'boto>=2.35.0',
            'filechunkio',
            'google-api-python-client>=1.5.0',
            'oauth2client>=2.0.0',
            'PyYAML>=3.08',
        ],
        'provides': ['mrjob'],
        'test_suite': 'tests.suite.load_tests',
        'tests_require': ['simplejson'],
        'zip_safe': False,  # so that we can bootstrap mrjob
    }

    if sys.version_info >= (3, 0):
        setuptools_kwargs['extras_require']['rapidjson'] = ['rapidjson']

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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Distributed Computing',
    ],
    description='Python MapReduce framework',
    entry_points=dict(
        console_scripts=[
            'mrjob=mrjob.cmd:main',
            'mrjob-%d=mrjob.cmd:main' % sys.version_info[:1],
            'mrjob-%d.%d=mrjob.cmd:main' % sys.version_info[:2],
        ]
    ),
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
        'mrjob.logs',
        'mrjob.tools',
        'mrjob.tools.emr',
    ],
    package_data={
        'mrjob': ['bootstrap/*.sh'],
        'mrjob.examples.bash_wrap': ['*.sh'],
        'mrjob.examples.mr_postfix_bounce': ['*.json'],
        'mrjob.examples.mr_travelling_salesman': ['example_graphs/*.json'],
    },
    url='http://github.com/Yelp/mrjob',
    version=mrjob.__version__,
    **setuptools_kwargs
)
