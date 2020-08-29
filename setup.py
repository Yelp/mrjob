# Copyright 2009-2015 Yelp and Contributors
# Copyright 2016-2017 Yelp
# Copyright 2018 Google Inc.
# Copyright 2019 Yelp
# Copyright 2020 Affirm, Inc.
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
            'aws': [
                'boto3>=1.4.6',
                'botocore>=1.6.0',
            ],
            'google': [
                # 1.1.0 is the last version that supports Python 2.7
                'google-cloud-dataproc>=0.3.0,<=1.1.0',
                'google-cloud-logging>=1.9.0',
                'google-cloud-storage>=1.13.1',
            ],
            'rapidjson': ['python-rapidjson'],
            'simplejson': ['simplejson'],
            'ujson': ['ujson'],
        },
        'install_requires': [
             'PyYAML>=3.10',
        ],
        'provides': ['mrjob'],
        'test_suite': 'tests',
        'tests_require': [
            'pyspark',
            'python-rapidjson',
            'simplejson',
            'ujson',
            'warcio',
        ],
        'zip_safe': False,  # so that we can bootstrap mrjob
    }

    # grpcio 1.11.0 and 1.12.0 seem not to compile with PyPy
    if hasattr(sys, 'pypy_version_info'):
        setuptools_kwargs['extras_require']['google'].append(
            'grpcio<=1.10.0')

    # rapidjson is not available on Python 2
    if sys.version_info[0] == 2:
        del setuptools_kwargs['extras_require']['rapidjson']
        setuptools_kwargs['tests_require'].remove('python-rapidjson')

    # limited Python 3.4 support
    elif sys.version_info[:2] <= (3, 4):
        # Google libs don't install on Python 3.4
        del setuptools_kwargs['extras_require']['google']

        # PyYAML dropped 3.4 support in version 5.3
        setuptools_kwargs['install_requires'] = [
            ir + ',<5.2' if ir.startswith('PyYAML') else ir
            for ir in setuptools_kwargs['install_requires']
        ]

    # pyspark doesn't yet work on 3.8
    elif sys.version_info[:2] >= (3, 8):
        setuptools_kwargs['tests_require'].remove('pyspark')

except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}

with open('README.rst') as f:
    long_description = f.read()

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
        'Programming Language :: Python :: 3.7',
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
    long_description=long_description,
    name='mrjob',
    packages=[
        'mrjob',
        'mrjob.examples',
        'mrjob.fs',
        'mrjob.logs',
        'mrjob.spark',
        'mrjob.tools',
        'mrjob.tools.emr',
    ],
    package_data={
        'mrjob': ['bootstrap/*.sh'],
        'mrjob.examples': ['*.txt', '*.jar', '*.rb', 'docs-to-classify/*.txt'],
        'mrjob.examples.mr_travelling_salesman': ['example_graphs/*.json'],
    },
    url='http://github.com/Yelp/mrjob',
    version=mrjob.__version__,
    **setuptools_kwargs
)
