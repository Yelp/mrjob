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

try:
    from setuptools import setup
    setup  # quiet "redefinition of unused ..." warning from pyflakes
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'install_requires': [
            'boto>=2.2.0',
            'PyYAML',
            'simplejson>=2.0.9',
        ],
        'provides': ['mrjob'],
        'test_suite': 'tests.suite.load_tests',
        'tests_require': ['unittest2', 'mock'],
        'zip_safe': False,  # so that we can bootstrap mrjob
    }
except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}

import mrjob

setup(
    author='David Marin',
    author_email='dave@yelp.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
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
