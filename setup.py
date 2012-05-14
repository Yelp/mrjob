try:
    from setuptools import setup
    setup  # quiet "redefinition of unused ..." warning from pyflakes
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'install_requires': [
            'boto>=2.0',
            'PyYAML',
            'simplejson>=2.0.9',
        ],
        'provides': ['mrjob'],
        'test_suite': 'tests.suite.load_tests',
        'tests_require': ['unittest2'],
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
        'Development Status :: 4 - Beta',
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
    packages=['mrjob',
              'mrjob.examples',
              'mrjob.tools',
              'mrjob.tools.emr'],
    url='http://github.com/Yelp/mrjob',
    version=mrjob.__version__,
    **setuptools_kwargs
)
