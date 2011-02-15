try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

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
    install_requires=[
        'boto>=1.6',
        'PyYAML',
        'simplejson>=2.0.9'
    ],
    license='Apache',
    long_description=open('README.rst').read(),
    name='mrjob',
    packages=['mrjob',
              'mrjob.botoemr',
              'mrjob.examples',
              'mrjob.tools',
              'mrjob.tools.emr'],
    provides=['mrjob'],
    url='http://github.com/Yelp/mrjob',
    version=mrjob.__version__,
    zip_safe=False, # so that we can bootstrap mrjob
)
