try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='mrjob',
    version='0.1',
    provides='mrjob',
    author='David Marin',
    author_email='dave@yelp.com',
    url='http://github.com/Yelp/mrjob',
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: System :: Distributed Computing',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
    ],
    packages=['mrjob', 'mrjob.examples'],
    install_requires=['boto>=1.6'],
    description='Python MapReduce framework',
    long_description='Write multi-step MapReduce jobs in Python, and run them on your own Hadoop cluster or Amazon Elastic MapReduce',

    zip_safe=False, # so that we can run examples
)
