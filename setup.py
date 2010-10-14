try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    author='David Marin',
    author_email='dave@yelp.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Operating System :: OS Independent',
        'Topic :: System :: Distributed Computing',
    ],
    description='Python MapReduce framework',
    install_requires=['boto>=1.6','PyYAML'],
    license='Apache',
    long_description='Write multi-step MapReduce jobs and run them on Amazon Elastic MapReduce or your own Hadoop Cluster',
    name='mrjob',
    packages=['mrjob',
              'mrjob.botoemr',
              'mrjob.examples',
              'mrjob.tools',
              'mrjob.tools.emr'],
    provides='mrjob',
    url='http://github.com/Yelp/mrjob',
    version='0.1.0-pre1',
    zip_safe=False, # so that we can easily bootstrap mrjob
)
