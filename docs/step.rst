mrjob.step - represent Job Steps
================================

.. automodule:: mrjob.step

.. currentmodule:: mrjob.step

Steps
-----

.. autoclass:: MRStep
.. autoclass:: JarStep
.. autoclass:: SparkStep
.. autoclass:: SparkJarStep
.. autoclass:: SparkScriptStep

Argument interpolation
----------------------

Use these constants in your step's *args* and mrjob will automatically replace
them before running your step.

.. autodata:: INPUT
.. autodata:: OUTPUT
.. autodata:: GENERIC_ARGS
