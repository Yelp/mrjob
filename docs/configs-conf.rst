mrjob.conf - parse and write config files
=========================================

.. automodule:: mrjob.conf

Reading and writing mrjob.conf
------------------------------

.. autofunction:: find_mrjob_conf
.. autofunction:: load_mrjob_conf
.. autofunction:: load_opts_from_mrjob_conf

Combining options
-----------------

Combiner functions take a list of values to combine, with later options taking precedence over earlier ones. ``None`` values are always ignored.

.. autofunction:: combine_values
.. autofunction:: combine_lists
.. autofunction:: combine_dicts
.. autofunction:: combine_cmds
.. autofunction:: combine_cmd_lists
.. autofunction:: combine_envs
.. autofunction:: combine_local_envs
.. autofunction:: combine_paths
.. autofunction:: combine_path_lists


