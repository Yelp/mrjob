Setting up your production environment inside Hadoop
====================================================

Many jobs have significant external dependencies, both libraries and other
source code.

Combining shell syntax with Hadoop's DistributedCache notation, mrjob's
:mrjob-opt:`setup` option provides a powerful, dynamic alternative to
pre-installing your Hadoop dependencies on every node.

Putting your source tree in :envvar:`PYTHONPATH`
------------------------------------------------

If your job spans multiple files, you can create a tarball of your source tree
and use :mrjob-opt:`setup` to have it decompressed and added to the
:envvar:`PYTHONPATH.

mrjob won't create the tarball for you, but, for reference, here is a command line
that will put an entire directory into a tarball:

.. code-block:: shell

    tar -C your-src-code -f your-src-code.tar.gz -z -c .

Then, run your job with:

.. code-block:: shell

    --setup 'PYTHONPATH=$PYTHONPATH:your-src-code.tar.gz#/'

It's basically just shell script, except the ``#/`` tells mrjob that
``your-src-code.tar.gz`` is an archive which should be expanded in your job's working
directory and the resulting path interpolated into the script. mrjob is smart enough
to know that the preceding ``PYTHONPATH=$PYTHONPATH:`` is part of the script
and not part of the filename.

Note that the ``-C`` is important; this ensures that, say, ``your-src-code/foo.py``
appears in the tarball with the path ``

If every job you run is going to want to use ``your-src-code.tar.gz``, you can do
this in your :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      hadoop:
        setup:
        - PYTHONPATH=$PYTHONPATH:your-src-code.tar.gz#/

Compiling source code
---------------------

Now, suppose that, for performance reasons, your source code includes some C
code that need to be compiled. You can't just compile them locally and
upload the object files because your Hadoop cluster nodes have a different
architecture.

One thing you can do is simply run :command:gcc:

.. code-block:: shell

    --setup 'gcc -c your-src-code.tar.gz#/*.c'

Again, mrjob treats ``your-src-code.tar.gz#/`` specially, and the rest is just
plain old shell script.

It's probably better practice use a makefile, which you can do like this:

.. code-block:: shell

    --setup 'cd your-src-code.tar.gz#/' --setup make

There's an important wrinkle here. Hadoop can run multiple tasks
(mappers/reducers) on the same node, but archives that are uploaded are shared
between all tasks, using symlinks.




Installing libraries in a virtualenv
------------------------------------



Making data files available to your job
---------------------------------------



The gory details
----------------
