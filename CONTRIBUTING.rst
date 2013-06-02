Contributing to mrjob
=====================

mrjob is developed using a standard Github pull request process. Almost all
code is reviewed in pull requests.

The general process for working on mrjob is:

* Fork the project on Github
* Clone your fork to your local machine
* Create a feature branch from master (e.g. ``git branch delete_all_the_code``)
* Write code, commit often
* Write test cases for all changed functionality
* Submit a pull request against ``master`` on Github
* Wait for code review!

It would also help to discuss your ideas on the `mailing list`_ so we can warn
you of possible merge conflicts with ongoing work or offer suggestions for
where to put code.

.. _`mailing list`: http://groups.google.com/group/mrjob

Things that will make your branch more likely to be pulled:

* Comprehensive, fast test cases
* Detailed explanation of what the change is and how it works
* Reference relevant issue numbers in the tracker
* API backward compatibility

If you add a new configuration option, please try to do all of these things:

* Make its name unambiguous in the context of multiple runners (e.g.
  ``ec2_task_instance_type`` instead of ``instance_type``)
* Add command line switches that allow full control over the option
* Document the option and its switches in the appropriate file under ``docs``

.. Copied from docs/guides/contributing.rst, which is the canonical text. This
   version exists only for Github.
