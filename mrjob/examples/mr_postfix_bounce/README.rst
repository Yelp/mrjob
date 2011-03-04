mr_postfix_bounce
=================

mr_postfix_bounce is a mrjob (github.com/Yelp/mrjob) script that processes a
Postfix log file to look for permanent delivery failures.

Some important features:
 * Loads a JSON file of rules specific to return codes of major domain providers
   (please submit pull requests if you have additions!)
 * Returns a list of date ordinals for each email address if run over multiple days

Try it out!
===========
Note: this will only stream output if there are permanent failures in the log
file. If you test on a log file without permanent failures, there will not be
any output

::

    # locally
    python mr_postfix_bounce.py postfix.log -v
    # on EMR
    python mr_postfix_bounce.py postfix.log -v -r emr

