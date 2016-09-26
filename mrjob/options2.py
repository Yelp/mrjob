from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_envs
from mrjob.conf import combine_lists
from mrjob.conf import combine_paths
from mrjob.conf import combine_path_lists
from mrjob.runner import CLEANUP_CHOICES

# TODO: allow custom combiners per runner store (e.g. combine_local_envs)

# TODO: connect and launch opts. this also depends on the runner (e.g.
# bootstrap_mrjob is a launch opt on EMR and Dataproc, but a regular
# job-running opt on the other runners


_RUNNER_OPTS = dict(
    bootstrap_mrjob=dict(
        role='launch',
        switches=[
            (['--bootstrap-mrjob'], dict(
                action='store_true',
                help=("Automatically tar up the mrjob library and install it"
                      " when we run the mrjob. This is the default. Use"
                      " --no-bootstrap-mrjob if you've already installed"
                      " mrjob on your Hadoop cluster."),
            )),
            (['--no-bootstrap-mrjob'], dict(
                action='store_false',
                help=("Don't automatically tar up the mrjob library and"
                      " install it when we run this job. Use this if you've"
                      " already installed mrjob on your Hadoop cluster."),
            )),
        ],
    ),
    check_input_paths=dict(
        switches=[
            (['--check-input-paths'], dict(
                action='store_true',
                help='Check input paths exist before running (the default)',
            )),
            (['--no-check-input-paths'], dict(
                action='store_false',
                help='Skip the checks to ensure all input paths exist',
            )),
        ],
    ),
    cleanup=dict(
        switches=[
            (['--cleanup'], dict(
                help=('Comma-separated list of which directories to delete'
                      ' when a job succeeds, e.g. TMP,LOGS. Choices:'
                      ' %s (default: ALL)' % ', '.join(CLEANUP_CHOICES)),
            )),
        ],
    ),
    cleanup_on_failure=dict(
        switches=[
            (['--cleanup'], dict(
                help=('Comma-separated list of which directories to delete'
                      ' when a job fails, e.g. TMP,LOGS. Choices:'
                      ' %s (default: NONE)' % ', '.join(CLEANUP_CHOICES)),
            )),
        ],
    ),
    cmdenv=dict(
        combiner=combine_envs,
        switches=[
            (['--cmdenv'], dict(
                action='append',
                default=[],  # TODO: custom callback
                help=('Set an environment variable for your job inside Hadoop '
                      'streaming. Must take the form KEY=VALUE. You can use'
                      ' --cmdenv multiple times.'),
            )),
        ],
    ),
    interpreter=dict(
        combiner=combine_cmds,
        switches=[
            (['--interpreter'], dict(
                help='Non-python command to run your script, e.g. "ruby".',
            )),
        ],
    ),
    jobconf=dict(
        combiner=combine_dicts,
        switches=[
            (['--jobconf'], dict(
                action='append',
                default=[],
                help=('-D arg to pass through to hadoop streaming; should'
                      ' take the form KEY=VALUE. You can use --jobconf'
                      ' multiple times.'),
            )),
        ],
    ),
    label=dict(
        switches=[
            (['--label'], dict(
                help='Alternate label for the job, to help us identify it.',
            )),
        ],
    ),
    libjars=dict(
        combiner=combine_path_lists,
        switches=[
            (['--libjar'], dict(
                action='append',
                default=[],
                help=('Path of a JAR to pass to Hadoop with -libjar. On EMR,'
                      ' this can also be a URI; use file:/// to reference JARs'
                      ' already on the EMR cluster'),
            )),
        ],
    ),
    local_tmp_dir=dict(
        combiner=combine_paths,
        # no switches, use $TMPDIR etc.
    ),
    owner=dict(
        switches=[
            (['--owner'], dict(
                help='User who ran the job (default is the current user)',
            )),
        ],
    ),
    python_archives=dict(
        combiner=combine_path_lists,
        switches=[
            (['--python-archive'], dict(
                action='append',
                default=[],
                help=('Archive to unpack and add to the PYTHONPATH of the'
                      ' MRJob script when it runs. You can use'
                      ' --python-archives multiple times.'),
            )),
        ],
    ),
    python_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--python-bin'], dict(
                help=('Alternate python command for Python mappers/reducers.'
                      ' You can include arguments, e.g. --python-bin "python'
                      ' -v"'),
            )),
        ],
    ),
    setup=dict(
        combiner=combine_lists,
        switches=[
            (['--setup'], dict(
                action='append',
                default=[],
                help=('A command to run before each mapper/reducer step in the'
                      ' shell ("touch foo"). You may interpolate files'
                      ' available via URL or on your local filesystem using'
                      ' Hadoop Distributed Cache syntax (". setup.sh#"). To'
                      ' interpolate archives, use #/: "cd foo.tar.gz#/; make'),
            )),
        ],
    ),
    setup_cmds=dict(
        combiner=combine_lists,
        switches=[
            (['--setup-cmd'], dict(
                action='append',
                default=[],
                help=('A command to run before each mapper/reducer step in the'
                      ' shell (e.g. "cd my-src-tree; make") specified as a'
                      ' string. You can use --setup-cmd more than once. Use'
                      ' mrjob.conf to specify arguments as a list to be run'
                      ' directly.'),
            )),
        ],
    ),
    setup_scripts=dict(
        combiner=combine_path_lists,
        switches=[
            (['--setup-script'], dict(
                action='append',
                default=[],
                help=('Path to file to be copied into the local working'
                      ' directory and then run. You can use --setup-script'
                      ' more than once. These are run after setup_cmds.'),
            )),
        ],
    ),
    sh_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--sh-bin'], dict(
                help=('Alternate shell command for setup scripts. You may'
                      ' include arguments, e.g. --sh-bin "bash -ex"'),
            )),
        ],
    ),
    steps_interpreter=dict(
        combiner=combine_cmds,
        switches=[
            (['--steps-interpreter'], dict(
                help=("Non-Python command to use to query the job about its"
                      " steps, if different from --interpreter."),
            )),
        ],
    ),
    steps_python_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--steps-python-bin'], dict(
                help=('Name/path of alternate python command to use to'
                      ' query the job about its steps, if different from the'
                      ' current Python interpreter.'),
            )),
        ],
    ),
    strict_protocols=dict(
        switches=[
            (['--strict-protocols'], dict(
                help=('If something violates an input/output '
                      'protocol then raise an exception (the default)'),
            )),
            (['--no-strict-protocols'], dict(
                help=('If something violates an input/output '
                      'protocol then increment a counter and continue'),
            )),
        ],
    ),
    upload_archives=dict(
        combiner=combine_path_lists,
        switches=[
            (['--archive'], dict(
                action='append',
                default=[],
                help=('Unpack archive in the working directory of this script.'
                      ' You can use --archive multiple times.'),
            )),
        ],
    ),
    upload_files=dict(
        combiner=combine_path_lists,
        switches=[
            (['--file'], dict(
                action='append',
                default=[],
                help=('Copy file to the working directory of this script. You'
                      ' can use --file multiple times.'),
            )),
        ],
    ),
)


def _add_runner_options(parser, dest):
    switches = _RUNNER_OPTS[dest].get(switches) or []

    for args, kwargs in switches:
        kwargs = dict(kwargs)
        kwargs['dest'] = dest
        kwargs.setdefault('default', None)  # no required args

        parser.add_option(*args, **kwargs)
