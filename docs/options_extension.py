"""
This extension allows you to define an option like this:

.. mrjob-opt::
    :config: base_tmp_dir
    :switch: --base-tmp-dir
    :type: :ref:`path <data-type-path>`
    :set: all
    :default: value of :py:func:`tempfile.gettempdir`

    Path to put local temp dirs inside.

You generate the table for a set of options (in the above, that would be the
'all' set) like this:

.. mrjob-optlist: all

If you need help, start here: http://sphinx-doc.org/ext/tutorial.html

Order of operations
===================

As doctree is read
------------------

For each mrjob-optlist directive, generate an ``optionlist`` node with an
``option_set`` attribute. This node will otherwise be empty as it will be
populated in the next step.

For each mrjob-option directive, generate an ``option`` node with the full
contents. Save all relevant table data in ``env.optionlist_all_options``.

After doctree is read
---------------------

For each ``optionlist`` node, populate its contents with a table. Use
``optionlink`` nodes in place of references to ``option`` nodes, as links have
not yet been resolved.

After doctree is resolved
-------------------------

Replace ``optionlink`` nodes with references to their respective ``option``
nodes.

"""
from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.util.compat import Directive


def setup(app):
    app.add_node(optionlist)
    app.add_node(optionlink,
                 html=(visit_noop, depart_noop),
                 latex=(visit_noop, depart_noop),
                 text=(visit_noop, depart_noop))
    app.add_node(option,
                 html=(visit_noop, depart_noop),
                 latex=(visit_noop, depart_noop),
                 text=(visit_noop, depart_noop))

    app.add_directive('mrjob-opt', OptionDirective)
    app.add_directive('mrjob-optlist', OptionlistDirective)
    app.connect('doctree-read', populate_option_lists)
    app.connect('doctree-resolved', replace_optionlinks_with_links)
    app.connect('env-purge-doc', purge_options)


class option(nodes.General, nodes.Element):
    """node for defining an option"""
    pass


class optionlist(nodes.General, nodes.Element):
    """node that specifies where an option list goes"""
    pass


class optionlink(nodes.General, nodes.Element):
    """temporary node created during doctree-read and replaced with a link
    during doctree-resolved
    """
    pass


# We are required to have visit/depart functions for each node that appears in
# the final tree, but our new nodes don't generate any markup of their own.
def visit_noop(self, node):
    pass


def depart_noop(self, node):
    pass


class OptionlistDirective(Directive):
    """.. mrjob-optlist: <set identifier>"""

    has_content = True  # content is the set identifier

    def run(self):
        # all we have to do during parsing is make a node where the directive
        # is and remember which options it's supposed to have
        node = optionlist('')
        node.option_set = self.content[0]
        return [node]


class OptionDirective(Directive):
    """
    .. mrjob-opt::
        :config: <snake_case_config_option>
        :switch: <--comma, --separated, --switches)
        :type: <name of or link to a data type>
        :set: <set identifier>
        :default: <arbitrary markup for describing default value>
    """

    has_content = True  # content is the option description
    required_arguments = 0
    optional_arguments = 5
    # pass all argument values through as strings; only set argument is
    # required
    option_spec = {
        'config': directives.unchanged,
        'switch': directives.unchanged,
        'type': directives.unchanged,
        'set': directives.unchanged_required,
        'default': directives.unchanged,
    }

    def run(self):
        env = self.state.document.settings.env

        # generate the linkback node for this option
        targetid = "option-%d" % env.new_serialno('mrjob-opt')
        targetnode = nodes.target('', '', ids=[targetid])

        # Each option will be outputted as a single-item definition list
        # (just like it was doing before we used this extension)
        dl = nodes.definition_list()
        dli = nodes.definition_list_item()

        term = nodes.term()

        # config option shall be bold
        if 'config' in self.options:
            cfg = self.options['config']
            term.append(nodes.strong(cfg, cfg))
            if 'switch' in self.options:
                term.append(nodes.Text(' (', ' ('))

        # switch shall be comma-separated literals
        if 'switch' in self.options:
            switches = self.options['switch'].split(', ')
            for i, s in enumerate(switches):
                if i > 0:
                    term.append(nodes.Text(', ', ', '))
                term.append(nodes.literal(s, s))
            if 'config' in self.options:
                term.append(nodes.Text(')', ')'))

        dli.append(term)

        # classifier is either plan text or a link to some more docs, so parse
        # its contents
        classifier = nodes.classifier()
        type_nodes, messages = self.state.inline_text(
            self.options.get('type', ''), self.lineno)

        # failed attempt at a markup shortcut; may be able to make this work
        # later
        #t = option_info['options']['type']
        #refnode = addnodes.pending_xref(
        #    t, reftarget='data-type-%s' % t,
        #    refexplicit=True, reftype='ref')
        #print refnode
        #refnode += nodes.Text(t, t)
        #type_nodes = [refnode]

        classifier.extend(type_nodes)
        dli.append(classifier)

        # definition holds the description
        defn = nodes.definition()

        # add a default if any
        default_nodes = []
        if 'default' in self.options:
            default_par = nodes.paragraph()
            default_par.append(nodes.strong('Default: ', 'Default: '))
            textnodes, messages = self.state.inline_text(
                self.options['default'], self.lineno)
            default_nodes = textnodes
            default_par.extend(textnodes)
            defn.append(default_par)

        # parse the description like a nested block (see
        # sphinx.compat.make_admonition)
        desc_par = nodes.paragraph()
        self.state.nested_parse(self.content, self.content_offset, desc_par)
        defn.append(desc_par)

        dli.append(defn)
        dl.append(dli)

        if not hasattr(env, 'optionlist_all_options'):
            env.optionlist_all_options = []

        # store info for the optionlist traversal to find
        env.optionlist_all_options.append({
            'docname': env.docname,
            'lineno': self.lineno,
            'options': self.options,
            'content': self.content,
            'target': targetnode,
            'type_nodes': [n.deepcopy() for n in type_nodes],
            'default_nodes': [n.deepcopy() for n in default_nodes]
        })

        return [targetnode, dl]


def purge_options(app, env, docname):
    """Clear our data from the environment when necessary"""
    if not hasattr(env, 'optionlist_all_options'):
        return
    env.optionlist_all_options = [
        option for option in env.optionlist_all_options
        if option['docname'] != docname]


# after doctree is read
def populate_option_lists(app, doctree):
    env = app.builder.env

    for node in doctree.traverse(optionlist):
        # see parsers/rst/states.py, build_table()
        # it's a mess and so is this
        table = nodes.table()

        # make the header block, I swear it's not my fault it's so convoluted
        tgroup = nodes.tgroup(cols=4)
        table += tgroup

        for i in xrange(4):
            tgroup += nodes.colspec(colwidth=1)

        thead = nodes.thead()
        row = nodes.row()
        for label in ['Config', 'Command line', 'Default', 'Type']:
            attributes = {
                'morerows': 0,
                'morecols': 0,
                'stub': False,
            }
            par = nodes.paragraph()
            par.append(nodes.Text(label, label))
            entry = nodes.entry(**attributes)
            entry += par
            row += entry
        thead.append(row)
        tgroup += thead

        tbody = nodes.tbody()
        tgroup += tbody
        # end of header block; whew

        # filter and sort options for this table
        my_options = [oi for oi in env.optionlist_all_options
                      if oi['options']['set'] == node.option_set]

        # automagically alphabetical
        # probably can assume we always have a config option, but who knows
        # what the future holds?
        def sort_key(oi):
            if 'config' in oi['options']:
                return oi['options']['config']
            else:
                return oi['options']['switch'].lstrip('-')

        my_options.sort(key=sort_key)

        # table body

        for option_info in my_options:
            row = nodes.row()

            config_column = nodes.entry()
            switches_column = nodes.entry()
            default_column = nodes.entry()
            type_column = nodes.entry()

            # make a stub node for us to replace after links have been
            # resolved. one of these for each config key and switch.
            def make_refnode(text):
                par = nodes.paragraph()
                ol = optionlink()
                ol.text = text
                ol.docname = option_info['docname']
                ol.target = option_info['target']
                par.append(ol)
                return par

            config_column.append(
                make_refnode(option_info['options'].get('config', '')))
            switches_column.append(
                make_refnode(option_info['options'].get('switch', '')))

            par = nodes.paragraph()
            par.extend(option_info['default_nodes'])
            default_column.append(par)

            par = nodes.paragraph()
            par.extend(option_info['type_nodes'])
            type_column.append(par)

            row.extend([
                config_column,
                switches_column,
                default_column,
                type_column,
            ])

            tbody.append(row)

        node.replace_self([table])


# after doctree is resolved
def replace_optionlinks_with_links(app, doctree, fromdocname):
    # optionlink has attrs text, docname, target,

    for node in doctree.traverse(optionlink):
        refnode = nodes.reference('', '')
        innernode = nodes.emphasis(node.text, node.text)
        refnode['refdocname'] = node.docname
        refnode['refuri'] = app.builder.get_relative_uri(
            fromdocname, node.docname)
        refnode['refuri'] += '#' + node.target['refid']
        refnode.append(innernode)

        node.replace_self([refnode])
