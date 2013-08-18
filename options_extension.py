# what you should really do is
# in the optlist directive, in doctree-read, output the whole table *but with
# a temporary node for the option where the link will go*. Then generate the
# ref with a separate traversal in doctree-resolved.
from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.util.compat import Directive


class option(nodes.General, nodes.Element):
    pass


class optionlist(nodes.General, nodes.Element):
    pass


class optionlink(nodes.General, nodes.Element):
    pass


def visit_noop(self, node):
    pass


def depart_noop(self, node):
    pass


class OptionlistDirective(Directive):
    has_content = True

    def run(self):
        node = optionlist('')
        node.option_set = self.content[0]
        return [node]


class OptionDirective(Directive):

    # this enables content in the directive
    has_content = True
    required_arguments = 0
    optional_arguments = 5
    option_spec = {
        'config': directives.unchanged,
        'switch': directives.unchanged,
        'type': directives.unchanged,
        'set': directives.unchanged_required,
        'default': directives.unchanged,
    }

    def run(self):
        env = self.state.document.settings.env

        targetid = "option-%d" % env.new_serialno('mrjob-opt')
        targetnode = nodes.target('', '', ids=[targetid])

        if not hasattr(env, 'optionlist_all_options'):
            env.optionlist_all_options = []

        option_info = {
            'docname': env.docname,
            'lineno': self.lineno,
            'options': self.options,
            'content': self.content,
            'target': targetnode,
        }

        # Each option will be outputted as a single-item definition list
        # (just like it was doing before we used this extension)
        dl = nodes.definition_list()
        dli = nodes.definition_list_item()

        term = nodes.term()

        if 'config' in option_info['options']:
            cfg = option_info['options']['config']
            term.append(nodes.strong(cfg, cfg))
            if 'switch' in option_info['options']:
                term.append(nodes.Text(' (', ' ('))
        if 'switch' in option_info['options']:
            switches = option_info['options']['switch'].split(', ')
            for i, s in enumerate(switches):
                if i > 0:
                    term.append(nodes.Text(', ', ', '))
                term.append(nodes.literal(s, s))
            if 'config' in option_info['options']:
                term.append(nodes.Text(')', ')'))

        dli.append(term)

        classifier = nodes.classifier()
        type_nodes, messages = self.state.inline_text(
            option_info['options'].get('type', ''), self.lineno)

        # failed attempt at a markup shortcut
        #t = option_info['options']['type']
        #refnode = addnodes.pending_xref(
        #    t, reftarget='data-type-%s' % t,
        #    refexplicit=True, reftype='ref')
        #print refnode
        #refnode += nodes.Text(t, t)
        #type_nodes = [refnode]

        classifier.extend(type_nodes)
        dli.append(classifier)

        desc_par = nodes.paragraph()
        self.state.nested_parse(self.content, self.content_offset, desc_par)
        defn = nodes.definition()

        default_nodes = []
        if 'default' in option_info['options']:
            default_par = nodes.paragraph()
            default_par.append(nodes.strong('Default: ', 'Default: '))
            textnodes, messages = self.state.inline_text(
                option_info['options']['default'], self.lineno)
            default_nodes = textnodes
            default_par.extend(textnodes)
            defn.append(default_par)

        defn.append(desc_par)
        dli.append(defn)

        dl.append(dli)

        option_info['type_nodes'] = [n.deepcopy() for n in type_nodes]
        option_info['default_nodes'] = [n.deepcopy() for n in default_nodes]
        env.optionlist_all_options.append(option_info)

        return [targetnode, dl]


def purge_options(app, env, docname):
    if not hasattr(env, 'optionlist_all_options'):
        return
    env.optionlist_all_options = [
        todo for todo in env.optionlist_all_options
        if todo['docname'] != docname]


def populate_option_lists(app, doctree):
    env = app.builder.env

    for node in doctree.traverse(optionlist):
        # see parsers/rst/states.py, build_table()
        table = nodes.table()

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

        my_options = [oi for oi in env.optionlist_all_options
                      if oi['options']['set'] == node.option_set]

        def sort_key(oi):
            if 'config' in oi['options']:
                return oi['options']['config']
            else:
                return oi['options']['switch'].lstrip('-')

        my_options.sort(key=sort_key)

        for option_info in my_options:
            row = nodes.row()

            config_column = nodes.entry()
            switches_column = nodes.entry()
            default_column = nodes.entry()
            type_column = nodes.entry()

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
