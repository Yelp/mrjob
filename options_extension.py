from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.util.compat import Directive


class option(nodes.Admonition, nodes.Element):
    pass


class optionlist(nodes.General, nodes.Element):
    pass


def visit_option_node(self, node):
    self.visit_admonition(node)


def depart_option_node(self, node):
    self.depart_admonition(node)


class OptionlistDirective(Directive):

    def run(self):
        return [optionlist('')]


class OptionDirective(Directive):

    # this enables content in the directive
    has_content = True
    required_arguments = 0
    optional_arguments = 5
    option_spec = {
        'config': directives.unchanged,
        'switch': directives.unchanged,
        'type': directives.unchanged,
        'set': directives.unchanged,
        'default': directives.unchanged,
    }

    def run(self):
        env = self.state.document.settings.env

        targetid = "option-%d" % env.new_serialno('mrjob-opt')
        targetnode = nodes.target('', '', ids=[targetid])

        #ad = make_admonition(option, self.name, ['Option'], self.options,
        #                     self.content, self.lineno, self.content_offset,
        #                     self.block_text, self.state, self.state_machine)

        if not hasattr(env, 'optionlist_all_options'):
            env.optionlist_all_options = []

        option_info = {
            'docname': env.docname,
            'lineno': self.lineno,
            #'option': ad[0].deepcopy(),
            'options': self.options,
            'content': self.content,
            'target': targetnode,
        }

        dl = nodes.definition_list()
        dli = nodes.definition_list_item()

        if 'config' in option_info['options']:
            if 'switch' in option_info['options']:
                opt_name = ', '.join([
                    option_info['options']['config'],
                    option_info['options']['switch'],
                ])
            else:
                opt_name = option_info['options']['config']
        else:
            opt_name = option_info['options']['switch']

        term = nodes.term()
        term.append(nodes.Text(opt_name, opt_name))

        dli.append(term)

        classifier = nodes.classifier()
        t = option_info['options'].get('type', None)
        classifier.append(nodes.Text(t, t))
        dli.append(classifier)

        option_desc = '\n'.join(option_info['content'])
        textnodes, messages = self.state.inline_text(option_desc, self.lineno)
        desc_par = nodes.paragraph(option_desc)
        desc_par.extend(textnodes)
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

        option_info['default_nodes'] = default_nodes
        env.optionlist_all_options.append(option_info)

        return [targetnode, dl]


def purge_options(app, env, docname):
    if not hasattr(env, 'optionlist_all_options'):
        return
    env.optionlist_all_options = [
        todo for todo in env.optionlist_all_options
        if todo['docname'] != docname]


def process_option_nodes(app, doctree, fromdocname):
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

        for option_info in env.optionlist_all_options:
            row = nodes.row()

            config_column = nodes.entry()
            switches_column = nodes.entry()
            default_column = nodes.entry()
            type_column = nodes.entry()

            def make_refnode(text):
                refnode = nodes.reference('', '')
                innernode = nodes.emphasis(text, text)
                refnode['refdocname'] = option_info['docname']
                refnode['refuri'] = app.builder.get_relative_uri(
                    fromdocname, option_info['docname'])
                refnode['refuri'] += '#' + option_info['target']['refid']
                refnode.append(innernode)
                par = nodes.paragraph()
                par.append(refnode)
                return par

            config_column.append(
                make_refnode(option_info['options'].get('config', '')))
            switches_column.append(
                make_refnode(option_info['options'].get('switch', '')))
            par = nodes.paragraph()
            par.extend(option_info['default_nodes'])
            default_column.append(par)
            t = option_info['options']['type']
            par = nodes.paragraph()
            par.append(nodes.Text(t, t))
            type_column.append(par)

            row.extend([
                config_column,
                switches_column,
                default_column,
                type_column,
            ])

            tbody.append(row)

        node.replace_self([table])


def setup(app):
    app.add_node(optionlist)
    app.add_node(option,
                 html=(visit_option_node, depart_option_node),
                 latex=(visit_option_node, depart_option_node),
                 text=(visit_option_node, depart_option_node))

    app.add_directive('mrjob-opt', OptionDirective)
    app.add_directive('mrjob-optlist', OptionlistDirective)
    app.connect('doctree-resolved', process_option_nodes)
    app.connect('env-purge-doc', purge_options)
