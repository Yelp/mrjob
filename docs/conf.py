# -*- coding: utf-8 -*-
import os
import sys

from better import better_theme_path

import mrjob

# Help sphinx find options_extension
sys.path += [os.path.abspath(os.path.split(__file__)[0])]

READ_THE_DOCS = os.environ.get('READTHEDOCS', None) == 'True'

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath('..'))

# -- General configuration ----------------------------------------------------

needs_sphinx = '1.0'

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.coverage', 'options_extension',
              'sphinx.ext.intersphinx', 'sphinx.ext.ifconfig']
intersphinx_mapping = {'http://docs.python.org/2/': None}

templates_path = ['_templates']
exclude_patterns = ['_build']
source_suffix = '.rst'
#source_encoding = 'utf-8-sig'
master_doc = 'index'
pygments_style = 'sphinx'

# project info

project = u'mrjob'
copyright = u'2009-2013 Yelp and Contributors'
# The short X.Y version. Can refer to in docs with |version|.
version = mrjob.__version__.split('-')[0]
# The full version, including alpha/beta/rc tags.
# Can refer to in docs with |release|.
release = mrjob.__version__
#language = None


# -- HTML output --------------------------------------------------------------

html_theme_path = [better_theme_path]
html_static_path = ['_static']
html_theme = 'better'
html_theme_options = {
    'html_show_sourcelink': True,
    'cssfiles': ['_static/style.css'],
}
html_context = {}
html_sidebars = {
    '**': ['localtoc.html', 'sidebarhelp.html', 'sourcelink.html',
           'searchbox.html'],
    'index': ['indexsidebar.html', 'sidebarhelp.html', 'sourcelink.html',
              'searchbox.html'],
}

html_title = "%(project)s v%(release)s documentation" % {
    'project': project, 'release': release}
html_short_title = "Home"
# we will set this again when sphinx-better-theme supports a logo in a good
# place
#html_logo = None
#html_favicon = None

if READ_THE_DOCS:
    html_theme_options['ga_ua'] = 'UA-42793220-1'
    html_theme_options['ga_domain'] = 'readthedocs.org'
else:
    html_theme_options['ga_ua'] = 'UA-42793220-2'
    html_theme_options['ga_domain'] = 'pythonhosted.org'

# Necessary for best search results
html_show_sourcelink = True

# Output file base name for HTML help builder.
htmlhelp_basename = 'mrjobdoc'

# -- Options for LaTeX output -------------------------------------------------

# The paper size ('letter' or 'a4').
#latex_paper_size = 'letter'

# The font size ('10pt', '11pt' or '12pt').
#latex_font_size = '10pt'

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass
# [howto/manual]).
latex_documents = [
    ('index', 'mrjob.tex', u'mrjob Documentation',
     u'Steve Johnson', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#latex_use_parts = False

# If true, show page references after internal links.
#latex_show_pagerefs = False

# If true, show URL addresses after external links.
#latex_show_urls = False

# Additional stuff for the LaTeX preamble.
#latex_preamble = ''

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_domain_indices = True

# -- Options for manual page output -------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ('index', 'mrjob', u'mrjob Documentation',
     [u'David Marin'], 1)
]
