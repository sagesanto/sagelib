import os
import types

project = "sagelib"
author = "Sage Santomenna"

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "autoapi.extension",
]

templates_path = ["_templates"]

smartquotes_action = "qe"

html_theme = "furo"

autoapi_ignore = ["*.venv*","*conf.py"]

html_static_path = ["_static"]

# -- Plausible support
ENABLE_PLAUSIBLE = os.environ.get("READTHEDOCS_VERSION_TYPE", "") in ["branch", "tag"]
html_context = {"enable_plausible": ENABLE_PLAUSIBLE}

autodoc_typehints = "description" 

autoapi_type = "python"
autoapi_dirs = ["../../../sagelib"]
autoapi_template_dir = "_templates/autoapi"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-inheritance-diagram",
    "show-module-summary",
    "imported-members",
]

autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

autoapi_keep_files = True


# -- custom auto_summary() macro by Antoine Beyeler ---------------------------------------------

def contains(seq, item):
    """Jinja2 custom test to check existence in a container.

    Example of use:
    {% set class_methods = methods|selectattr("properties", "contains", "classmethod") %}

    Related doc: https://jinja.palletsprojects.com/en/3.1.x/api/#custom-tests
    """
    return item in seq


def prepare_jinja_env(jinja_env) -> None:
    """Add `contains` custom test to Jinja environment."""
    jinja_env.tests["contains"] = contains


autoapi_prepare_jinja_env = prepare_jinja_env

# Custom role for labels used in auto_summary() tables.
rst_prolog = """
.. role:: summarylabel
"""

# Related custom CSS
html_css_files = [
    "css/label.css",
]

avoid = ["PipelineInputAssociation", "PrecursorProductAssociation"]

def autoapi_skip_members(app, what, name, obj, skip, options):
    for phrase in avoid:
        if phrase in name:
            skip = True
    if getattr(obj, '__doc__', None) and isinstance(obj, (types.FunctionType, types.MethodType)):
        return False
    else:
        if isinstance(obj, (types.FunctionType, types.MethodType)):
            print("SKIPPING",obj)
        return skip

def setup(app):
    app.connect("autoapi-skip-member", autoapi_skip_members)