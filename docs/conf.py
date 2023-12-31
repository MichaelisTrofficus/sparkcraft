"""Sphinx configuration."""
project = "SparkCraft"
author = "Miguel Otero Pedrido"
copyright = "2023, Miguel Otero Pedrido"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.inheritance_diagram",
    "sphinx.ext.graphviz",
    "sphinx.ext.autosummary",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "sphinx_rtd_theme"
