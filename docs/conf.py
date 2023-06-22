"""Sphinx configuration."""
project = "SparkCraft"
author = "Miguel Otero Pedrido"
copyright = "2023, Miguel Otero Pedrido"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
