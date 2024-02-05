# Installation

QToolKit is available on [PyPI](https://pypi.org/project/qtoolkit) and can be installed with `pip`:

```shell
pip install qtoolkit
```

## Development installation

Clone this repository and then install with `pip` in the virtual environment of your choice.

```shell
git clone git@https://github.com:matgenix/qtoolkit
cd qtoolkit
pip install -e .[dev,tests]
```

This will perform an editable installation with additional development and test dependencies.
You can then activate `pre-commit` in your local repository with `pre-commit install`.
