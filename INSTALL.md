# Installation

Clone this repository and then install with `pip` in the virtual environment of your choice.

```
git clone git@https://github.com:matgenix/qtoolkit
cd qtoolkit
pip install .
```

## Development installation

You can use

```
pip install -e .[dev,tests]
```

to perform an editable installation with additional development and test dependencies.
You can then activate `pre-commit` in your local repository with `pre-commit install`.
