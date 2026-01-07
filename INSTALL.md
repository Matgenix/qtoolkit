# Installation

QToolKit is available on [PyPI](https://pypi.org/project/qtoolkit) and can be installed with `pip`:

```shell
pip install qtoolkit
```

## Remote tools installation

To be able to use the `QueueManager` object on a remote cluster, QToolKit needs
to be installed with the `remote` extra:

```shell
pip install qtoolkit[remote]
```

## MSONable installation

If the objects need to be JSON serializable (MSONable), QToolKit needs to be installed with the `msonable` extra:

```shell
pip install qtoolkit[msonable]
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
