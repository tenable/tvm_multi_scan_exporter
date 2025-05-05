### Getting Started

Here are some basic commands that come in handy when you get started.

First, the `build` library need to be installed.

```
pip install build
```

Install the development dependencies using the following command:

```
pip install -e .[dev]
```

Run the tests

```
pytest
```

Then, the following command builds the project, and the package should be made available under `dist/`.

```
python -m build
```

Then, the user can install it locally using pip, as follows:

```
pip install <path to the .whl file>
```

Other resources for development, such as docker compose files can be found under the `/dev` folder.
