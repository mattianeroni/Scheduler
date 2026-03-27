# Scheduling

A simple Python scheduling package with `task` and `solver` modules under `src/`.

## Prerequisites

- Python 3.8+
- Git (optional)

## Install dependencies

```bash
cd /Users/mattianeroni/python-projects/scheduling
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
pip install pytest
```

Or (if you use `pyproject.toml` optional dependencies):

```bash
pip install -e .[test]
```

## Run the app

```bash
source .venv/bin/activate
python main.py
```

## Run tests

```bash
source .venv/bin/activate
pytest
```

## Build

```bash
source .venv/bin/activate
python -m build
```

## VS Code debugging

Use the provided `.vscode/launch.json` with:
- Python: Run main.py
- Python: Pytest

Ensure `PYTHONPATH` is `${workspaceFolder}/src` or run `export PYTHONPATH=$PWD/src`.
