# Scheduler


## Prerequisites

- Python 3.11+

## Install dependencies

```bash
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

## Run via code

```bash
source .venv/bin/activate
python main.py
```

## Run via cli 
```bash
python -m scheduler.cli --input-path <input-folder> --output-path <output-folder>
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

## Bug Report

Contact: [mattianeroni93@gmail.com](mailto:mattianeroni93@gmail.com)

## ToDo
- Introduce additional concept of task group. Forcing the scenario where, if a resource takes care of a task, it must take care of all the tasks in the group
