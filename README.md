# Spark POC Project

A small PySpark proof-of-concept project demonstrating text transformation logic and test coverage with `pytest`.

## Project Structure

- `apps/main_job.py` - main job entrypoint for Spark processing
- `src/transformations.py` - Spark DataFrame transformation helpers
- `src/utils.py` - Spark utility helpers
- `tests/` - pytest test suite
- `requirements.txt` - Python dependencies
- `mise.toml` - local development tooling and task definitions

## Prerequisites

- Python 3.14
- Java (OpenJDK 25)
- `mise` for managing the local environment

## Setup

1. Install dependencies:

   ```bash
   mise run bootstrap
   ```

2. Activate the local virtual environment if not already active:

   ```bash
   source .venv/bin/activate
   ```

## Running Tests

Run the pytest suite:

```bash
mise run test
```

## Coverage

Generate a coverage report with:

```bash
mise run coverage
```

This writes a terminal coverage summary and `coverage.xml`.

## Notes

- The test suite currently includes coverage for both `src/transformations.py` and `src/utils.py`.
- The project uses `pytest-cov` to collect coverage data.
