#!/usr/bin/env python3
"""Create (or complete) the repository layout for the Real-Time IoT Sensor Data Pipeline.

Run from the repository root:  python setup_structure.py
Safe to re-run: existing files are never overwritten or deleted.

* Empty folders get a .gitkeep so Git tracks them until real code is added.
* Python folders under src/ and tests/ get an empty __init__.py so imports and pytest work.
* Environment files (docker-compose.yml, scripts/, .devcontainer/, ...) are NOT created here:
  they are committed from the V7.1 environment and listed below only so you can see what is missing.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Folders for the project code and documents (kept in sync with README.md "Repository structure").
FOLDERS = [
    # Documents
    "docs/planning", "docs/requirements", "docs/design", "docs/testing", "docs/final",
    # Source code
    "src/contracts",             # sensor_event.schema.json (event contract)
    "src/simulator",             # device registry, event generator, fault injection, Kafka producer
    "src/streaming",             # PySpark Structured Streaming: validate, dedupe, window, write Parquet/SQL
    "src/serving",               # SQL Server DDL and load logic
    "src/serving/sql",           # .sql files for tables, views and maintenance
    "src/batch",                 # batch jobs run by Airflow: gold_backfill, lake_compaction, warehouse_maintenance
    "src/data_quality",          # Great Expectations suites and the daily report
    "src/dashboard",             # Grafana dashboard JSON exports
    "src/airflow/dags",          # capstone_healthcheck exists; five pipeline DAGs planned
    # Tests
    "tests/unit", "tests/integration", "tests/data_quality", "tests/fixtures",
]

# Python packages (get __init__.py). DAG and SQL/JSON folders are not packages.
PACKAGES = [
    "src", "src/simulator", "src/streaming", "src/serving", "src/batch", "src/data_quality",
    "tests", "tests/unit", "tests/integration", "tests/data_quality",
]

# Environment files that must exist (committed from the V7.1 environment).
ENVIRONMENT_FILES = [
    "docker-compose.yml", "docker-compose.test.yml", ".env.example", ".env.test", ".dockerignore",
    "requirements.txt", "requirements-dev.txt",
    ".devcontainer/devcontainer.json", ".devcontainer/Dockerfile",
    "airflow/Dockerfile", "grafana/provisioning/datasources/sqlserver.yml",
    "scripts/_common.py", "scripts/init_db.py", "scripts/create_topics.py",
    "scripts/smoke_test.py", "scripts/check-clean-build.ps1",
    "docs/dev-environment.md", "src/airflow/dags/capstone_healthcheck.py",
]


def main() -> None:
    created = []
    for folder in FOLDERS:
        path = ROOT / folder
        path.mkdir(parents=True, exist_ok=True)
    for pkg in PACKAGES:
        init = ROOT / pkg / "__init__.py"
        if not init.exists():
            init.touch()
            created.append(init)
    for folder in FOLDERS:
        path = ROOT / folder
        has_content = any(p.name != ".gitkeep" for p in path.iterdir())
        keep = path / ".gitkeep"
        if has_content and keep.exists():
            keep.unlink()          # real files now track the folder
        elif not has_content and not keep.exists():
            keep.touch()
            created.append(keep)
    print(f"Created {len(created)} placeholder file(s).")
    missing = [f for f in ENVIRONMENT_FILES if not (ROOT / f).exists()]
    if missing:
        print("Missing environment files (add them from the V7.1 environment):")
        for f in missing:
            print("  -", f)
    else:
        print("All environment files are present.")


if __name__ == "__main__":
    main()
