# Real-Time IoT Sensor Data Pipeline

**Digital Egypt Pioneers Initiative (DEPI) | Track: Microsoft Data Engineer | ONL5_AIS5_G1 | Team_1 | 2026**

An end-to-end, real-time data pipeline for IoT sensor data. A Python device simulator publishes readings (temperature, humidity, CO2, power draw) from a fleet of devices across several sites. The readings flow through Kafka, are validated, deduplicated and aggregated by a PySpark Structured Streaming job, are stored in layered storage, and are served to a live Grafana dashboard. Bad records are quarantined in a dead-letter store instead of being lost or crashing the pipeline.

The whole stack runs locally from Docker Compose, uses free tools only, and is tested with late, duplicate and malformed events.

---

## Table of contents

1. [Architecture](#architecture)
2. [Technology stack](#technology-stack)
3. [Repository structure](#repository-structure)
4. [Prerequisites](#prerequisites)
5. [Installation and configuration](#installation-and-configuration)
6. [Running the pipeline](#running-the-pipeline)
7. [Service endpoints](#service-endpoints)
8. [Event contract](#event-contract)
9. [Airflow DAGs](#airflow-dags)
10. [Data quality](#data-quality)
11. [KPIs](#kpis)
12. [Testing](#testing)
13. [Troubleshooting](#troubleshooting)
14. [Documentation](#documentation)
15. [Team and workflow](#team-and-workflow)
16. [Project status](#project-status)

---

## Architecture

Event-driven, Kappa-style design with layered storage. Kafka is the replayable log, and a single streaming job produces the clean and aggregated layers.

```
 Python simulator ──► Kafka (raw topic) ──► PySpark Structured Streaming
 (fault injection)         │                    │   parse, validate, deduplicate,
                           │                    │   event-time windows + watermark
                           │                    │
                           ▼                    ├──► Kafka (dead-letter topic) ──► durable dead-letter storage
                    Kafka dead-letter           ├──► Parquet lake: raw / clean / aggregated / late-events
                                                └──► SQL Server Developer (serving tables) ──► Grafana dashboard

 Apache Airflow ── orchestrates: health checks, data-quality report, compaction, backfill, maintenance
 Great Expectations ── validates stored clean Parquet and SQL Server tables
```

Key behaviours:

- **Validation:** required fields, types, value ranges per sensor type. Invalid events go to the dead-letter topic with the failure reason and original payload.
- **Deduplication:** by `event_id` within the watermark.
- **Late data:** event-time windows with a watermark (starting value 2 minutes). Events later than the watermark go to a separate late-events Parquet path and are corrected by the `gold_backfill` DAG.
- **Reliability:** failed writes are retried, writes are idempotent, and an event counts as persisted only after the required writes are verified.
- **Replay:** the Kafka raw topic keeps 7 days; the raw Parquet layer is kept for the whole project, so replay after 7 days uses Parquet.

## Technology stack

| Layer | Tool | Role |
|---|---|---|
| Data source | Python simulator | Generates sensor events with fault injection |
| Messaging | Apache Kafka | Partitioned event log (by device ID) plus a dead-letter topic |
| Stream processing | PySpark Structured Streaming | Per-event validation, deduplication, watermarks, windows |
| Lake storage | Apache Parquet | Raw and clean layers for history and replay |
| Serving layer | SQL Server Developer | Relational tables (keys and constraints) for the dashboard and analysts |
| Data quality | Great Expectations | Expectation suites on stored clean Parquet and SQL Server tables, daily report |
| Orchestration | Apache Airflow | Five scheduled and on-demand DAGs |
| Dashboard | Grafana | Live panels on the SQL Server serving tables |
| Testing | pytest | Unit, integration and quality tests |
| DevOps | Docker Compose, GitHub | One-command run, version control |

All tools are free to use. SQL Server Developer edition is licensed for development and testing only.

## Repository structure

```
.
├── README.md                  This file
├── docker-compose.yml         Full stack, pinned image versions
├── .env.example               Environment variables (no credentials)
├── .gitignore
├── .devcontainer/             Dev container definition (if used)
├── docs/
│   ├── planning/              Planning document and literature review
│   ├── requirements/          Requirements document
│   ├── design/                Diagrams, schema, wireframes
│   ├── testing/               Test plan, test report, bug reports
│   └── final/                 Technical documentation, user manual, presentation, infographic, demo
├── src/
│   ├── contracts/             Event schema (sensor_event.schema.json)
│   ├── ...                    Producers, streaming jobs, serving layer, dashboard,
│   │                          Airflow DAGs, Great Expectations suites
└── tests/                     Automated tests, data quality checks, fixtures
```

> Subfolders under `src/` are finalized during implementation (Iteration 3). Update this tree to match the repository before submission.

## Prerequisites

- Docker Desktop (Windows or macOS) or Docker Engine with the Compose plugin (Linux)
- Git
- Memory: the full stack (Kafka, Spark, SQL Server, Airflow, Grafana) is memory-hungry. Give Docker enough RAM, and use the reduced demo profile and per-container limits on small laptops. *(Record the minimum tested RAM here once measured.)*
- Free ports on the host: see [Service endpoints](#service-endpoints)

No native installs of Kafka, Spark, SQL Server or Airflow are needed. Everything runs in containers.

## Installation and configuration

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Create your environment file from the template and set the values (passwords, ports):

   ```bash
   cp .env.example .env
   ```

   Never commit `.env`. It is listed in `.gitignore`.

3. (Optional) Open the folder in VS Code and choose **Reopen in Container**, or open it in GitHub Codespaces, to work in the same environment the stack runs in.

## Running the pipeline

Start the whole stack with one command:

```bash
docker compose up -d
```

Check that the services are healthy:

```bash
docker compose ps
```

Stop the stack cleanly:

```bash
docker compose down
```

To also remove stored volumes (this deletes all data):

```bash
docker compose down -v
```

> Confirm the exact commands, profile names (for example a reduced demo profile) and start-up order against the final `docker-compose.yml`, and update this section.

### Simulator settings

The simulator is configurable: number of devices and sites, event rate, and fault-injection percentages for late, duplicate, malformed and out-of-range events. Baseline: 500 devices sending one reading per second (about 500 events/s), with peak tests at 1,000 events/s. Settings are read from environment variables or the simulator config file. *(Document the exact variable names here.)*

## Service endpoints

| Service | Address | Notes |
|---|---|---|
| Airflow | http://localhost:8080 | DAG UI |
| Grafana | http://localhost:3000 | Dashboard |
| SQL Server | `localhost,1433` | Connect with SSMS or Azure Data Studio |

Credentials come from your `.env` file. Services are meant to be reachable on the local network only.

## Event contract

Every event carries the fields below. The machine-readable schema is in `src/contracts/sensor_event.schema.json`, and test cases are in `tests/fixtures/contract_test_cases.jsonl`.

| Field | Type | Rule |
|---|---|---|
| `event_id` | string | Required; UUID created by the producer, reused when a send is retried |
| `device_id` | string | Required; must exist in the device registry |
| `site_id` | string | Required; must match the site registered for the device, otherwise dead-letter |
| `sensor_type` | string | One of `temperature`, `humidity`, `co2`, `power`; unknown types go to the dead-letter queue |
| `value` | double | Required, not null; temperature -40 to 85 °C, humidity 0 to 100 %, CO2 0 to 5,000 ppm, power 0 to 5,000 W |
| `unit` | string | Matches the sensor type (°C, %, ppm, W) |
| `event_ts` | timestamp | Required; ISO 8601 in UTC with `Z`; rejected if more than 5 s in the future |

Other rules: missing or null required fields go to the dead-letter queue; extra fields are dropped and counted. Each record also carries four UTC timestamps for measurement: event time, ingest time (Kafka append time), processed time and served time.

## Airflow DAGs

| DAG | Purpose |
|---|---|
| `stream_health_check` | Samples the health of Kafka, the streaming job, SQL Server, Airflow and the dashboard |
| `dq_daily_report` | Runs the Great Expectations suites and produces the daily data-quality report |
| `lake_compaction` | Compacts small Parquet files in the lake |
| `gold_backfill` | Corrects aggregates using events from the late-events path |
| `warehouse_maintenance` | Routine maintenance of the SQL Server serving tables |

Each DAG runs on a schedule and on demand, with logged results.

## Data quality

Quality is checked at two levels:

- **Inside the stream (Spark):** fast per-event structural checks (schema, types, ranges) and dead-letter routing.
- **After storage (Great Expectations):** expectation suites validate the stored clean Parquet and SQL Server tables, run on a schedule through `dq_daily_report` and on demand, with stored validation results and generated data docs.

A reconciliation check compares produced, stored, rejected and duplicate-suppressed event counts.

## KPIs

Targets are initial values, validated in the benchmark run and reported in the test report.

| KPI | Target |
|---|---|
| Processing latency (Kafka ingest time to served time) | p95 of 10 s or less; median of 5 s or less |
| Throughput | 500 events/s sustained; 1,000 events/s peak |
| Data freshness | 15 s or less |
| Error / dead-letter rate | 100% of injected invalid events captured with a reason; under 0.1% pipeline-caused failures |
| Pipeline availability | 99% or more over a 2-hour soak |
| Recovery time | 60 s or less per tested service |
| Data completeness (extra) | 100% |
| Duplicate rate (extra) | 0 |
| Consumer lag (extra) | 5 s or less under normal load |

The benchmark runs three scenarios: normal load, peak load, and a fault-injection run with late, duplicate and malformed events. Results and the machine specification go into `docs/testing/`.

## Testing

Run the automated tests:

```bash
pytest tests/
```

> Update with the exact command (inside a container or a virtual environment) once the test layout is final.

Tests cover unit behaviour, data quality checks, late, duplicate and malformed events, the Great Expectations validation, and a clean-start run of the documented Docker Compose command. The test plan, test report and bug reports are in `docs/testing/`.

## Troubleshooting

| Problem | What to try |
|---|---|
| A container exits or restarts repeatedly | Check `docker compose logs <service>`. Low memory is the most common cause, especially for SQL Server. |
| SQL Server will not start | Confirm the password in `.env` meets SQL Server's complexity rules and that Docker has enough RAM. |
| A port is already in use | Change the host port in `.env` or stop the program using it. |
| Dashboard shows no data | Check that the simulator and the streaming job are running, then check the freshness and quality panels. |
| Consumer lag keeps growing | Lower the simulator rate or use the reduced demo profile. |

Shared troubleshooting notes live in `docs/final/`.

## Documentation

| Stage | Location | Due |
|---|---|---|
| Planning, literature review, requirements | `docs/planning/`, `docs/requirements/` | 19 Oct 2026 |
| System analysis and design | `docs/design/` | 6 Nov 2026 |
| Implementation | `src/`, `tests/`, this README | 30 Nov 2026 |
| Testing, final presentation and reports | `docs/testing/`, `docs/final/` | 4 Dec 2026 |

Internal feature freeze: 23 Nov 2026.

## Team and workflow

| Member | Role |
|---|---|
| Member 1 | Team lead, documentation, presentation |
| Member 2 | Kafka and simulator |
| Member 3 | PySpark and data quality |
| Member 4 | Storage, ER and dashboard |
| Member 5 | QA, DevOps and GitHub |

*(Replace "Member N" with names when the team is ready to publish them.)*

Working agreements:

- One feature branch per task, merged through a pull request reviewed by someone other than the author.
- Meaningful commit messages and pull request descriptions.
- Interfaces (event schema, topic names, table names) are fixed early. Changes go through an issue and a pull request.
- No credentials, data dumps or large files are committed.

## Project status

| Milestone | Status |
|---|---|
| Planning, literature review, requirements | Submitted |
| System analysis and design | In progress |
| Implementation | Not started |
| Testing, final presentation and reports | Not started |

Docker environment: built and working (smoke test passes).

---

*All sensor data is simulated. No personal or sensitive data is involved. SQL Server Developer edition is for development and demonstration only.*
