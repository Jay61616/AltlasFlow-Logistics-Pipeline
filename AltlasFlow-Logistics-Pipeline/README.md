# AtlasFlow — YouTube Edition

A practical end-to-end data engineering project for a fictional global logistics company, ABC Logistics. Operational changes flow through PostgreSQL, Debezium, Kafka, Flink SQL and Apache Iceberg, then become shipment-risk analytics in Trino and Superset. Prometheus and Grafana provide observability. Bronze, Silver CDC, Silver current-state models and Gold are submitted as Flink SQL jobs from PowerShell.

## Architecture

```text
Python logistics generator
          |
          v
PostgreSQL atlasflow_ops (7 source tables)
          | WAL / logical replication
          v
Debezium PostgreSQL connector
          |
          v
Kafka: atlasflow.cdc.v1
          |
          v
Bronze: raw CDC envelopes in Iceberg
          |
          v
Silver CDC: normalized change history in Iceberg
          |--------------------------------------|
          v                                      v
Silver current-state models                Gold streaming model
(8 keyed Iceberg upsert tables)            (keyed shipment-risk table)
                                                 |
                                                 v
                                       Trino --> Superset

Prometheus + Grafana monitor the services; Kafka UI and Flink UI expose pipeline activity.
```

The generator writes tracking/disruption rows and shipment updates to PostgreSQL. It does not publish directly to Kafka. Debezium captures the database changes and routes them to the single business topic; Kafka Connect's internal topics are separate infrastructure.

## Source tables

| PostgreSQL table | Purpose |
|---|---|
| `customers` | Customer name, segment and country |
| `orders` | Customer order, SKU, quantity and delivery promise |
| `inventory_balances` | On-hand, reserved and damaged stock by warehouse/SKU |
| `carriers` | Carrier name, service level and region |
| `shipments` | Order/carrier linkage, warehouse and status |
| `tracking_events` | Generated shipment milestones and delay/exception events |
| `disruptions` | Generated route/port disruption records |

All seven tables are in `atlasflow_ops`. The schema initializes on the first start of the project's isolated PostgreSQL volume.

## SQL layers

| Layer | Main template | Purpose |
|---|---|---|
| Bronze | `sql/bronze/01_cdc_to_bronze.sql.template` | Persist raw Debezium envelopes with Kafka topic/partition/offset metadata |
| Silver CDC | `sql/silver/01_cdc_to_silver.sql.template` | Normalize seven row shapes into shared change history |
| Silver models | `sql/silver/maintain_silver_models.sql.template` | Continuously maintain eight keyed current-state tables |
| Gold | `sql/gold/stream_shipment_control_tower.sql.template` | Continuously maintain one shipment row with a risk level and primary reason |

Silver model outputs are `customers`, `orders`, `shipments`, `carriers`, `inventory`, `tracking_ranked`, `latest_tracking`, and `disruption_summary`. The helper views `latest_versions` and `current_rows` remain temporary and are not persisted as separate tables.

Gold runs as a long-lived Flink streaming query and writes keyed updates to `atlasflow.gold.shipment_control_tower`. The Gold query reads the append-only `atlasflow.silver.cdc_changes` history directly to receive new change records. This is intentional: Iceberg streaming reads follow append-only snapshots and do not provide a CDC changelog of updates/deletes from the keyed Silver model tables. The same CDC history maintains the eight Silver model outputs in parallel. Gold retains the original shipment-risk precedence and includes `risk_level`, `primary_risk_reason`, `refreshed_at`, and `refresh_id` (`STREAMING`).

## First installation / same-repository replacement

Requirements: Windows 10/11, Docker Desktop with Compose v2, PowerShell 5.1 or 7, and Python 3.10+. Start Docker Desktop first.

1. Extract this ZIP to a staging folder.
2. In the existing project, run `docker compose down` without `-v`.
3. From the extracted staging folder, run `powershell -ExecutionPolicy Bypass -File .\install-in-place.ps1 -TargetPath "$env:USERPROFILE\Desktop\AtlasFlow_YouTube_Edition"`. Type `REPLACE` when prompted. The installer preserves the existing `.git` entry, replaces source files in place and removes local `.env`, `.venv`, logs and rendered SQL. Docker volumes are not deleted.
4. In the existing repository, run:

```powershell
.\scripts\prepare-local-env.ps1
docker compose up -d --build
```

This creates local-only credentials in `.env` and prepares the Python environment. The Compose project keeps the existing `atlasflow-lite-*` volume/container identity to avoid an accidental second copy of the project. Do not run another edition simultaneously because host ports overlap.

## Run the eight manual steps

Follow the commands in [`MANUAL-RUNBOOK.md`](MANUAL-RUNBOOK.md), in order: infrastructure; PostgreSQL publication; Kafka topic; Bronze and Debezium initial snapshot; Silver normalization plus the eight current-state model stream; Gold streaming; generate live business events and query with Trino; dashboard and observability.

The initial Debezium snapshot contains 34 records: 6 customers, 8 orders, 8 inventory balances, 4 carriers, and 8 shipments. Tracking and disruption records are generated after the streaming jobs are running.

## Files to explain on camera

- `docker/postgres/init/10-atlasflow-schema.sql` — seven source tables and relationships.
- `generators/generate_logistics_data.py` — baseline and fault-bundle scenarios; all changes go to PostgreSQL first.
- `connectors/postgres-cdc-connector.json` — one connector, publication and routing rule.
- `sql/bronze/01_cdc_to_bronze.sql.template` — raw landing and Kafka metadata.
- `sql/silver/01_cdc_to_silver.sql.template` — JSON extraction, entity keys, operation and quality labels.
- `sql/gold/stream_shipment_control_tower.sql.template` — streaming joins, inventory availability and risk precedence.
- `sql/atlasflow_queries.sql` — nine dashboard and validation examples.

## Service URLs

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Kafka Connect API | http://localhost:8083/connectors |
| Flink dashboard | http://localhost:8081 |
| Iceberg REST | http://localhost:18181/v1/config |
| Trino | http://localhost:18080 |
| Superset | http://localhost:8088 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

Superset and Grafana credentials are in `.env`. Superset includes the Trino SQLAlchemy driver, and Grafana loads the provisioned platform-observability dashboard.

## Validation

Static validation:

```powershell
.\.venv\Scripts\python.exe .\scripts\check-atlasflow-static.py
```

Optional live check after all streaming jobs are running:

```powershell
.\scripts\verify-atlasflow.ps1
```

Static checks validate repository contracts and syntax, not every Flink SQL runtime semantic. The new streaming SQL still needs a Docker integration run. This remains a local educational stack; do not expose its services publicly or reuse local credentials elsewhere.
