# AtlasFlow manual runbook

Run each of these eight steps in order from PowerShell in the repository root.

## 1. Prepare the local environment and start infrastructure

```powershell
docker compose up -d --build
```

The command builds Flink and Superset images and starts the Compose stack.

## 2. Prepare PostgreSQL CDC

The PostgreSQL image initializes `atlasflow_ops`, the seven operational tables, and seed records when the project volume is first initialized. The schema explicitly switches to `atlasflow_ops` before table creation.

```powershell
.\scripts\setup-cdc-publications.ps1
```

This creates or validates `atlasflow_publication` for the seven source tables.

## 3. Create the business Kafka topic

```powershell
.\scripts\create-connect-topics.ps1
```

This creates `atlasflow.cdc.v1`. Kafka Connect internal topics are separate infrastructure.

## 4. Start Bronze and register Debezium

Start Bronze first so it is reading Kafka before Debezium emits the initial snapshot:

```powershell
.\scripts\submit-streaming-stage.ps1 -Stage Bronze
.\scripts\register-debezium-connectors.ps1
.\.venv\Scripts\python.exe .\scripts\wait-for-cdc-snapshot.py --table atlasflow.bronze.cdc_events --minimum-rows 34 --timeout 300
```

The initial 34 records are 6 customers, 8 orders, 8 inventory balances, 4 carriers, and 8 shipments. Tracking and disruption events are added later.

## 5. Start Silver CDC and its eight streaming models

```powershell
.\scripts\submit-streaming-stage.ps1 -Stage Silver
.\.venv\Scripts\python.exe .\scripts\wait-for-cdc-snapshot.py --table atlasflow.silver.cdc_changes --minimum-rows 34 --timeout 300
```

The Silver job normalizes the Bronze snapshot and follows new Bronze appends. Its helper views `latest_versions` and `current_rows` are temporary only.

## 6. Start the continuous Gold stream

```powershell
.\scripts\start-gold-stream.ps1
```

This creates a keyed Iceberg Gold table and submits `atlasflow-gold-control-tower-stream`. Gold starts by scanning the existing Silver CDC history, then follows new appended changes and upserts the shipment control tower. Gold reads that CDC history directly because Iceberg streaming reads do not provide a CDC changelog of updates/deletes from the keyed Silver output tables.

## 7. Generate live business events and query Gold

```powershell
.\.venv\Scripts\python.exe .\generators\generate_logistics_data.py --scenario baseline --count 12 --seed 7
.\.venv\Scripts\python.exe .\generators\generate_logistics_data.py --scenario fault-bundle
.\.venv\Scripts\python.exe .\scripts\wait-for-demo-events.py --timeout 180
.\.venv\Scripts\python.exe .\scripts\run-trino-query-pack.py --file .\sql\atlasflow_queries.sql
```

The baseline inserts tracking events. The fault bundle updates `SHP-008` and inserts an exception tracking event plus a port-congestion disruption. Those changes move through PostgreSQL, Debezium, Kafka, Bronze and Silver, and then update Gold without rerunning a refresh script.

## 8. Dashboards and observability

### Superset control tower

1. Open `http://localhost:8088` and sign in with `SUPERSET_ADMIN_USERNAME` and `SUPERSET_ADMIN_PASSWORD` from `.env`.
2. Add a database connection with SQLAlchemy URI `trino://atlasflow@trino:8080/atlasflow/gold`, then test and save it. This hostname is for containers on the Compose network, not the Windows host.
3. Add the `shipment_control_tower` dataset from schema `gold`.
4. Create charts for shipment count by `risk_level`, a table filtered to `risk_level = HIGH`, and shipment/delivery counts by `carrier_name`. Build a dashboard named `ABC Logistics Control Tower`.

### Observability

- Kafka UI: `http://localhost:8090` — inspect `atlasflow.cdc.v1` and offsets.
- Flink: `http://localhost:8081` — confirm four long-running jobs: Bronze, Silver CDC, Silver current-state models and Gold.
- Prometheus: `http://localhost:9090` — check scrape targets and metrics.
- Grafana: `http://localhost:3000` — open the provisioned platform-observability dashboard; use `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` from `.env`.
- Trino: `http://localhost:18080` — query Iceberg-backed tables.
- Iceberg REST: `http://localhost:18181/v1/config` — catalog health/config endpoint.

This is a local educational stack. Do not expose its services publicly or reuse local credentials elsewhere.
