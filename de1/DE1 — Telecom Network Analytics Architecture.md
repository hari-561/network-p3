# DE1 — Telecom Network Analytics Architecture

## Objective

Build a reliable data platform that processes daily telecom activity CSV files, enriches them with static Milan grid reference data, produces trusted grid-level analytics, and exposes those results to React, ML, and Claude-based consumers.

## Architecture

Daily `sms-call-internet-mi-YYYY-MM-DD.csv` files enter the landing layer unchanged. Airflow orchestrates validation and pipeline execution. Files passing structural and quality checks are accepted into the raw layer, while invalid files are isolated in a rejected/quarantine area. Spark performs cleaning, country-code aggregation, grid enrichment, hourly and daily aggregation, baseline calculations, hotspot detection, and preparation of analytical datasets. Processed datasets are stored primarily as Parquet, while curated analytics are exposed through SQL tables where structured querying and serving are required. FastAPI exposes approved analytical and ML results, React provides the interactive dashboard, and Claude provides natural-language interpretation over approved analytics.

`milano-grid.geojson` is maintained separately as static reference data and is not treated as a daily ingestion file. It provides grid boundaries and reference information used during enrichment.

## Data Layers

| Layer | Format | Responsibility |
|---|---|---|
| Landing | CSV | Receive daily source files |
| Raw | CSV | Preserve validated source data |
| Rejected | CSV + metadata | Isolate invalid input |
| Reference | GeoJSON | Store static Milan grid reference |
| Processed | Parquet | Store cleaned/transformed analytical data |
| Analytics | SQL tables / Parquet | Store trusted analytical outputs |

## Quality Gates

### Raw acceptance gate

A file must be readable, follow the expected filename/schema, contain valid timestamps, match the expected processing date, contain exactly 24 hourly intervals, and satisfy source-level completeness and duplicate checks before entering the raw layer.

### Analytics publication gate

Processed data must satisfy schema, completeness, null, value-range, uniqueness, aggregation-grain, baseline, alert, and risk-score checks before analytics are published to downstream consumers.

Failed files are quarantined and failed analytical outputs are not published as trusted results.

## Analytics Outputs

### hourly_grid_summary
One row per `grid_id` and hourly timestamp containing aggregated activity measures.

### daily_grid_summary
One row per `grid_id` and day containing daily activity statistics and supporting measures.

### hotspots
Grid-hour records where activity is unusually high relative to the applicable activity baseline.

### activity_alerts
Operational activity anomaly records containing alert severity and supporting measurements.

### risk_scores
ML-generated risk predictions containing the grid/time context, risk score, risk level, and model version.

High activity is treated as an activity signal rather than confirmed network congestion because the available dataset does not contain network utilization, throughput, latency, or packet-loss measurements.

## Component Responsibilities

- **Spark:** Performs distributed cleaning, transformation, enrichment, aggregation, and analytical processing.
- **SQL:** Stores and queries curated analytical and dimensional data.
- **Airflow:** Orchestrates scheduling, dependencies, retries, and execution order.
- **FastAPI:** Exposes approved analytics and ML results through APIs.
- **React:** Presents API results through an interactive user interface.
- **ML:** Produces predictive risk scores from prepared analytical features.
- **Claude:** Provides natural-language interpretation and question answering over approved analytics.

## Assumptions

1. One daily CSV represents one expected processing date.
2. Each source file should contain 24 hourly intervals.
3. `grid_id` is the canonical spatial identifier after grid enrichment.
4. The analytical hourly grain is `(grid_id, timestamp)`.
5. `milano-grid.geojson` is relatively static reference data and is updated independently from daily activity files.
6. Processed analytical data is stored in Parquet for Spark-oriented processing.
7. SQL is used where curated structured querying and serving are beneficial.
8. Activity-based alerts indicate unusual activity and do not prove physical network congestion.
9. Airflow orchestrates the pipeline but does not contain the core Spark transformation logic.
10. Downstream consumers only access analytics that have passed the publication quality gate.

## Non-Goals

1. Real-time network telemetry or packet-level monitoring.
2. Proving actual network congestion from activity counts alone.
3. Replacing Spark transformations with Airflow task logic.
4. Treating the static GeoJSON reference as daily source data.
5. Building a fully streaming architecture in this phase.
6. Allowing React, Claude, or other consumers to directly modify analytical datasets.
7. Building ML models before the required analytical features and quality controls are available.

## End-to-End Flow

`Daily CSV → Landing → Validation → Raw → Spark Processing → Processed Parquet → Analytics Quality Gate → Analytics/Warehouse → FastAPI → React/Claude`

`milano-grid.geojson → Reference Layer → Spark Enrichment → Processed/Analytics`