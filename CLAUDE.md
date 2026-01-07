# Capital Bikeshare Analytics — CLAUDE.md

This document describes the intent, architecture, and guiding principles of the Capital Bikeshare analytics project.  
It is written to orient both human contributors and AI assistants to the *what*, *why*, and *how* of the system.

---

## WHAT

This project builds a scalable, reproducible analytics pipeline and interactive dashboard for publicly available Capital Bikeshare trip data (2010–present).

The system:

- Ingests monthly Capital Bikeshare trip data from the public AWS S3 bucket
- Reconciles **schema changes pre- and post-April 2020**
- Stores data in **columnar Parquet format on S3**
- Produces lightweight, aggregated summary tables for fast analytics
- Serves an **interactive data exploration app** focused on:
  - System-level trends
  - Station-level demand and flow patterns
  - Individual bike journey trajectories (where bike IDs exist)

The project is intentionally scoped as a **data product**, not a general-purpose website.

---

## WHY

This project exists to demonstrate:

1. **Data engineering fundamentals**
   - Schema normalization across evolving datasets
   - Efficient storage and query patterns for large datasets
   - Separation of raw data, transformed data, and analytics outputs

2. **Analytical thinking**
   - Demand patterns by time of day, season, and station
   - Inference of station pressure (full / empty risk) without explicit capacity data
   - Interpretation of behavioral differences (member vs casual, classic vs electric)

3. **Pragmatic technology choices**
   - Avoiding unnecessary big-data tooling (e.g., Spark) when single-node solutions suffice
   - Favoring simplicity, performance, and explainability

4. **Product mindset**
   - Delivering a usable, interactive tool rather than static notebooks
   - Designing views around user questions, not just available data

The project is designed to complement a separate Flask-based basketball analytics site by showcasing **geospatial analytics, data pipelines, and dashboarding** using a different stack.

---

## HOW

### Data Storage & Architecture

- **Raw and processed data live in AWS S3**
- **Parquet** is used everywhere for:
  - Columnar storage
  - Predicate pushdown
  - Compatibility with multiple engines

This S3 + Parquet layout functions as a small-scale **data lake**.

Local storage is used only for:
- Small sampled datasets (development)
- Cached summary tables (optional)

Raw data is never committed to the repository.

---

### Ingestion Pipeline

- Data is pulled from:
  `https://s3.amazonaws.com/capitalbikeshare-data/`
- Monthly ZIP files are downloaded, extracted, cleaned, and written to:
  `s3://capital-bikeshare-public/`

Key ingestion logic lives in:
src/capitalbike/data/ingest.py

A CLI wrapper (`scripts/pull_data_from_cabi.py`) orchestrates:
- Incremental monthly updates
- Full historical refreshes
- Downstream transformation triggers

---

### Transformation & Modeling

The dataset changes structure in April 2020:

- **Pre-2020**:
  - Includes unique bike numbers
  - No lat/lng coordinates
- **Post-2020**:
  - Includes lat/lng and rideable type
  - No bike numbers (especially for electric bikes)

Transformations:
- Normalize column names and types
- Compute derived fields (e.g., duration)
- Separate logic for:
  - Bike-level data (pre-2020)
  - Station-level spatial metadata (post-2020)

Transformation logic lives in:
src/capitalbike/data/transform.py
src/capitalbike/data/summarize.py

---

### Query & Compute Engine Choices

This project intentionally **does not use Spark or Databricks**.

Instead, it relies on:

- **Polars (lazy execution)** for transformations
- **DuckDB** for fast analytical queries over Parquet
- **Single-node parallelism**, which is sufficient for ~30M rows

This choice:
- Reduces complexity
- Improves developer velocity
- Matches real-world trends toward lightweight analytics engines

---

### Dashboard & Visualization Layer

The interactive app is built with **Streamlit**.

Why Streamlit:
- Python-native
- Rapid development
- Well-suited for analytical tools and dashboards
- Minimal front-end overhead

The app reads **only pre-aggregated summary tables**, not raw trip data.

Dashboard modules live in:
src/capitalbike/app/streamlit/

Reusable visualization logic lives in:
src/capitalbike/viz/

---

### Automation & Scheduling

Data ingestion is designed to run on a **monthly cadence**.

Supported execution modes:
- Local CLI execution
- GitHub Actions scheduled workflow (preferred for automation)

This provides:
- Reproducibility
- Auditability
- Hands-off maintenance

---

## GUIDING PRINCIPLES

- **Do not load full datasets into memory**
- **Prefer lazy execution and pushdown filters**
- **Precompute summaries for interactive use**
- **Avoid overengineering**
- **Optimize for clarity, not cleverness**

If a tool or abstraction does not materially improve:
- correctness
- performance
- or clarity

…it should not be added.

---

## NON-GOALS

This project does **not** aim to:
- Be a production SaaS
- Handle user authentication
- Serve real-time bike availability
- Replace Capital Bikeshare’s internal systems

It is a **demonstration of sound data engineering and analytics design**, not an enterprise platform.

---

## EXPECTATIONS FOR AI ASSISTANTS

When contributing to or modifying this project:

- Respect existing architectural decisions
- Do not introduce Spark, Databricks, or unnecessary cloud services
- Favor Polars 0.20, DuckDB, and Parquet
- Keep functions small and testable
- Preserve the WHAT / WHY / HOW structure when adding new components

---

End of document.