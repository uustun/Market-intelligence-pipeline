# Market Intelligence Pipeline (UK Companies House)

## Overview

This project implements a practical market intelligence pipeline using official UK Companies House data.  
It was built to support recurring commercial analysis of UK SMEs by industry (SIC code) and location.

The pipeline ingests company data via the Companies House API, normalises it into a SQL Server database, and produces a monthly extract of newly incorporated companies for downstream analysis and outreach.

The design prioritises data quality, idempotency, and operational reliability over raw scraping volume.

## Target Scope

- **Geography:** Luton–Milton Keynes corridor and surrounding towns  
- **Company status:** Active companies only  
- **Incorporation window:** 2018 to present  
- **Industries (SIC codes):**
  - 62020 – Information technology consultancy activities  
  - 62012 – Business and domestic software development  

These SIC codes were selected to align with  data engineering, consulting, and software services activity in the region.

## Data Source

UK Companies House API (Advanced Search)

Only officially registered and active companies are included.

## Pipeline Structure

### 1. Backfill ingestion
- One-off historical ingestion from **2018 → present**
- Unique upserts using company number as the primary key
- Normalised schema (companies, addresses, SIC codes)

### 2. Monthly incremental ingestion
- Runs on the **1st of each month**
- Automatically processes companies incorporated in the **previous month**
- Updates existing companies and presents brand new corporations

### 3. Output
- CSV export of newly incorporated companies for the month
- Stored locally under `data/exports/`
- Designed for downstream commercial, financial, or market analysis

## Database Design

- Microsoft SQL Server
- Normalised schema:
  - `companies`
  - `company_addresses`
  - `company_sic`
  - `sic_codes`
  - `ingestion_log`

Each ingestion run is logged with a unique run ID, timestamp, and record counts for transparency.

## Automation

The incremental pipeline is designed to run unattended for batch of the previous month's using Windows Task Scheduler.

- Schedule: Monthly (1st of the month)
- Output: One CSV per run
- No duplication on reruns

## Tools & Technologies

- Python
- SQL Server
- Companies House API
- Pandas (for exports)
- Git
- requests
- python-dotenv
- pydantic
- pandas
- sqlalchemy
- pyodbc


## Notes

This repository reflects a production-style data pipeline built with maintainability and auditability in mind, rather than a one-off scraping script.
