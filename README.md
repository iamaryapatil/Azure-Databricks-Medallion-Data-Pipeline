# Azure-Databricks-Medallion-Data-Pipeline

This project extends a real-world ETL automation problem into a cloud-native Azure data engineering pipeline using Databricks, Delta Lake, Unity Catalog, ADLS Gen2, and Azure Data Factory orchestration.

## Overview

This project implements an end-to-end Azure data engineering pipeline using a Medallion architecture (Bronze → Silver → Gold) to ingest, transform, and model Australian migration and visa datasets into analytics-ready Delta Lake tables.

The pipeline is orchestrated using Azure Data Factory and executed in Azure Databricks, with data stored in Azure Data Lake Storage Gen2 and governed through Unity Catalog.

The goal of this project is to demonstrate production-style cloud data engineering workflows including layered ETL design, Delta Lake processing, orchestration pipelines, and catalog-based data governance.

---

## Architecture

![Pipeline Architecture](Azure-Databricks-Medallion-Data-Pipeline/screenshots/Bronze Tables.png)

### Stack Used

| Layer | Technology |
|------|-------------|
Storage | Azure Data Lake Storage Gen2 |
Compute | Azure Databricks |
Format | Delta Lake |
Governance | Unity Catalog |
Orchestration | Azure Data Factory |
Language | Python / PySpark |
Framework | Medallion Architecture |

---

## Medallion Architecture Design

### Bronze Layer

Raw ingestion from public migration datasets.

Features:

- Automated dataset retrieval
- Pivot cache XML extraction
- Schema preservation
- Partition metadata tracking
- Delta format storage


Screenshot:

![Bronze Tables](screenshots/04_bronze_tables_registered.png)

---

### Silver Layer

Cleaned and standardized transformation layer.

Transformations applied:

- column normalization
- datatype casting
- partition readiness
- categorical cleanup
- date parsing
- schema alignment across datasets

Validation:

![Silver Validation](screenshots/07_silver_layer_validation.png)

---

### Gold Layer

Analytics-ready KPI modelling layer for reporting and dashboards.


Example:

![Gold Tables](screenshots/06_gold_tables_registered.png)

---

## Azure Data Factory Orchestration Pipeline

ADF controls execution order across layers:

Bronze Ingestion -> Silver Transformations -> Gold KPI Modelling

## Example Dataset Processed

Pipeline processes multiple migration datasets including:

- student visa grants
- graduate visa lodgements
- skilled work visa holders
- working holiday visas
- visitor visas
- overseas arrivals and departures

---

## Engineering Highlights

This pipeline demonstrates:

-  Medallion architecture implementation  
-  Delta Lake storage design  
-  Azure Data Factory orchestration  
-  Unity Catalog integration  
-  External location governance  
-  Schema standardisation across datasets  
-  Partition-ready transformations  
-  KPI modelling layer construction  
-  Multi-dataset ingestion automation  

---
