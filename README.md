# Azure-Databricks-Medallion-Data-Pipeline

This project extends a real-world ETL automation problem into a cloud-native Azure data engineering pipeline using Databricks, Delta Lake, Unity Catalog, ADLS Gen2, and Azure Data Factory orchestration.

## Overview

This project implements an end-to-end Azure data engineering pipeline using a Medallion architecture (Bronze → Silver → Gold) to ingest, transform, and model Australian migration and visa datasets into analytics-ready Delta Lake tables.

The pipeline is orchestrated using Azure Data Factory and executed in Azure Databricks, with data stored in Azure Data Lake Storage Gen2 and governed through Unity Catalog.

The goal of this project is to demonstrate production-style cloud data engineering workflows including layered ETL design, Delta Lake processing, orchestration pipelines, and catalog-based data governance.

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

<img width="1000" height="400" alt="Bronze Tables" src="https://github.com/user-attachments/assets/86e99707-d562-4082-9f4a-f288b93589af" />


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
<img width="1000" height="400" alt="Silver Tables" src="https://github.com/user-attachments/assets/49bd30f3-89ea-4cdc-94fa-070da776533d" />


### Gold Layer

Analytics-ready KPI modelling layer for reporting and dashboards.

<img width="1000" height="400" alt="Gold Tables" src="https://github.com/user-attachments/assets/2f06e45e-ddbc-4c79-99c0-7440bfe2181e" />


## Azure Data Factory Orchestration Pipeline

ADF controls execution order across layers:

Bronze Ingestion -> Silver Transformations -> Gold KPI Modelling

<img width="1000" height="400" alt="Azure Data Factory Pipeline Orchestration" src="https://github.com/user-attachments/assets/6826aecf-6d00-4663-95f1-d95e25b8ba54" />

## Example Dataset Processed

Pipeline processes multiple migration datasets including:

- student visa grants
- graduate visa lodgements
- skilled work visa holders
- working holiday visas
- visitor visas
- overseas arrivals and departures


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

 
