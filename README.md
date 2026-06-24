Retail ETL Pipeline
A complete data engineering project that transforms raw UK e-commerce transaction data into a clean, dimensionally modelled dataset with a Power BI dashboard. The entire pipeline runs locally using free, open-source tools within Docker containers.

Project Overview
The pipeline ingests a public retail transactions dataset containing approximately 500,000 rows. PySpark performs data cleaning and transformation through a layered architecture (raw to bronze to silver). Data quality validation occurs before building a star schema in PostgreSQL. Power BI connects directly to the star schema for visualisation. Apache Airflow orchestrates the entire workflow and Docker manages the environment configuration.


The pipeline executes as a single Airflow DAG with each stage dependent on successful completion of the previous stage. A data quality gate halts the pipeline if validation checks fail.

Technology Stack
Component	         Purpose
PySpark	            Data cleaning, transformation, and star schema modelling
PostgreSQL	         Star schema storage
Apache Airflow	      Pipeline orchestration and scheduling
Docker 	            Containerisation for consistent local execution
Power BI	            Dashboard and visualisation layer

Dataset
The project uses the Online Retail Dataset containing real anonymised transaction data from a UK based online retailer. The dataset includes invoice numbers, product details, quantities, prices, customer identifiers and country information.

Pipeline Stages
Bronze Layer
Converts the raw CSV file into Parquet format. No business logic is applied at this stage; the conversion serves as a structural improvement for enhanced Spark performance.

Silver Layer
Applies data cleaning rules including removal of rows without customer identifiers, elimination of returns and cancellations (negative quantities), duplicate removal, correction of price formatting issues and derivation of new columns including line item revenue, invoice month and invoice year.

Data Quality Gate
Validates the silver layer data by checking for null values in critical columns ensuring revenue values are non-negative and confirming the dataset is non-empty. The star schema build proceeds only after all validation checks pass.

Star Schema
Constructs a dimensional model from the silver data comprising a fact_sales table (one row per transaction line) referencing dimension tables for customers, products and dates. This model serves as the authoritative data source for Power BI.

Orchestration
An Airflow DAG chains the PySpark scripts with explicit dependencies in a linear pipeline.

Dashboard
Power BI connects directly to the star schema tables and visualises key performance indicators including revenue trends, top performing countries, leading products and highest value customers. All aggregations are computed by Power BI against the fact and dimension tables.

Configuration
Database and Airflow credentials are supplied via environment variables. Copy .env.example to .env and populate with your credentials before running the project:


Execution Instructions
Clone this repository and verify Docker Desktop is installed and operational.

Copy .env.example to .env and populate with your credentials.

From the project root directory, execute:
docker-compose up --build
Upon seeing "Listening at: http://0.0.0.0:8080" in the logs, navigate to http://localhost:8080 and authenticate using the admin credentials specified in your .env file.

Activate the retail_etl_pipeline DAG and trigger it manually.

After all tasks complete successfully, connect Power BI Desktop to PostgreSQL (localhost:5433, database airflow) using the credentials from your .env file to explore the results or open powerbi/retail_dashboard.pbix directly.

Author
Mpho Moloto
GitHub: https://github.com/mpho-moloto
LinkedIn: www.linkedin.com/in/mpho-moloto-0a996228b
