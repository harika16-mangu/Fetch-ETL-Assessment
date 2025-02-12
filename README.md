# Fetch-ETL-Pipelining

### Table of Contents
<a name="1.Tech Stack"></a>
<a name="2.Description"></a>
<a name="3.Prerequisites"></a>
<a name="4.Run Data transformation (Jupyter Notebook)"></a>
<a name="5.Installation Guide"></a>
<a name="6.Setup Apache Airflow"></a>
<a name="7.Run the ETL pipeline"></a>
<a name="8.Verify data in Snowflake"></a>
<a name="9.Run Data Quality Checks"></a>
<a name="10.Setup Tableau for Visualization"></a>
<a name="11.Conclusion"></a>

### Project Title:
* End-to-End ETL Pipelining *

### 1.Tech stack:
1. Python
2. Apache Airflow
3. Snowflake
4. Tableau

### 2.Description:
This project demonstrates the ETL pipeline that extract data from raw json files to driving business insights and recommendations
Workflow:
1. ** Data Integration **: Extracted data from JSON sources and transformed/normalized it to structured CSV files using Python in Jupyter notebook.
2. ** Data Orchestration **: Utilized Apache Airflow to orchestrate the structured normalized CSV files through scheduled ETL pipelines to Snowflake.
3. ** Data Warehousing & Transformation **: Stored, transformed, and performed data quality checks and basic data analysis using SQL in Snowflake.
4. ** Visualization & Dashboarding **: Visualized the transformed data and created interactive dashboards using Tableau.

* Please check the Architecture_Workflow diagram for detailed workflow *

### 3.Prerequisites
Install Required Tools
1. Python 3.8 -[Download](https://www.python.org/downloads/)
2. WSL (Windows Subsystem for Linux) (if running on Windows)
3. Apache Airflow -> Installed in WSL
4. Snowflake Account-[Sign In](https://app.snowflake.com/)
5. Tableau Desktop -[Download](https://www.tableau.com/support/releases)

### 4.Run Data transformation (Jupyter Notebook)
1.Open notebooks/json_to_csv_datatransformation.ipynb
2.This notebook is specially designed to handle data extraction and flatten unstructure json data into structured CSV. This code will also perform transformation of data before pipelining
3.Save the CSV files and proceed downloading for future references.
***Explanation: Given json files users,brands,receipts are future normalized until 3NF,this resulted in creation of 8 tables users,brands,receipts,receipt_items,products,rewards,userflagged_items,metabrite_items.***

### 5.Installation Guide
1. Clone the Repository
![Cloning Git Repo in WSL](image.png)
![current directory](image-1.png)
2. Create and Activate a Virtual Environment
![Virtual Environment](image-2.png)
3. Install Dependencies
![Install Dependencies](image-3.png)

### 6.Setup Apache Airflow
1. Initialize Airflow
![Initializing db and airflow](image-4.png)
2. Start Airflow Webserver & Scheduler
![Connect to port:8080](image-5.png)
3. Add Snowflake Connection in Airflow
![SnowflakeConnection](image-6.png)

### 7.Run the ETL Pipeline
Once Airflow is running, trigger the ETL DAG from the UI.
1. Open Airflow UI at http://localhost:8080
2. Find the DAG named ETL
3. Click Trigger DAG to start the pipeline

### 8.Verify Data in Snowflake
Please navigate to ** SQL_queries/create_tables **

### 9.Run Data Quality Checks
For full quality checks, please refer to ** SQL_queries/Data Quality Checks/ ** folder to check for nulls,duplicates and datatype validations for pipelined data


### 10.Setup Tableau for Visualization
1. Open Tableau Desktop
2. Click Connect to a Server -> Snowflake
3. Enter your Snowflake credentials
4. Select the ANALYTICS database & PUBLIC schema
5. Please check the visualizations and dashboard created

### 11.Conclusion:
This ETL pipeline successfully extracts, transforms, and loads data into Snowflake while ensuring data quality. The final dashboard in Tableau provides meaningful insights into user and brand and rewards activity.




** Contact
Author: Harika Mangu
Email: manguharika16@gmail.com
GitHub Repo: Fetch-ETL-Assessment **