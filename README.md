# Fetch-ETL-Pipelining

### Table of Contents
[1.Tech Stack](#1tech-stack)<br>
[2.Description](#2description)<br>
[3.Prerequisites](#3prerequisites)<br>
[4.Run Data transformation (Jupyter Notebook)](#4run-data-transformation-jupyter-notebook)<br>
[Data modeling/ E-R Diagram](#data-modeling-e-r-diagram)<br>
[5.Installation Guide](#5installation-guide)<br>
[6.Setup Apache Airflow](#6setup-apache-airflow)<br>
[7.Run the ETL pipeline](#7run-the-etl-pipeline)<br>
[8.Verify data in Snowflake](#8verify-data-in-snowflake)<br>
[9.Run Data Quality Checks](#9run-data-quality-checks)<br>
[10.Setup Tableau for Visualization](#10setup-tableau-for-visualization)<br>
[11.Second: Answering Stakeholder questions](#11second-answering-stakeholder-questions)<br>
[12.Fourth: Communicate with Stakeholders](#12fourth-communicate-with-stakeholders)<br>
[13.Conclusion](#13conclusion)<br>
[14.Future Scope](#14future-scope)

### Project Title:
* End-to-End ETL Pipelining *

### 1.Tech stack:
1. Python
2. Apache Airflow
3. Snowflake
4. Tableau

### 2.Description:
This project demonstrates the ETL pipeline that extract data from raw json files to driving business insights and recommendations<br>
Workflow:
1. Data Integration: Extracted data from JSON sources and transformed/normalized it to structured CSV files using Python in Jupyter notebook.<br>
2. Data Orchestration: Utilized Apache Airflow to orchestrate the structured normalized CSV files through scheduled ETL pipelines to Snowflake.<br>
3. Data Warehousing & Transformation: Stored, transformed, and performed data quality checks and basic data analysis using SQL in Snowflake.<br>
4. Visualization & Dashboarding: Visualized the transformed data and created interactive dashboards using Tableau.<br>

**Please check the Architecture_Workflow diagram for detailed workflow** -Refer main/Architecture_Workflow diagram.gif

### 3.Prerequisites
Install Required Tools
1. Python 3.8 -[Download](https://www.python.org/downloads/)<br>
2. WSL (Windows Subsystem for Linux) (if running on Windows)<br>
3. Apache Airflow -> Installed in WSL<br>
4. Snowflake Account-[Sign In](https://app.snowflake.com/)<br>
5. Tableau Desktop -[Download](https://www.tableau.com/support/releases)<br>

### 4.Run Data transformation (Jupyter Notebook)
1. Open notebooks/json_to_csv_datatransformation.ipynb<br>
2. This notebook is specially designed to handle data extraction and flatten unstructure json data into structured CSV. This code will also perform transformation of data before pipelining<br>
3. Save the CSV files and proceed downloading for future references.<br>
**Explanation: Given json files users,brands,receipts are future normalized until 3NF,this resulted in creation of 8 tables users,brands,receipts,receipt_items,products,rewards,userflagged_items,metabrite_items.**<br>

### Data modeling/ E-R Diagram
Please refer to E-R Diagram for detailed understanding of entities and relationships between tables

### 5.Installation Guide
1. Clone the Repository<br>
![Image](https://github.com/user-attachments/assets/97a1d43f-c61d-46d5-b581-c24fd6a03fa0)
![Image](https://github.com/user-attachments/assets/2db4800d-0444-4de5-9ca9-8bb80eef60df)
2. Create and Activate a Virtual Environment<br>
![Image](https://github.com/user-attachments/assets/dd62b510-7e76-4f9f-84b3-990d7cd0b211)
3. Install Dependencies<br>
![Image](https://github.com/user-attachments/assets/4e633e39-1d85-4e72-a94d-0c06449bb3f3)


### 6.Setup Apache Airflow
1. Initialize Airflow<br>
![Image](https://github.com/user-attachments/assets/5bb6a633-85ef-4bba-bc81-dbff3c218493)
2. Start Airflow Webserver & Scheduler<br>
![Image](https://github.com/user-attachments/assets/b493506e-779d-4a50-8f51-024f7a15d0eb)
3. Add Snowflake Connection in Airflow<br>
![Image](https://github.com/user-attachments/assets/06d94ab0-39d0-4114-a8cf-4628d12a1a2c)

### 7.Run the ETL Pipeline
**Note:** The file paths in this project are configured for a WSL environment (e.g., /mnt/c/Users/<your_username>/Downloads/fetch/...). Please update these paths in the CSV_FILES dictionary in the upload_to_snowflake DAG to match the location of your CSV files on your local machine.
If you’re using WSL, ensure you include the /mnt/c/ prefix to access Windows directories.
If you’re not using WSL, provide absolute paths based on your operating system's directory structure.<br>
Once Airflow is running, trigger the ETL DAG from the UI.<br>
1. Open Airflow UI at http://localhost:8080<br>
2. Find the DAG named ETL<br>
3. Click Trigger DAG to start the pipeline<br>

### 8.Verify Data in Snowflake<br>
Please navigate to **SQL_queries/create_tables**<br>

### 9.Run Data Quality Checks<br>
For full quality checks, please refer to **SQL_queries/Data Quality Checks/** folder to check for nulls,duplicates and datatype validations for pipelined data<br>


### 10.Setup Tableau for Visualization<br>
**Please refer Fetch rewards tableau dashboard.png for dashboard insights**</br>
1. Open Tableau Desktop<br>
2. Click Connect to a Server -> Snowflake<br>
3. Enter your Snowflake credentials<br>
4. Select the ANALYTICS database & PUBLIC schema<br>
5. Please check the visualizations and dashboard created<br>

### 11.Second: Answering Stakeholder questions
 *Write queries that directly answer predetermined questions from a business stakeholder*
 Please find the queries answering the business stakeholders in folder **SQL_queries/Stakeholders_Questions/**

 ### 12.Fourth: Communicate with Stakeholders
*Construct an email or slack message that is understandable to a product or business leader.*<br>
Please find the email attachment to business stakeholders in **main/Gmail - Fetch Rewards Project_ Data Assessment Summary**


### 13.Conclusion:<br>
This ETL pipeline successfully extracts, transforms, and loads data into Snowflake while ensuring data quality. The final dashboard in Tableau provides meaningful insights into user and brand and rewards activity.<br>

### 14.Future Scope
**Customer Behavior Insights:**
Analyze customer purchase frequency, retention rates, and loyalty by tracking user activity, points earned and redeemtions. This helps in understanding buying patterns and conducting loyalty programs.<br>
**Recommendation Systems:**
Build product recommendation models using collaborative filtering or content-based filtering. This enhances user experience by providing personalized product suggestions based on past purchases.<br>


**Contact**<br>
Author: Harika Mangu<br>
Email: manguharika16@gmail.com<br>
GitHub Repo: Fetch-ETL-Assessment <br>
