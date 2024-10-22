# SPI_Cohort_Matching
This code is used to glean insights for SPI values based on cohort matching.

This code was designed to be deployed in Databricks, so the main functions are formatted as Python Notebooks and data is processed mostly as spark dataframes.

#### Data Ingestion:
This folder contains the code used to extract data from the servers. 
- main_cg_ingestion: This is a script that queries Cedargate for claims, utilization, and demographic details. Since Databricks does not currently support connection to Cedargate, it must be run locally.
- main_edw_ingestion: This is a Notebook that queries the edw cluster in Databricks to identify the exposed cohort.
- main_ms_ingestion: This is a Notebook that queries the emo cluster in Databricks to select a control subset from the IBM Marketscan dataset.

#### Cohort Mapping:
This folder contains the code used to match and analyze the cohorts. 
- 1_main_select_cohorts: This Notebook is used to select matching variables and prepare the data for matching.
- 2_main_match_cohorts: This Notebook runs the algorithm to match control subjects to those in the chosen exposed cohort.
- 3_main_process_cohorts: This Notebook takes the matched cohorts and crunches the numbers for final analysis.
