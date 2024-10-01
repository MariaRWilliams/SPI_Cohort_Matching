# Databricks does not connect to CedarGate at this time
# Code must be run on a local machine and uploaded to Databricks

from src.query_class_cg import QueryClass
from src.helper_class import CG_Helper
from src.connector_class import SQLConnector
import pandas as pd

cg_conn = SQLConnector('cg')
cg_queries = QueryClass()
cg_helper = CG_Helper()
file_path = 'data_ingestion/src_data/'
start_year = '2024'
export_ver = '01'

from warnings import filterwarnings
filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

print('Calling Schema Query...')
q = cg_queries.query_schema()
schema_df = cg_conn.query_data(q)

print('Collecting Customers...')
customer_df = pd.DataFrame()
for x in range(len(schema_df.index)):
    q = cg_queries.query_customers(schema_df.iloc[x,0])
    c_df = cg_conn.query_data(q)
    c_df['acronym'] = schema_df.iloc[x,1]
    customer_df = pd.concat([customer_df, c_df])
            
cust_df = cg_helper.map_customers(customer_df, schema_df)

# comment out this line to pull full BoB
# cust_df = cust_df[cust_df['edw_cust'].isin(['ADVANTAGE SOLUTIONS','AMERICAN AIRLINES', 'BANK OF NEW YORK MELLON',
#                                             'BENEFITS4ME', 'CALIFORNIA ASSOCIATION OF HIGHWAY PATROLMEN','CBIZ',
#                                             'CHOCTAW NATION','CITY OF SEATTLE','COMMSCOPE','COUNTY OF SANTA BARBARA',
#                                             'CSL BEHRING','DUPONT','FIDELITY','FIRST AMERICAN FINANCIAL','GENERAL MILLS',
#                                             'GENTIVA','GREIF INC','HYATT HOTEL CORPORATION','INTERNATIONAL PAPER',
#                                             'INTUITIVE SURGICAL','L3HARRIS','LANDRYS INC',"LOWE'S",'MCKESSON','META',
#                                             'MOFFITT CANCER CENTER','NEVADA GOLD MINES','PAYPAL','PERATON','SAFRAN USA',
#                                             'SAN FRANCISCO HEALTH SERVICE SYSTEM','SEDGWICK','SEVEN ELEVEN','STATE FARM',
#                                             'UNIVERSITY OF CALIFORNIA'])]
cust_df = cust_df[cust_df['edw_cust'].isin(['AMERICAN AIRLINES',"LOWE'S"])]
print(str(len(cust_df['edw_cust'].unique()))+" Customer(s) selected")

full_df = pd.DataFrame()
for x in cust_df['table_schema'].unique().tolist():
    
    print("Running Medical Claims Query for "+x)
    sql_statement = cg_queries.query_med_claims(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_df = temp_df
    
    print("Running Pharmacy Claims Query for "+x)
    sql_statement = cg_queries.query_pharma_claims(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_df = pd.merge(schema_df, temp_df, on = ['service_month', 'dw_member_id', 'table_schema'], how='outer')
    
    # print("Running Utilization Query for "+x)
    # sql_statement = cg_queries.query_utilization(x, start_year)
    # temp_df = cg_conn.query_data(sql_statement)
    # temp_df['table_schema'] = x
    # schema_df = pd.merge(schema_df, temp_df, on = ['service_month', 'dw_member_id', 'table_schema'], how='outer')
    
    # print("Running Demographics Query for "+x)
    # sql_statement = cg_queries.query_demographics(x, start_year)
    # temp_df = cg_conn.query_data(sql_statement)
    # temp_df['table_schema'] = x
    # full_demo_df = pd.concat([full_demo_df, temp_df])
    
    # print("Running Chronic Conditions Query for "+x)
    # sql_statement = cg_queries.query_conditions(x, start_year)
    # temp_df = cg_conn.query_data(sql_statement)
    # temp_df['table_schema'] = x
    # full_chron_df = pd.concat([full_chron_df, temp_df])
    
    full_df = pd.concat([full_df, schema_df])

print(full_df.head())
full_df.to_parquet(file_path+'cg_data.parquet', index=False)

cg_conn.dispose()
 