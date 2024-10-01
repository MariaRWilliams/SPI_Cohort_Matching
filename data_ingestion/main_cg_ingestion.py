# Databricks does not connect to CedarGate at this time
# Code must be run on a local machine and uploaded to Databricks

from src.query_class_cg import QueryClass
from src.helper_class_cg import CG_Helper
from src.connector_class import SQLConnector
import pandas as pd

cg_conn = SQLConnector('cg')
cg_queries = QueryClass()
cg_helper = CG_Helper()
file_path = 'data_ingestion/src_data/'
start_year = '2023'
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
#cust_df = cust_df[cust_df['edw_cust'].isin(['GENERAL MILLS'])]
print(str(len(cust_df['edw_cust'].unique()))+" Customer(s) selected")

claims_df = pd.DataFrame()
for x in cust_df['table_schema'].unique().tolist():
    
    print("Running Medical Claims Query for "+x)
    sql_statement = cg_queries.query_med_claims(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_claims_df = temp_df[['table_schema', 'dw_member_id', 'service_month']]
    
    print("Running Pharmacy Claims Query for "+x)
    sql_statement = cg_queries.query_pharma_claims(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_claims_df = pd.merge(schema_claims_df, temp_df, on = ['table_schema', 'dw_member_id', 'service_month'], how='outer')
    
    print("Running Utilization Query for "+x)
    sql_statement = cg_queries.query_utilization(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df = pd.pivot_table(temp_df,index=['dw_member_id', 'service_month'],
                             columns='categorydescription',
                             values='count_units', 
                             aggfunc='sum').reset_index()
    
    temp_df['table_schema'] = x
    schema_claims_df = pd.merge(schema_claims_df, temp_df, on = ['table_schema', 'dw_member_id', 'service_month'], how='outer')
    claims_df = pd.concat([claims_df, schema_claims_df])

member_df = pd.DataFrame()
for x in cust_df['table_schema'].unique().tolist():
    
    print("Running Demographics Query for "+x)
    sql_statement = cg_queries.query_demographics(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_mem_df = temp_df
    
    print("Running Chronic Conditions Query for "+x)
    sql_statement = cg_queries.query_conditions(x, start_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_mem_df = pd.merge(schema_mem_df, temp_df, on = ['dw_member_id', 'table_schema'], how='left')
    member_df = pd.concat([member_df, schema_mem_df])

claims_df.fillna(0, inplace=True)
member_df.fillna(0, inplace=True)

claims_df.to_parquet(file_path+'cg_mo_data.parquet', index=False)
member_df.to_parquet(file_path+'cg_mem_data.parquet', index=False)

cg_conn.dispose()