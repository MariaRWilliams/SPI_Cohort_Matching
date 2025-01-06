# Databricks does not connect to CedarGate at this time
# Code must be run on a local machine and uploaded to Databricks

from src.query_class_cg import QueryClass
from src.query_class_conditions import ConditionsClass
from src.helper_class import CG_Helper
from src.connector_class import SQLConnector
import pandas as pd

cg_conn = SQLConnector('cg')
cg_queries = QueryClass()
cond_queries = ConditionsClass()
cg_helper = CG_Helper()
file_path = 'data_ingestion/src_data/'

svc_year = '2023'
export_ver = '08'

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
            
cust_df = cg_helper.map_customers_schema(customer_df, schema_df)
#cust_df = cust_df[cust_df['edw_cust'].isin(['E AND J GALLO WINERY', 'FIDELITY'])]
print(str(len(cust_df['edw_cust'].unique()))+" Customer(s) selected")

claims_df = pd.DataFrame()
for x in cust_df['table_schema'].unique().tolist():
    
    print("Running Medical Claims Query for "+x)
    sql_statement = cg_queries.query_med_claims(x, svc_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_claims_df = temp_df[['table_schema', 'dw_member_id', 'service_month', 'med_total', 'med_total_net']]
    
    print("Running Pharmacy Claims Query for "+x)
    sql_statement = cg_queries.query_pharma_claims(x, svc_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    schema_claims_df = pd.merge(schema_claims_df, temp_df, on = ['table_schema', 'dw_member_id', 'service_month'], how='outer')
    
    print("Running Utilization Query for "+x)
    sql_statement = cg_queries.query_utilization(x, svc_year)
    temp_df = cg_conn.query_data(sql_statement)    
    temp_df['table_schema'] = x
    schema_claims_df = pd.merge(schema_claims_df, temp_df, on = ['table_schema', 'dw_member_id', 'service_month'], how='outer')
    
    schema_claims_df.fillna(0, inplace=True)
    schema_claims_df['total_claims'] = schema_claims_df['med_total'] + schema_claims_df['pharma_total']
    schema_claims_df['total_claims_net'] = schema_claims_df['med_total_net'] + schema_claims_df['pharma_total_net']
    claims_df = pd.concat([claims_df, schema_claims_df])

member_demo_df = pd.DataFrame()
for x in cust_df['table_schema'].unique().tolist():
    
    print("Running Demographics Query for "+x)
    sql_statement = cg_queries.query_demographics(x, svc_year)
    temp_df = cg_conn.query_data(sql_statement)
    temp_df['table_schema'] = x
    member_demo_df = pd.concat([member_demo_df, temp_df])
    
sql_statement = cond_queries.temp_conditions()
cg_conn.create_table(sql_statement)
member_chron_df = pd.DataFrame()
for x in cust_df['table_schema'].unique().tolist():
    
    print("Running Chronic Conditions Query for "+x)   
    sql_statement = cond_queries.temp_conditions_cg(x)
    cg_conn.create_table(sql_statement)
    sql_statement = cond_queries.query_conditions()
    temp_df = cg_conn.query_data(sql_statement)
    member_chron_df = pd.concat([member_chron_df, temp_df])
    
member_chron_df = member_chron_df.rename(columns = {'member_id':'dw_member_id'})
    
member_demo_df = cg_helper.map_customers_member(member_demo_df, cust_df)
member_demo_df = cg_helper.map_industry(member_demo_df)
member_demo_df.fillna(0, inplace=True)

claims_df.to_parquet(file_path+'cg_claims_data_'+str(svc_year)+'_'+str(export_ver)+'.parquet', index=False)
member_demo_df.to_parquet(file_path+'cg_mem_data_'+str(svc_year)+'_'+str(export_ver)+'.parquet', index=False)
member_chron_df.to_parquet(file_path+'cg_mem_chron_data_'+str(svc_year)+'_'+str(export_ver)+'.parquet', index=False)

cg_conn.dispose()