# Databricks does not connect to CedarGate at this time
# Code must be run on a local machine and uploaded to Databricks

from src.helper_class import CG_Helper
from src.connector_class import SQLConnector
import pandas as pd

cg_conn = SQLConnector('cg')
edw_conn = SQLConnector('edw')
emo_conn = SQLConnector('emo')
cg_helper = CG_Helper()
file_path = 'data_ingestion/src_data/'

svc_year = '2022'
export_ver = '09'

from warnings import filterwarnings
filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

print('Calling Joiner Queries...')
disease_joiner_tmp, drug_table_tmp, avoidable_er_tmp = cg_helper.base_tables_edw(edw_conn)
util_tmp = cg_helper.base_tables_ms(emo_conn) 

edw_conn.dispose()
emo_conn.dispose()

#query customer list
cust_df = cg_helper.get_customers(cg_conn, sample=1)

#query member demographics
member_demo_df = cg_helper.member_demographics(cg_conn, cust_df, svc_year)
member_demo_df.to_parquet(file_path+'cg_mem_data_'+str(svc_year)+'_'+str(export_ver)+'.parquet', index=False)

#query claims spend
claims_df = cg_helper.claims_spend(cg_conn, cust_df, svc_year)

#query claims codes
rx_codes_df = cg_helper.rx_codes(cg_conn, cust_df, svc_year)
dx_codes_df = cg_helper.dx_codes(cg_conn, cust_df, svc_year, util_tmp, avoidable_er_tmp)

cg_conn.dispose()

#convert claims codes into utilization
visit_df = cg_helper.get_visit_util(dx_codes_df)
full_util_df = cg_helper.add_util(visit_df)
util_df_complete = cg_helper.count_util(full_util_df)

#add utilization to claims and write file
full_claims_df = pd.merge(claims_df, util_df_complete, on = ['member_id', 'service_month'], how='left').fillna(0)
full_claims_df.to_parquet(file_path+'cg_claims_data_'+str(svc_year)+'_'+str(export_ver)+'.parquet', index=False)

#convert claims codes into conditions
cond_df = cg_helper.get_conditions(dx_codes_df, disease_joiner_tmp)

cond_util = cg_helper.util_conditions(cond_df)
cond_dx = cg_helper.dx_conditions(cond_df)
cond_rx = cg_helper.rx_conditions(rx_codes_df, drug_table_tmp)

member_chron_df = cg_helper.conditions_together(cond_util, cond_dx, cond_rx)
member_chron_df['cal_yr'] = svc_year
member_chron_df.to_parquet(file_path+'cg_mem_chron_data_'+str(svc_year)+'_'+str(export_ver)+'.parquet', index=False)