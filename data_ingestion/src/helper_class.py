import pandas as pd
from src.query_class_cg import QueryClass as CG_QC
from src.query_class_source_codes import QueryClass as SC_QC
from src.query_class_conditions import ConditionsClass

class CG_Helper():

    def __init__(self):
        self.cg_queries = CG_QC()
        self.base_queries = SC_QC()
        self.cond_queries = ConditionsClass()

    def map_customers(self, customer_df, column_nm):
                                        
        mapping = {"7-ELEVEN, INC.": "SEVEN ELEVEN",
                    "ABA EMPLOYEE SERVICES": "LIGHTHOUSE AUTISM CENTER",
                    "ADVANTAGE SALES AND MARKETING": "ADVANTAGE SOLUTIONS",
                    "AERA ENERGY SERVICES COMPANY": "AERA ENERGY",
                    "API GROUP, INC": "API GROUP",
                    "CALIFORNIA HIGHWAY PATROL": "CALIFORNIA ASSOCIATION OF HIGHWAY PATROLMEN",
                    "CENTERWELL": "CENTERWELL HOME HEALTH",
                    "COREWELL": "COREWELL HEALTH",
                    "EMD": "BENEFITS4ME",
                    "ENERCON SERVICES, INC.": "ENERCON",
                    "ENLYTE (FORMERLY MITCHELL-GENEX SERVICES)": "ENLYTE",
                    "FIRST AMERICAN FINANCIAL CORP": "FIRST AMERICAN FINANCIAL",
                    "FULL BLOOM": "CATAPULT",
                    "GENERAL MILLS, INC.": "GENERAL MILLS",
                    "GREIF PACKAGING": "GREIF INC",
                    "INTUIT INC.": "INTUIT",
                    "L3HARRISTECHNOLOGIESINC": "L3HARRIS",
                    "LAND O'LAKES, INC.": "LAND O'LAKES",
                    "LANDRY'S AND GOLDEN NUGGET": "LANDRYS INC",
                    "LASSONDE PAPPAS": "LASSONDE PAPPAS AND COMPANY",
                    "LOWES": "LOWE'S",
                    "NEW ARCLIN U.S. HOLDING CORP.": "ARCLIN",
                    "PEPSI-COLA AND NATIONAL BRAND BEVERAGES": "HONICKMAN",
                    "PRISM/CITY OF REDDING": "CITY OF REDDING",
                    "PRISM/COUNTY OF SANTA BARBARA": "COUNTY OF SANTA BARBARA",
                    "RACETRAC PETROLEUM, INC.": "RACETRAC",
                    "RED RIVER TECHNOLOGY LLC": "RED RIVER TECHNOLOGY",
                    "SA RECYCLING, LLC": "SA RECYCLING",
                    "TEMPLE UNIVERSITY HEALTH SYSTEM": "TEMPLE",
                    "TUTOR PERINI CORPORATION": "TUTOR PERINI",
                    "UGI": "AMERIGAS",
                    "UNISYS CORPORATION": "UNISYS"
                }
        
        customer_df['edw_cust']  = customer_df[column_nm].str.upper().map(mapping).fillna(customer_df[column_nm].str.upper())

        return customer_df
                
    def map_customers_schema(self, customer_df, schema_df):
               
        customer_df = self.map_customers(customer_df, 'cg_cust')
        customer_df = customer_df.merge(schema_df, on='acronym')
        
        #remove the extra lines
        customer_df = customer_df[~customer_df['edw_cust'].isin(['BK', 'NON-ACCOLADE', 'ACCOLADE', 'BLANK'])]
        
        #mark the schemas with multiple customers
        schema_cust = customer_df.groupby('table_schema')['edw_cust'].nunique().reset_index().rename(columns={'edw_cust':'num_cust'})
        customer_df = customer_df.merge(schema_cust, on='table_schema')
        
        #return
        return customer_df[['table_schema', 'acronym', 'edw_cust', 'cg_cust', 'num_cust']]
    
    def map_customers_member(self, member_df, customer_df):
        
        member_df = self.map_customers(member_df, 'dw_customer_nm')
        cdf = customer_df[customer_df['num_cust']==1]
        
        member_df = member_df.merge(cdf, on='table_schema', how='left')
        member_df.loc[member_df['edw_cust_x'].isin(['BK', 'NON-ACCOLADE', 'ACCOLADE', 'BLANK']), 'edw_cust_x'] = None
        member_df['edw_cust'] = member_df['edw_cust_x'].fillna(member_df['edw_cust_y'])
        
        member_df = member_df.drop(columns = ['edw_cust_x', 'edw_cust_y', 'table_schema', 'acronym', 'cg_cust', 'num_cust', 'dw_customer_nm'])
                
        return member_df
    
    def map_industry(self, member_df):
        
        c_df = pd.read_csv('data_ingestion/src_data/Industry_Map.csv')
        member_df = pd.merge(member_df, c_df[['edw_cust', 'industry']], on ='edw_cust', how='left')
        member_df['industry'] = member_df['industry'].fillna('unknown')
        
        return member_df
    
    def get_customers(self, cg_conn, sample):
    
        print('Calling Schema Query...')
        q = self.cg_queries.query_schema()
        schema_df = cg_conn.query_data(q)

        print('Collecting Customers...')
        customer_df = pd.DataFrame()
        for x in range(len(schema_df.index)):
            q = self.cg_queries.query_customers(schema_df.iloc[x,0])
            c_df = cg_conn.query_data(q)
            c_df['acronym'] = schema_df.iloc[x,1]
            customer_df = pd.concat([customer_df, c_df])
                    
        cust_df = self.map_customers_schema(customer_df, schema_df)
        
        if sample==1:
            #cust_df = cust_df[cust_df['edw_cust'].isin(['E AND J GALLO WINERY'])]
            cust_df = cust_df[cust_df['edw_cust'].isin(['E AND J GALLO WINERY', 'FIDELITY'])]
            
        print(str(len(cust_df['edw_cust'].unique()))+" Customer(s) selected")
        
        return cust_df
    
    def base_tables_edw(self, edw_conn):
        
        q = self.base_queries.query_cond()
        disease_joiner_tmp = edw_conn.query_data(q)
        
        q = self.base_queries.query_d_med()
        drug_table_tmp = edw_conn.query_data(q)
        
        q = self.base_queries.query_avoidable_er()
        avoidable_er_tmp = edw_conn.query_data(q)
        
        return disease_joiner_tmp, drug_table_tmp, avoidable_er_tmp
    
    def base_tables_ms(self, edw_conn):
        
        q = self.base_queries.query_utilization_codes()
        util_tmp = edw_conn.query_data(q)
        
        return util_tmp
    
    def member_demographics(self, cg_conn, cust_df, svc_year):
    
        member_demo_df = pd.DataFrame()
        for x in cust_df['table_schema'].unique().tolist():
            
            print("Running Demographics Query for "+x)
            sql_statement = self.cg_queries.query_demographics(x, svc_year)
            temp_df = cg_conn.query_data(sql_statement)
            temp_df['table_schema'] = x
            member_demo_df = pd.concat([member_demo_df, temp_df])

        member_demo_df = self.map_customers_member(member_demo_df, cust_df)
        member_demo_df = self.map_industry(member_demo_df)
        member_demo_df.fillna(0, inplace=True)
        
        return member_demo_df
        
    def claims_spend(self, cg_conn, cust_df, svc_year):
        
        claims_df = pd.DataFrame()
        for x in cust_df['table_schema'].unique().tolist():
            
            print("Running Medical Spend Query for "+x)
            sql_statement = self.cg_queries.query_med_claims(x, svc_year)
            temp_df = cg_conn.query_data(sql_statement)
            schema_claims_df = temp_df[['member_id', 'service_month', 'med_total', 'med_total_net']]
            
            print("Running Pharmacy Spend Query for "+x)
            sql_statement = self.cg_queries.query_rx_claims(x, svc_year)
            temp_df = cg_conn.query_data(sql_statement)
            schema_claims_df = pd.merge(schema_claims_df, temp_df, on = ['member_id', 'service_month'], how='outer')
            
            schema_claims_df.fillna(0, inplace=True)
            schema_claims_df['total_claims'] = schema_claims_df['med_total'] + schema_claims_df['pharma_total']
            schema_claims_df['total_claims_net'] = schema_claims_df['med_total_net'] + schema_claims_df['pharma_total_net']
            claims_df = pd.concat([claims_df, schema_claims_df])
            
        return claims_df
    
    def dx_codes(self, cg_conn, cust_df, svc_year, util_tmp, avoidable_er_tmp):
        
        claims_df = pd.DataFrame()
        for x in cust_df['table_schema'].unique().tolist():
            
            print("Running Medical Codes Query for "+x)
            sql_statement = self.cg_queries.query_med_codes(x, svc_year)
            temp_df = cg_conn.query_data(sql_statement)
            claims_df = pd.concat([claims_df, temp_df])
        
        claims_df.fillna(0, inplace=True)    
        claims_df = claims_df.merge(util_tmp[['p_code', 'category']], left_on='code_pos', right_on='p_code', how='left').rename(columns = {'category':'p_category'})
        claims_df = claims_df.merge(util_tmp[['o_code', 'category']], left_on='code_rev', right_on='o_code', how='left').rename(columns = {'category':'o_category'})
        claims_df = claims_df.merge(avoidable_er_tmp[['cd_sys_concept_cd', 'category']], left_on='dx1', right_on='cd_sys_concept_cd', how='left').rename(columns = {'category':'er_category'})
        
        return claims_df
    
    def rx_codes(self, cg_conn, cust_df, svc_year):
        
        claims_df = pd.DataFrame()
        for x in cust_df['table_schema'].unique().tolist():
            
            print("Running Pharmacy Codes Query for "+x)
            sql_statement = self.cg_queries.query_rx_codes(x, svc_year)
            temp_df = cg_conn.query_data(sql_statement)
            claims_df = pd.concat([claims_df, temp_df])
        
        claims_df.fillna(0, inplace=True)          
        return claims_df
    
    def get_visit_util(self, dx_codes_df):
        
        #get members, times, and categories in one column
        keep_col = ['member_id', 'svc_start', 'svc_end']
        cat_col = [col for col in dx_codes_df if col.endswith('category')]
        
        util_df = pd.DataFrame()
        for x in cat_col:
            temp_df = dx_codes_df[dx_codes_df[x].notna()].rename(columns={x:'category'})
            temp_df = temp_df[keep_col+['category']]
            util_df = pd.concat([util_df, temp_df])
        
        util_df = util_df[util_df['category'].notna()].drop_duplicates()
        
        #label with visit id it start and end dates overlap
        util_df = util_df.sort_values(by=['member_id', 'svc_start', 'svc_end'])
        util_df['last_start'] = util_df.groupby('member_id')['svc_start'].shift()
        util_df['max_end'] = util_df.groupby(['member_id'])['svc_end'].transform(lambda x: x.cummax())
        util_df['max_end'] = util_df.groupby('member_id')['max_end'].shift()
        
        util_df.loc[((util_df['svc_start']>util_df['max_end'])|(util_df['max_end'].isnull())), 'util_id'] = util_df[((util_df['svc_start']>util_df['max_end'])|(util_df['max_end'].isnull()))].index.values
        util_df['util_id'] = util_df['util_id'].ffill()
        util_df['util_id'] = util_df['util_id'].astype(int)
        
        #add additional columns
        util_df = util_df.merge(util_df.rename(columns={'svc_start':'visit_start'}).groupby(['member_id', 'util_id'])['visit_start'].min(), on=['member_id', 'util_id'] )
        util_df = util_df.merge(util_df.rename(columns={'svc_end':'visit_end'}).groupby(['member_id', 'util_id'])['visit_end'].max(), on=['member_id', 'util_id'])
        util_df['visit_len'] = (util_df['visit_end'] - util_df['visit_start']).apply(lambda s: s.days)
        util_df['visit_len'] = util_df['visit_len']+1
        util_df['service_month'] =  pd.to_datetime(util_df['visit_start'])
        util_df['service_month'] =  util_df['service_month'].dt.strftime('%Y%m')
        
        #transpose on visit
        util_df_T = util_df[['service_month', 'member_id', 'util_id', 'visit_start', 'visit_end', 'visit_len', 'category']].drop_duplicates()
        
        util_df_T['category'] = util_df_T['category'].str.lower()
        util_df_T['category'] = util_df_T['category'].str.replace(' ', '_')
        
        one_hot = pd.get_dummies(util_df_T['category'], dtype=int)
        without_hot = util_df_T.drop(['category'], axis=1)
        
        util_df_T = without_hot.join(one_hot)
        util_df_T = util_df_T.groupby(['service_month', 'member_id', 'util_id', 'visit_start', 'visit_end', 'visit_len']).sum().reset_index()        
        
        return util_df_T
    
    def add_util(self, df):
        
        #fix avoidable er
        df.loc[(df['er']==0) | (df['inpatient']==1), 'avoidable_er'] = 0
        
        #readmits
        readmit_df = df[df['inpatient']==1]
        readmit_df = readmit_df[['member_id', 'util_id', 'visit_start', 'visit_end']].sort_values(by='visit_start')
        
        readmit_df['last_end'] = readmit_df.groupby('member_id')['visit_end'].shift()
        readmit_df.loc[~readmit_df['last_end'].isna(), 'diff'] = (readmit_df.loc[~readmit_df['last_end'].isna(), 'visit_start'] 
                                                                  - readmit_df.loc[~readmit_df['last_end'].isna(), 'last_end']).apply(lambda s: s.days)
        readmit_df.loc[(readmit_df['diff']>1) & (readmit_df['diff']<30), 'readmission'] = 1
        
        #add to main df
        readmit_df = readmit_df[['member_id', 'util_id', 'visit_start', 'visit_end', 'readmission']].drop_duplicates()
        df = df.merge(readmit_df, on=['member_id', 'util_id', 'visit_start', 'visit_end'], how='left').fillna(0)
        df['readmission'] = df['readmission'].astype(int)

        return df
        
    def count_util(self, df):
        
        util_list = list(set(df.columns) - set(['member_id', 'service_month', 'util_id', 'visit_start', 'visit_end', 'visit_len']))
        
        df = df[['member_id', 'service_month']+util_list]
        df = df.groupby(['member_id', 'service_month']).sum().reset_index()
        
        return df
    
    def get_conditions(sefl, med_codes_df, disease_joiner_tmp):
        
        djt = disease_joiner_tmp[['cd_sys_concept_cd', 'condition']]
        med_codes_df = med_codes_df.drop(columns='cd_sys_concept_cd')
        cond_df = med_codes_df.merge(djt, left_on='dx1', right_on='cd_sys_concept_cd', how='left').rename(columns={'condition':'d1_condition'}).drop(columns='cd_sys_concept_cd')
        cond_df = cond_df.merge(djt, left_on='dx2', right_on='cd_sys_concept_cd', how='left').rename(columns={'condition':'d2_condition'}).drop(columns='cd_sys_concept_cd')
        cond_df = cond_df.merge(djt, left_on='dx3', right_on='cd_sys_concept_cd', how='left').rename(columns={'condition':'d3_condition'}).drop(columns='cd_sys_concept_cd')
        cond_df = cond_df.merge(djt, left_on='dx4', right_on='cd_sys_concept_cd', how='left').rename(columns={'condition':'d4_condition'}).drop(columns='cd_sys_concept_cd')
        
        cond_df = cond_df[(cond_df['d1_condition'].notna()) | (cond_df['d2_condition'].notna()) | (cond_df['d3_condition'].notna()) | (cond_df['d4_condition'].notna())]
        
        return cond_df
    
    def util_conditions(self, cond_df):
        
        cond_util = cond_df[(cond_df['d1_condition'].notna()) &
                            ((cond_df['p_category']=='ER') | (cond_df['o_category']=='ER') | (cond_df['p_category']=='Inpatient') | (cond_df['o_category']=='Inpatient'))]
        cond_util = cond_util[['member_id', 'd1_condition']].rename(columns={'d1_condition':'condition'}).drop_duplicates()
        
        return cond_util
    
    def dx_conditions(self, dx_codes_df):
        
        #get members, times, and categories in one column
        keep_col = ['member_id', 'svc_start']
        cat_col = [col for col in dx_codes_df if col.endswith('condition')]
        
        cond_dx = pd.DataFrame()
        for x in cat_col:
            temp_df = dx_codes_df[~dx_codes_df[x].isnull()].rename(columns={x:'condition'})
            temp_df = temp_df[keep_col+['condition']]
            cond_dx = pd.concat([cond_dx, temp_df])
            
        cond_dx = cond_dx[cond_dx['condition'].notna()]
        cond_dx = cond_dx.groupby(['member_id', 'condition'])['svc_start'].nunique().reset_index()
        cond_dx = cond_dx[cond_dx['svc_start']>1]
        
        return cond_dx[['member_id', 'condition']]

    def rx_conditions(self, pharma_codes_df, drug_table_tmp):
        
        cond_rx = pharma_codes_df.merge(drug_table_tmp, left_on='svc_ndc_code', right_on='ndc', how='inner')
        cond_rx = cond_rx[['member_id', 'condition']].drop_duplicates()

        return cond_rx
    
    def conditions_together(self, cond_util, cond_dx, cond_rx):
        
        cond_df = pd.concat([cond_util, cond_dx, cond_rx]).drop_duplicates()
        cond_df['condition'] = cond_df['condition'].str.lower()
        cond_df['condition'] = cond_df['condition'].str.replace(' ', '_')
        
        one_hot = pd.get_dummies(cond_df['condition'], dtype=int)
        without_hot = cond_df.drop(['condition'], axis=1)
        
        member_chron_df = without_hot.join(one_hot)
        member_chron_df = member_chron_df.groupby('member_id').sum().reset_index()   
        
        return member_chron_df
