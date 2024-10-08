from datetime import datetime as dt
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
import numpy as np
import pyspark as spark
import pyspark.pandas as ps

class Data_Prep():

    def __init__(self):
        self.event_list = []
        self.util_list = []

    def set_event_categories(self, event_df):

        self.event_list = event_df.select(F.collect_set('category').alias('category')).first()['category']

        return 0
    
    def set_util_categories(self, claims_df):

        col = claims_df.columns
        col.remove('table_schema')
        col.remove('dw_member_id')
        col.remove('service_month')
        col.remove('med_allowed')
        col.remove('pharma_allowed')
        self.util_list = col

        return 0
    
    def query_data(self, spark, dbutils, table):

        df = spark.sql(f"""SELECT * FROM dev.`clinical-analysis`.{table}""")

        return df
       
    def clean_exposed(self, spark_c, event_df, exposed_categories, preperiod, postperiod):

        df_schema = event_df.select('person_id', 'utc_period', 'category').schema
        exposed_df = spark_c.createDataFrame(spark_c.sparkContext.emptyRDD(), df_schema)

        for category in exposed_categories:
            cat_df = event_df.filter(event_df.category==category)
            cat_df = cat_df.withColumn('preperiod', F.add_months(cat_df.utc_period, -preperiod))
            cat_df = cat_df.withColumn('postperiod', F.add_months(cat_df.utc_period, postperiod))

            other_df = event_df.filter(event_df.category!=category)
            other_df = other_df.selectExpr('person_id as other_person_id', 'utc_period as other_period', 'category as other_category')

            condition_list = [cat_df.person_id == other_df.other_person_id,
                              other_df.other_period.between(cat_df.preperiod, cat_df.postperiod) 
                              ]

            cat_df = cat_df.join(other_df, condition_list, how='left_anti')
            cat_df = cat_df.select('person_id', 'utc_period', 'category')

            exposed_df = exposed_df.union(cat_df)

        return exposed_df
    
    def limit_exposed(self, df, demo_df, preperiod, postperiod):

        df = df.withColumn('preperiod', F.add_months(df.utc_period, -preperiod))
        df = df.withColumn('postperiod', F.add_months(df.utc_period, postperiod))

        condition_list = [df.person_id == demo_df.person_id,
                          df.preperiod>= demo_df.start_date,
                          df.postperiod<= demo_df.end_date
                        ]

        df = df.join(demo_df, condition_list, how='inner').drop(demo_df.person_id)

        return df
    
    def limit_control(self, df, preperiod, postperiod, min_claim, max_claim):

        df.show(5)

        df_wd = df.withColumn('preperiod', F.add_months(df.start_date, preperiod))
        df_wd = df_wd.withColumn('postperiod', F.add_months(df_wd.end_date, -postperiod))

        range_min = df_wd.agg(F.min('preperiod')).collect()[0][0]
        range_max = df_wd.agg(F.max('postperiod')).collect()[0][0]      

        if range_min < min_claim:
            range_min = min_claim

        if range_max > max_claim:
            range_max = max_claim

        mo = ps.date_range(start=range_min, end=range_max, freq='M')
        # mo_df = pd.DataFrame({'utc_period':mo})

        # df_wd = df_wd.merge(mo_df, how='cross')
        # df_wd = df_wd[(df_wd['preperiod']<=df_wd['utc_period']) & (df_wd['postperiod']>=df_wd['utc_period'])]

        # df_wd.drop(columns=['preperiod', 'postperiod'], inplace=True)

        # return df_wd
        return mo
    
    def merge_claims(self, df, claims_df, preperiod, postperiod):

        df['preperiod'] = df['utc_period'] - pd.DateOffset(months=preperiod)
        df['postperiod'] = df['utc_period'] + pd.DateOffset(months=postperiod)       

        c_df = df[['dw_member_id', 'utc_period', 'start_date', 'end_date', 'preperiod', 'postperiod']].merge(claims_df, how='inner', on=['dw_member_id'])
        c_df = c_df[(c_df['service_month']>=c_df['start_date']) &
                    (c_df['service_month']>=c_df['preperiod']) &
                    (c_df['service_month']<=c_df['postperiod']) &
                    (c_df['service_month']<=c_df['end_date'])]
        
        c_df['full_claims'] = c_df['med_allowed'] + c_df['pharma_allowed']
        c_df.drop(columns=['preperiod', 'postperiod'], inplace=True)
        
        return c_df
    
    def pivot_claims(self, df, col_list):

        df_pivot = df[['dw_member_id', 'utc_period']]
        df_pivot = df_pivot.drop_duplicates()
        print(df_pivot.columns)

        df['mo_seq'] =  (df['service_month'].dt.to_period('M').astype(int) - 
                         df['utc_period'].dt.to_period('M').astype(int))

        for c in col_list:
            time_set = pd.pivot_table(df, values=c, index=['dw_member_id', 'utc_period'], columns=['mo_seq'])
            time_set = time_set.add_prefix(c+' ')
            time_set = time_set.reset_index()

            df_pivot = df_pivot.merge(time_set, how='left', on=['dw_member_id', 'utc_period'])

        return df_pivot
    

    














