from datetime import datetime as dt
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
import numpy as np
import pyspark as spark
import pyspark.pandas as ps
import datetime

class Data_Prep():

    def __init__(self):
        self.event_list = []
        self.util_list = []
        self.min_claim = 0
        self.max_claim = 0

    def set_event_categories(self, event_df):

        self.event_list = event_df.select(F.collect_set('category').alias('category')).first()['category']

        return 0
    
    def set_util_categories(self, claims_df):

        col = claims_df.columns
        col.remove('table_schema')
        col.remove('dw_member_id')
        col.remove('service_month')
        self.util_list = col

        return 0
    
    def set_claims_window(self, claims_df, claims_cap):

        self.min_claim = claims_df.agg(F.min('service_month')).collect()[0][0]
        self.max_claim = datetime.datetime.strptime(claims_cap, '%Y-%m-%d')
       
    def clean_exposed(self, spark_c, event_df, exposed_categories, exclude_categories, preperiod, postperiod):

        keep = exposed_categories + exclude_categories
        event_df = event_df.filter(event_df.category.isin(keep))

        df_schema = event_df.select('person_id', 'utc_period', 'category').schema
        exposed_df = spark_c.createDataFrame(spark_c.sparkContext.emptyRDD(), df_schema)

        for category in exposed_categories:
            cat_df = event_df.filter(event_df.category==category)
            cat_df = cat_df.withColumn('preperiod', F.add_months(cat_df.utc_period, -preperiod))
            cat_df = cat_df.withColumn('postperiod', F.add_months(cat_df.utc_period, postperiod))

            ex_cat = list(set(exclude_categories) - set([category]))
            other_df = event_df.filter(event_df.category.isin(ex_cat))
            other_df = other_df.selectExpr('person_id as other_person_id', 'utc_period as other_period', 'category as other_category')

            condition_list = [cat_df.person_id == other_df.other_person_id,
                              other_df.other_period.between(cat_df.preperiod, cat_df.postperiod) 
                              ]

            cat_df = cat_df.join(other_df, condition_list, how='left_anti')
            cat_df = cat_df.select('person_id', 'utc_period', 'category')

            exposed_df = exposed_df.union(cat_df)

        return exposed_df
    
    def limit_exposed(self, df, demo_df, preperiod, postperiod, claims_cap):

        df = df.withColumn('preperiod', F.add_months(df.utc_period, -preperiod))
        df = df.withColumn('postperiod', F.add_months(df.utc_period, postperiod))

        df = df.withColumn('first_claim', F.lit(self.min_claim))
        df = df.withColumn('last_claim', F.lit(self.max_claim))
        df = df.filter((df.preperiod >= df.first_claim) & (df.postperiod <= df.last_claim))

        condition_list = [df.person_id == demo_df.person_id,
                          df.preperiod>= demo_df.start_date,
                          df.postperiod<= demo_df.end_date
                        ]

        df = df.join(demo_df, condition_list, how='inner').drop(demo_df.person_id)

        drop_lst = ['preperiod', 'postperiod', 'first_claim', 'last_claim', 'start_date', 'end_date', 'table_schema']
        df = df.drop(*drop_lst)

        return df
    
    def generate_control(self, spark_session, df, preperiod, postperiod, claims_cap):

        min_claim_range = self.min_claim + pd.DateOffset(months=preperiod)
        max_claim_range = self.max_claim - pd.DateOffset(months=postperiod)
        df_wd = df.withColumn('pre_claim', F.lit(min_claim_range))
        df_wd = df_wd.withColumn('post_claim', F.lit(max_claim_range))
        df_wd = df_wd.withColumn('pre_elig', F.add_months(df.start_date, preperiod))
        df_wd = df_wd.withColumn('post_elig', F.add_months(df_wd.end_date, -(postperiod+1)))
        df_wd = df_wd.withColumn('range_min', F.greatest(df_wd['pre_elig'], df_wd['pre_claim']))
        df_wd = df_wd.withColumn('range_max', F.least(df_wd['post_elig'], df_wd['post_claim']))

        range_min = df_wd.agg(F.min('range_min')).collect()[0][0]
        range_max = df_wd.agg(F.max('range_max')).collect()[0][0]

        mo = pd.date_range(start=range_min, end=range_max, freq='MS')   
        mo = pd.DataFrame({'range_period':mo})

        df_wd = df_wd.crossJoin(spark_session.createDataFrame(mo))
        df_wd = df_wd.filter((df_wd['range_min']<=df_wd['range_period']) & (df_wd['range_max']>=df_wd['range_period']))
        df_wd = df_wd.withColumn('utc_period', F.trunc('range_period', 'month'))
        df_wd = df_wd.drop('pre_elig', 'post_elig', 'pre_claim', 'post_claim', 'range_min', 'range_max', 'range_period', 'start_date', 'end_date', 'table_schema')

        df_wd = df_wd.withColumn('category', F.lit('control'))

        return df_wd
    
    def merge_claims(self, spark_session, df, claims_df, preperiod, postperiod):
        
        df = df.withColumn('preperiod', F.add_months(df.utc_period, -preperiod))
        df = df.withColumn('postperiod', F.add_months(df.utc_period, postperiod))

        condition_list = [df.dw_member_id == claims_df.dw_member_id,
                    claims_df.service_month.between(df.preperiod, df.postperiod) 
                ]
        
        df = df.join(claims_df, condition_list, how='inner').drop(claims_df.dw_member_id)
       
        return df
    
    def pivot_claims(self, df, leading_list, trailing_list):

        col_list = list(set(leading_list+trailing_list))
        df = df.withColumn('mo_seq', F.round(F.months_between(df['service_month'], df['utc_period']), 0).cast('integer'))
        
        df_pivot = df.select('dw_member_id', 'utc_period').dropDuplicates()
        for c in col_list:
            seq_start = 0
            seq_end = 0

            if c in leading_list: 
                seq_start = df.agg(F.min('mo_seq')).collect()[0][0]
            if c in trailing_list: 
                seq_end = df.agg(F.max('mo_seq')).collect()[0][0]

            time_set = (df.withColumn('mo_pivot',  F.concat(F.lit(c), F.col('mo_seq')))
                        .filter((F.col('mo_seq')>=seq_start) & (F.col('mo_seq')<=seq_end))
                        .groupBy(['dw_member_id', 'utc_period'])
                        .pivot('mo_pivot')
                        .agg(F.round(F.first(c), 2))
            )

            condition_list = [df_pivot.dw_member_id == time_set.dw_member_id, 
                              df_pivot.utc_period == time_set.utc_period]
            
            df_pivot = df_pivot.join(time_set, condition_list, how='left').select(df_pivot["*"], *[time_set[c] for c in time_set.columns if c not in ["dw_member_id", "utc_period"]])

        df_pivot = df_pivot.na.fill(0)
        df_pivot = df_pivot.select([F.col(x).alias(x.replace(' ', '_')) for x in df_pivot.columns])
        df_pivot = df_pivot.select(*[x.lower() for x in df_pivot.columns])

        return df_pivot
    
    def remove_negatives(self, df, leading_list, trailing_list):

        col_list = list(set(leading_list+trailing_list))

        col_list = [x.lower() for x in col_list]
        col_list = [x.replace(' ', '_') for x in col_list]

        selected_columns = []
        for c in col_list:
            selected_columns = selected_columns + [column for column in df.columns if column.startswith(c)]

        df = df.filter(F.least(*[F.col(c) >= 0 for c in selected_columns]) == True)

        return df
    
    def calc_age(self, df):

        df = df.withColumn('age', F.round(F.months_between(df['utc_period'], df['birth_year'])/12, 0).cast('integer'))
        df = df.withColumn('age_band', 
                           F.when(df['age']<18, '0-17')
                           .when((df['age']>=18) & (df['age']<40), '18-39')
                           .when((df['age']>=40) & (df['age']<60), '40-59')
                           .when(df['age']>=60, '60+'))

        return df
    
    def sum_periods(self, df, col_list, preperiod, postperiod):

        col_list = [x.lower() for x in col_list]
        col_list = [x.replace(' ', '_') for x in col_list]
        
        for c in col_list:
            col_nm = c+'_'+str(preperiod)+'to'+str(postperiod)+'sum'
            df = df.withColumn(col_nm, F.lit(0))
            
            for n in range(preperiod, postperiod):
                df = df.withColumn(col_nm, F.round(df[col_nm]+ df[c+str(n)], 2))

        return df

    














