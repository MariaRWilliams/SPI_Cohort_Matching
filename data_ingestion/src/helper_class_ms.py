import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window

class MS_Helper():

    def __init__(self):
        """
        no variables to initialize yet 
        """
    
    def join_claims_spend(self, med_df, pharma_df):

        claims_df = med_df.join(pharma_df, on=['member_id', 'service_month'], how = 'outer').fillna(0)
        claims_df = claims_df.withColumn('total_claims', F.col('med_total') + F.col('pharma_total'))
        claims_df = claims_df.withColumn('total_claims_net', F.col('med_total_net') + F.col('pharma_total_net'))
        
        return claims_df
    
    def join_med_codes(self, med_codes_df, util_tmp, avoidable_er_tmp):

        med_codes_df = (med_codes_df
                    .join(util_tmp.select('p_code', 'category').alias('p'), med_codes_df['code_pos']==util_tmp['p_code'], how='left')
                    .join(util_tmp.select('o_code', 'category').alias('o'), med_codes_df['code_rev']==util_tmp['o_code'], how='left')
                    .join(avoidable_er_tmp.select('ms_joiner', 'category').alias('er'), med_codes_df['dx1']==avoidable_er_tmp['ms_joiner'], how='left')
                    .cache()                       
                    )
        
        return med_codes_df
    
    def get_visit_util(self, med_codes_df):

        util_df = (med_codes_df
                            .select('member_id', 'svc_start', 'svc_end', F.expr("stack(3, p.category, o.category, er.category) as (util)"))
                            .filter(F.col('util').isNotNull())
                            .orderBy('member_id', 'svc_start', 'svc_end')
                            .distinct()                
                            )

        win1 = Window.partitionBy('member_id').orderBy('svc_start', 'svc_end')
        win2 = Window.partitionBy('member_id', 'util_id')

        util_df = (util_df
                    .withColumn('util_id', F.when(F.col('svc_start')
                                                            .between(F.lag(F.col('svc_start'), 1).over(win1), F.max(F.col('svc_end')).over(win1.rowsBetween(Window.unboundedPreceding, -1)))
                                                            , None).otherwise(F.monotonically_increasing_id()))
                    .withColumn('util_id', F.last(F.col('util_id'), ignorenulls=True).over(win1.rowsBetween(Window.unboundedPreceding, 0)))
                    .withColumn('visit_start', F.min(F.col('svc_start')).over(win2))
                    .withColumn('visit_end', F.max(F.col('svc_end')).over(win2))
                    .withColumn('visit_len', F.datediff(F.col('visit_end'), F.col('visit_start'))+1)
                    .withColumn('service_month', F.date_format(F.col('visit_start'), 'yyyyMM'))
                    .orderBy('member_id', 'visit_start', 'visit_end')
                    )
        
        util_df_T = (util_df
                            .withColumn('util', F.regexp_replace(F.lower(F.col('util')), ' ', '_'))
                            .groupBy(['service_month', 'member_id', 'util_id', 'visit_start', 'visit_end', 'visit_len'])
                            .pivot('util')
                            .agg(F.when(F.first('util').isNotNull(), 1).otherwise(0))
                            .fillna(0)
                            )
        
        return util_df_T

    def add_util(self, util_df_T):

        util_df_T = util_df_T.withColumn('avoidable_er', F.when((F.col('avoidable_er') == 1) & (F.col('er') == 1) & (F.col('inpatient') == 0), 1).otherwise(0))

        readmits = (util_df_T.filter(F.col('inpatient') == 1)
                    .select('member_id', 'util_id', 'visit_start', 'visit_end')
                    .orderBy('visit_start')
                    )

        readmits = (readmits.withColumn('diff', F.datediff(F.col('visit_start'), F.lag(F.col('visit_end'), 1)
                                                                                .over(Window.partitionBy('member_id').orderBy('visit_start'))))
                    .withColumn('readmission', F.when((F.col('diff') < 30)&(F.col('diff') > 1), F.lit(1)).otherwise(F.lit(0))))

        util_df_T = util_df_T.join(readmits.select('member_id', 'util_id', 'visit_start', 'visit_end', 'readmission'), on=['member_id', 'util_id', 'visit_start', 'visit_end'], how='left').fillna(0)

        return util_df_T
    
    def count_util(self, util_df_T):

        util_list = list(set(util_df_T.columns) - set(['member_id', 'service_month', 'util_id', 'visit_start', 'visit_end', 'visit_len']))
        util_df_complete = util_df_T.groupby('member_id', 'service_month').agg(*[F.sum(x).alias(x) for x in util_list])

        return util_df_complete
    
    def get_conditions(self, med_codes_df, disease_joiner_tmp):

        cond_df = (med_codes_df
                    .join(disease_joiner_tmp.select('ms_diag', 'condition').alias('d1'), med_codes_df['dx1'] == F.col('d1.ms_diag'), 'left')
                    .join(disease_joiner_tmp.select('ms_diag', 'condition').alias('d2'), med_codes_df['dx2'] == F.col('d2.ms_diag'), 'left')
                    .join(disease_joiner_tmp.select('ms_diag', 'condition').alias('d3'), med_codes_df['dx3'] == F.col('d3.ms_diag'), 'left')
                    .join(disease_joiner_tmp.select('ms_diag', 'condition').alias('d4'), med_codes_df['dx4'] == F.col('d4.ms_diag'), 'left')
                    .filter(F.col('d1.condition').isNotNull() | F.col('d2.condition').isNotNull() | F.col('d3.condition').isNotNull() | F.col('d4.condition').isNotNull())
        )

        return cond_df
    
    def util_conditions(self, cond_df):

        cond_util = (cond_df.filter(F.col('d1.condition').isNotNull() & 
                                    ((F.col('p.category')=='ER')|(F.col('o.category')=='ER')|(F.col('p.category')=='Inpatient')|(F.col('o.category')=='Inpatient')))
                            .select('member_id', 'd1.condition')
                            .dropDuplicates()
                    )

        return cond_util
    
    def dx_conditions(self, cond_df):

        cond_dx = (cond_df
                    .select('member_id', 'svc_start', F.expr("stack(4, d1.condition, d2.condition, d3.condition, d4.condition) as (condition)"))
                    .filter(F.col('condition').isNotNull())
                    .groupby('member_id', 'condition').agg(F.countDistinct('svc_start').alias('times'))
                    .filter(F.col('times') > 1)
                    .select('member_id', 'condition')
                    .dropDuplicates() 
                )

        return cond_dx
    
    def rx_conditions(self, pharma_codes_df, drug_table_tmp):

        cond_rx = (pharma_codes_df
                            .join(drug_table_tmp, pharma_codes_df['ndcnum']==drug_table_tmp['ndc'], how='inner')
                            .select('member_id', 'condition')
                            .dropDuplicates()                     
                            )

        return cond_rx

    def map_state(self, member_df):

        mapping = {"Alabama": "AL",
                    "Alaska": "AK",
                    "Arizona": "AZ",
                    "Arkansas": "AR",
                    "American Samoa": "AS",
                    "California": "CA",
                    "Colorado": "CO",
                    "Connecticut": "CT",
                    "Delaware": "DE",
                    "District of Columbia": "DC",
                    "Florida": "FL",
                    "Georgia": "GA",
                    "Guam": "GU",
                    "Hawaii": "HI",
                    "Idaho": "ID",
                    "Illinois": "IL",
                    "Indiana": "IN",
                    "Iowa": "IA",
                    "Kansas": "KS",
                    "Kentucky": "KY",
                    "Louisiana": "LA",
                    "Maine": "ME",
                    "Maryland": "MD",
                    "Massachusetts": "MA",
                    "Michigan": "MI",
                    "Minnesota": "MN",
                    "Mississippi": "MS",
                    "Missouri": "MO",
                    "Montana": "MT",
                    "Nebraska": "NE",
                    "Nevada": "NV",
                    "New Hampshire": "NH",
                    "New Jersey": "NJ",
                    "New Mexico": "NM",
                    "New York": "NY",
                    "North Carolina": "NC",
                    "North Dakota": "ND",
                    "Northern Mariana Islands": "MP",
                    "Ohio": "OH",
                    "Oklahoma": "OK",
                    "Oregon": "OR",
                    "Pennsylvania": "PA",
                    "Puerto Rico": "PR",
                    "Rhode Island": "RI",
                    "South Carolina": "SC",
                    "South Dakota": "SD",
                    "Tennessee": "TN",
                    "Texas": "TX",
                    "Trust Territories": "TT",
                    "Utah": "UT",
                    "Vermont": "VT",
                    "Virginia": "VA",
                    "Virgin Islands": "VI",
                    "Washington": "WA",
                    "West Virginia": "WV",
                    "Wisconsin": "WI",
                    "Wyoming": "WY"
                }
        
        map_col = F.create_map([F.lit(x) for i in mapping.items() for x in i])
        member_df = member_df.withColumn('state_abr', map_col[F.col('state')])
        member_df = member_df.drop('state')

        return member_df