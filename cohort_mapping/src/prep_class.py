import pandas as pd
import numpy as np

class Data_Prep():

    def __init__(self):
        self.category_list = []

    def set_categories(self, event_df):

        self.category_list = event_df['category'].unique()

        return 0
       
    def clean_exposed(self, event_df, preperiod, postperiod):

        exposed_df = pd.DataFrame()
        for category in self.category_list:
            cat_df = event_df[event_df['category']==category].copy()
            other_df = event_df[event_df['category']!=category].copy()

            cat_df['preperiod'] = cat_df['utc_period'] - pd.DateOffset(months=preperiod)
            cat_df['postperiod'] = cat_df['utc_period'] + pd.DateOffset(months=postperiod)

            cat_df = cat_df.merge(other_df, on='person_id', how="left")
            cat_df = cat_df[(cat_df['utc_period_y']<cat_df['preperiod']) | 
                            (cat_df['utc_period_y']>cat_df['postperiod']) |
                            (cat_df['utc_period_y'].isna())]

            cat_df = cat_df[['org_nm_x', 'person_id', 'drvd_mbrshp_covrg_id_x', 'utc_period_x', 'category_x']]
            cat_df = cat_df.rename(columns={'org_nm_x':'org_nm','drvd_mbrshp_covrg_id_x':'drvd_mbrshp_covrg_id', 'utc_period_x':'utc_period', 'category_x':'category'})
            exposed_df = pd.concat([exposed_df, cat_df])

        return exposed_df
    
    def merge_claims_exp(self, df, claims_df, preperiod, postperiod):

        df['preperiod'] = df['utc_period'] - pd.DateOffset(months=preperiod)
        df['postperiod'] = df['utc_period'] + pd.DateOffset(months=postperiod)       

        c_df = df[['dw_member_id', 'utc_period', 'start_date', 'end_date', 'preperiod', 'postperiod']].merge(claims_df, how='inner', on=['dw_member_id'])
        c_df = c_df[(c_df['service_month']>=c_df['start_date']) &
                    (c_df['service_month']>=c_df['preperiod']) &
                    (c_df['service_month']<=c_df['postperiod']) &
                    (c_df['service_month']<=c_df['end_date'])]
        
        c_df['mo_seq'] =  (c_df['service_month'].dt.to_period('M').astype(int) - c_df['utc_period'].dt.to_period('M').astype(int))

        return c_df