import pandas as pd

class Data_Prep():

    def __init__(self):
        category_list = []

    def set_categories(self, event_df):

        self.category_list = event_df['category'].unique()

        return 0
    
    def interventions_subset(self, m, big_intervention_df, pre_event, post_event, intervention_month):
                            
        i_month_label = intervention_month
        intervention_month = m
     
        prior_start = self.month_shift(intervention_month,-pre_event)
        last_month = self.month_shift(intervention_month,post_event)
        
        SPI_df = big_intervention_df[(big_intervention_df['utc_period']>=prior_start) & (big_intervention_df['utc_period']<=last_month)].copy()
        SPI_df['int_month'] = i_month_label
        SPI_df['int_count'] = 1 
        
        return SPI_df
    
    def clean_exposed(self, event_df, preperiod, postperiod):

        event_df['utc_period'] = pd.to_datetime(event_df['utc_period'].astype(str), format='%Y%m')

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
            cat_df = cat_df.rename(columns={'org_nm_x':'org_nm','drvd_mbrshp_covrg_id_x':'drvd_mbrshp_covrg_id', 'utc_period_x':'event_period', 'category_x':'category'})
            exposed_df = pd.concat([exposed_df, cat_df])

        return exposed_df