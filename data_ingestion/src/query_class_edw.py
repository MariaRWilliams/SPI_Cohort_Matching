class QueryClass():

    def __init__(self):
        """
        no variables to initialize yet 
        """
    
    def query_spi_events(self, start_year, customer_list):

        q = f"""
              SELECT distinct pmc.org_nm
              , pmc.person_id
              , pmc.drvd_mbrshp_covrg_id
              , sd.utc_period
              , case when sd.savings_category in 
                     ('High Cost Claimants (HCC)', 'Case Management', 'Disease Management',
                     'Maternity Program', 'Treatment Decision Support') then sd.savings_category
                     else 'exclude' end as category
              FROM acp_edw.info_layer.v1_uat_spi_dtl sd
              inner join acp_edw.info_layer.prs_mbrshp_covrg pmc on 
                     upper(sd.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id) and 
                     sd.utc_period = pmc.utc_period
              WHERE left(sd.utc_period,4) >= '{start_year}' 
              AND pmc.org_nm in ('{customer_list}')
              """
              
        return q
 
    def query_crosswalk(self, start_year, customer_list):
           
        q = f"""
              SELECT distinct pmc.org_nm
              , pmc.person_id
              , pmc.utc_period
              from acp_edw.info_layer.prs_mbrshp_covrg pmc
              WHERE left(utc_period,4) >= '{start_year}' 
              AND pmc.org_nm in ('{customer_list}')
              """

        return q
