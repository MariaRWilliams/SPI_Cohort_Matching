class QueryClass():

    def __init__(self):
        """
        no variables to initialize yet 
        """
        
    def query_demographics(self, svc_year):

        q = f"""
            select mem.enrolid                                  as member_id
                , mem.dobyr                                     as birth_year
                , case
                    when mem.sex = 1 then 'M'
                    when mem.sex = 2 then 'F' end                as sex
                , right(li.description, len(li.description) - 2) as industry
                , right(lg.description, len(lg.description) - 3) as state
                , case when mem.msa_cd != 'X' 
                    and (mem.msa_cd like '%R%') is false 
                    then mem.msa_cd end                          as msa_5
                , max(case when mem.msa_cd like '%R%' 
                    then 1 else 0 end)                           as rural
                , max(case when mem.msa_cd != 'X' 
                    then 1 else 0 end)                           as msa_provided
                , min(mem.start_date)                            as start_date
                , max(mem.end_date)                              as end_date
            from emo_edw.ibm_marketscan.member_table_full mem
                    left join emo_edw.ibm_marketscan.lookup_indstry li on mem.indstry = li.indstry
                    left join emo_edw.ibm_marketscan.lookup_geoloc lg on mem.state_cd = lg.geoloc
                    left join emo_edw.ibm_marketscan.lookup_region lr on mem.region_cd = lr.region
            where to_char(mem.end_date, 'YYYY') >= '{svc_year}'
            and to_char(mem.start_date, 'YYYY') <= '{svc_year}'
            group by 1, 2, 3, 4, 5, 6
            """
              
        return q
    
    def query_med_claims(self, svc_year):

        q = f"""
            select to_char(ac.svcdate, 'YYYYMM')    as service_month
                , ac.enrolid                        as member_id
                , sum(ac.pay)                       as med_total
                , sum(ac.netpay)                    as med_total_net
            from emo_edw.ibm_marketscan.all_medical_claims ac
            where to_char(ac.svcdate, 'YYYY') = '{svc_year}'
            and ac.pay > 0
            group by 1,2
            """
              
        return q

    def query_rx_claims(self, svc_year):

        q = f"""
            select to_char(ac.svcdate, 'YYYYMM')    as service_month
                , ac.enrolid                        as member_id
                , sum(ac.pay)                       as pharma_total
                , sum(ac.netpay)                    as pharma_total_net
            from emo_edw.ibm_marketscan.all_drug_claims ac
            where to_char(ac.svcdate, 'YYYY') = '{svc_year}'
            and ac.pay > 0
            group by 1,2
            """
              
        return q
        
    def query_med_codes(self, svc_year):   

        q = f"""select ac.enrolid                                                         as member_id
                , ac.svcdate                                                              as svc_start
                , ac.tsvcdat                                                              as svc_end
                , ac.proc1                                                                as code_hcpcs_cpt
                , ac.stdplac                                                              as code_pos
                , ac.revcode                                                              as code_rev
                , ac.dx1
                , ac.dx2
                , ac.dx3
                , ac.dx4
            from emo_edw.ibm_marketscan.all_medical_claims ac
            where to_char(ac.svcdate, 'YYYY') = '{svc_year}'
            """

        return q
    
    def query_rx_codes(self, svc_year):

        q = f"""
            SELECT P.enrolid  as member_id
                   , P.ndcnum
            FROM emo_edw.ibm_marketscan.all_drug_claims P
            where to_char(P.svcdate, 'YYYY') = '{svc_year}'
            """
        return q