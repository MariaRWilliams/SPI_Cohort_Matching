class QueryClass():

    def __init__(self):
        """
        no variables to initialize yet 
        """
        
    def query_demographics(self, start_year):

        q = f"""
            select mem.enrolid                                  as ms_member_id
                , mem.dobyr                                     as birth_year
                , case
                    when mem.sex = 1 then 'M'
                    when mem.sex = 2 then 'F' end                as sex
                , right(li.description, len(li.description) - 2) as industry
                , right(lg.description, len(lg.description) - 3) as state
                , case
                    when mem.msa_cd like '%R%' then 'rural'
                    when mem.msa_cd = 'X' then 'not_provided'
                    else mem.msa_cd end                          as msa_5
                , min(mem.start_date)                            as start_date
                , max(mem.end_date)                              as end_date
            from eoc_workarea.member_table_full mem
                    left join lookup_indstry li on mem.indstry = li.indstry
                    left join lookup_geoloc lg on mem.state_cd = lg.geoloc
                    left join lookup_region lr on mem.region_cd = lr.region
            where to_char(end_date, 'YYYY') >= '{start_year}'
            group by 1, 2, 3, 4, 5, 6, 7
            """
              
        return q
    
    def query_med_claims(self, start_year):

        q = f"""
            select to_char(ac.svcdate, 'YYYYMM')    as service_month
                , ac.enrolid                        as ms_member_id
                , sum(ac.pay)                       as med_total
                , sum(ac.netpay)                    as med_total_net
            from emo_edw.ibm_marketscan.all_claims ac
            where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
            group by 1,2
            """
              
        return q

    def query_pharma_claims(self, start_year):

        q = f"""
            select to_char(ac.svcdate, 'YYYYMM')    as service_month
                , ac.enrolid                        as ms_member_id
                , sum(ac.pay)                       as pharma_total
                , sum(ac.netpay)                    as pharma_total_net
            from emo_edw.ibm_marketscan.all_drug_claims ac
            where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
            group by 1,2
            """
              
        return q
    
    def query_utilization(self, start_year):

        q = f"""
            with #Utilization as (select ac.svcdate
                                    , ac.enrolid
                                    , max(case when lsc.description like '% IP %' then 1 else 0 end)                as IP_flg
                                    , max(case when lsc.description like '% OP %' then 1 else 0 end)                as OP_flg
                                    , max(case when lsc.description like '%Physician Specialty%' then 1 else 0 end) as Specialty_flg
                                    , max(case when lsc.description like '%Preventive Visits%' then 1 else 0 end)   as Preventative_flg
                                    , max(case when lsc.description like '% ER %' then 1 else 0 end)                as ER_flg
                                from emo_edw.ibm_marketscan.all_claims ac
                                        left join lookup_svcscat lsc on ac.svcscat = lsc.svcscat
                                where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
                                group by svcdate, enrolid)
            select to_char(svcdate, 'YYYYMM')   as period
                , enrolid                       as ms_member_id
                , sum(IP_flg)                   as IP_days
                , sum(OP_flg)                   as OP_days
                , sum(Specialty_flg)            as Specialty_days
                , sum(Preventative_flg)         as Preventative_days
                , sum(ER_flg)                   as ER_days
            from #Utilization
            group by period, enrolid
            """
              
        return q