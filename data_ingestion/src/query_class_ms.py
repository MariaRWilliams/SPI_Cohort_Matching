class QueryClass():

    def __init__(self):
        """
        no variables to initialize yet 
        """
        
    def query_demographics(self, start_year):

        q = f"""
            select mem.enrolid
                , mem.age
                , ls.description      as sex
                , li.description      as industry
                , lg.description      as state
                , lr.description      as region
                , min(mem.start_date) as start_date
                , max(mem.end_date)   as end_date
            from eoc_workarea.member_table_full mem
                left join lookup_sex ls on mem.sex = ls.sex
                left join lookup_indstry li on mem.indstry = li.indstry
                left join lookup_geoloc lg on mem.state_cd = lg.geoloc
                left join lookup_region lr on mem.region_cd = lr.region
            where len(mem.age) > 0
            and to_char(end_date, 'YYYY') >= '{start_year}'
            group by 1, 2, 3, 4, 5
            """
              
        return q
    
    def query_med_claims(self, start_year):

        q = f"""
            select to_char(ac.svcdate, 'YYYYMM') as cal_mo
                , ac.enrolid
                , sum(ac.pay)                   as claims
            from eoc_workarea.all_claims ac
            where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
            group by 1,2
            """
              
        return q

    def query_pharma_claims(self, start_year):

        q = f"""
            select to_char(ac.svcdate, 'YYYYMM') as cal_mo
                , ac.enrolid
                , sum(ac.pay)                   as claims
            from eoc_workarea.all_drug_claims ac
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
                                from eoc_workarea.all_claims ac
                                        left join lookup_svcscat lsc on ac.svcscat = lsc.svcscat
                                where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
                                group by svcdate, enrolid)
            select to_char(svcdate, 'YYYYMM') as period
                , enrolid
                , sum(IP_flg)                as IP_days
                , sum(OP_flg)                as OP_days
                , sum(Specialty_flg)         as Specialty_days
                , sum(Preventative_flg)      as Preventative_days
                , sum(ER_flg)                as ER_days
            from #Utilization
            group by period, enrolid
            """
              
        return q
    
    def query_conditions(self, start_year):
        
        q = f"""
            select ac.enrolid
                , max(case
                        when dx1 between 'F32' and 'F33' then 1
                        when dx2 between 'F32' and 'F33' then 1
                        when dx3 between 'F32' and 'F33' then 1
                        when dx4 between 'F32' and 'F33' then 1 else 0
                end) as depression
                , max(case
                        when dx1 between 'E78' and 'E785' then 1
                        when dx2 between 'E78' and 'E785' then 1
                        when dx3 between 'E78' and 'E785' then 1
                        when dx4 between 'E78' and 'E785' then 1 else 0
                end) as hyperlipidemia
                , max(case
                        when dx1 between 'M15' and 'M19' then 1
                        when dx2 between 'M15' and 'M19' then 1
                        when dx3 between 'M15' and 'M19' then 1
                        when dx4 between 'M15' and 'M19' then 1 else 0
                end) as osteoarthritis
                , max(case
                        when dx1 between 'C00' and 'C7A' then 1
                        when dx2 between 'C00' and 'C7A' then 1
                        when dx3 between 'C00' and 'C7A' then 1
                        when dx4 between 'C00' and 'C7A' then 1 else 0
                end) as cancer
                , max(case
                        when dx1 between 'I502' and 'I5043' then 1
                        when dx2 between 'I502' and 'I5043' then 1
                        when dx3 between 'I502' and 'I5043' then 1
                        when dx4 between 'I502' and 'I5043' then 1 else 0
                end) as CHF
                , max(case
                        when dx1 between 'E08' and 'E13' then 1
                        when dx2 between 'E08' and 'E13' then 1
                        when dx3 between 'E08' and 'E13' then 1
                        when dx4 between 'E08' and 'E13' then 1 else 0
                end) as diabetes
                , max(case
                        when dx1 between 'I251' and 'I25119' then 1
                        when dx1 between 'I251' and 'I25119' then 1
                        when dx1 between 'I251' and 'I25119' then 1
                        when dx1 between 'I251' and 'I25119' then 1 else 0
                end) as CAD
                , max(case
                        when dx1 between 'J44' and 'J449' then 1
                        when dx1 between 'J44' and 'J449' then 1
                        when dx1 between 'J44' and 'J449' then 1
                        when dx1 between 'J44' and 'J449' then 1 else 0
                end) as COPD
            from eoc_workarea.all_claims ac
            where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
            group by 1
            having depression+hyperlipidemia+osteoarthritis+cancer+CHF+diabetes+CAD+COPD >= 1
        """
              
        return q