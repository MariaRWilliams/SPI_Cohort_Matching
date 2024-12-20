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
            --where to_char(ac.svcdate, 'YYYY') = '{svc_year}'
            """

        return q
    
    def query_rx_codes(self, svc_year):

        q = f"""
            SELECT P.enrolid  as member_id
                   , P.ndcnum
            FROM emo_edw.ibm_marketscan.all_drug_claims P
            --where to_char(P.svcdate, 'YYYY') = '{svc_year}'
            """
        return q
    
    # #old
    # def query_utilization_prelim(self, start_year):

    #     q = f"""
    #         with #Utilization as (select ac.svcdate
    #                                 , ac.enrolid
    #                                 , max(case when lsc.description like '% IP %' then 1 else 0 end)                as IP_flg
    #                                 , max(case when lsc.description like '% OP %' then 1 else 0 end)                as OP_flg
    #                                 , max(case when lsc.description like '%Physician Specialty%' then 1 else 0 end) as Specialty_flg
    #                                 , max(case when lsc.description like '%Preventive Visits%' then 1 else 0 end)   as Preventative_flg
    #                                 , max(case when lsc.description like '% ER %' then 1 else 0 end)                as ER_flg
    #                             from emo_edw.ibm_marketscan.all_claims ac
    #                                     left join lookup_svcscat lsc on ac.svcscat = lsc.svcscat
    #                             where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
    #                             group by svcdate, enrolid)
    #         select to_char(svcdate, 'YYYYMM')   as period
    #             , enrolid                       as ms_member_id
    #             , sum(IP_flg)                   as IP_days
    #             , sum(OP_flg)                   as OP_days
    #             , sum(Specialty_flg)            as Specialty_days
    #             , sum(Preventative_flg)         as Preventative_days
    #             , sum(ER_flg)                   as ER_days
    #         from #Utilization
    #         group by period, enrolid
    #         """
              
    #     return q
    
    # #old
    # def query_util_2(self, start_year):

    #     q = f"""
    #         select ac.enrolid                                                              as member_id
    #             , to_char(ac.svcdate, 'YYYYMM')                                            as service_month
    #             , count(distinct case
    #                                 when ac.revcode in ('0450', '0451', '0452', '0459', '0981')
    #                                     or ac.proc1 between '99281' and '99285'
    #                                     or ac.proc1 between 'G0380' and 'G0384'
    #                                     then ac.enrolid || ac.svcdate end)                as er_days
    #             , count(distinct case
    #                                 when ac.revcode between '0100' and '0169'
    #                                     or ac.revcode between '0200' and '0229'
    #                                     or ac.revcode between '0720' and '0722'
    #                                     or ac.revcode between '0800' and '0804'
    #                                     or ac.revcode = '0987'
    #                                     or ac.revcode between '99221' and '99223'
    #                                     or ac.revcode between '99231' and '99233'
    #                                     or ac.revcode between '99238' and '99239'
    #                                     or ac.revcode between '99251' and '99255'
    #                                     or ac.revcode between '99291' and '99292'
    #                                     or ac.revcode between '99356' and '99357'
    #                                     then ac.enrolid || ac.svcdate end)                as ip_days
    #             , count(distinct case
    #                                 when ac.revcode in ('0456')
    #                                     or ac.proc1 in ('S9083', 'S9088')
    #                                     then ac.enrolid || ac.svcdate end)                as uc_days
    #             , count(distinct case
    #                                 when ac.revcode in ('0403')
    #                                     then ac.enrolid || ac.svcdate end)                as mamm_screening
    #             , count(distinct case
    #                                 when ac.revcode in ('0360', '0361', '0362', '0369')
    #                                     then ac.enrolid || ac.svcdate end)                as ip_surgical_days
    #             , count(distinct case
    #                                 when ac.revcode in ('0500', '0509')
    #                                     then ac.enrolid || ac.svcdate end)                as gen_outpatient_days
    #         from emo_edw.ibm_marketscan.all_medical_claims ac
    #         where to_char(ac.svcdate, 'YYYY') >= '{start_year}'
    #         and ac.revcode in ('0450', '0451', '0452', '0459', '0403', '0456', '0360', '0361', '0362', '0369', '0500', '0509')
    #         group by 1, 2
    #         limit 200
    #         """
        
    #     return q

        # limited to relevant conditions/diagnosed twice in notebook
    # def query_all_conditions(self):
    #     q = f"""
    #             SELECT distinct to_char(AC.svcdate, 'YYYY') as svc_yr
    #                             , AC.svcdate
    #                             , AC.enrolid                  as member_id
    #                             , AC.dx1                      as dx
    #             FROM emo_edw.ibm_marketscan.all_medical_claims AC
    #             UNION
    #             DISTINCT
    #             SELECT distinct to_char(AC.svcdate, 'YYYY') as svc_yr
    #                             , AC.svcdate
    #                             , AC.enrolid                  as member_id
    #                             , AC.dx2                      as dx
    #             FROM emo_edw.ibm_marketscan.all_medical_claims AC
    #             UNION
    #             DISTINCT
    #             SELECT distinct to_char(AC.svcdate, 'YYYY') as svc_yr
    #                             , AC.svcdate
    #                             , AC.enrolid                  as member_id
    #                             , AC.dx3                      as dx
    #             FROM emo_edw.ibm_marketscan.all_medical_claims AC
    #             UNION
    #             DISTINCT
    #             SELECT distinct to_char(AC.svcdate, 'YYYY') as svc_yr
    #                             , AC.svcdate
    #                             , AC.enrolid                  as member_id
    #                             , AC.dx4                      as dx
    #             FROM emo_edw.ibm_marketscan.all_medical_claims AC
    #     """
    #     return q

    # def query_all_admits(self):

    #     q = f"""
    #         -- inpatient admit
    #         SELECT distinct cast(AC.enrolid as varchar) as member_id
    #             , AC.pdx
    #         FROM emo_edw.ibm_marketscan.ccaei AC

    #         UNION DISTINCT

    #         -- er visit with condition as primary diagnosis
    #         SELECT distinct AC.enrolid                  as member_id
    #                     , AC.dx1
    #         FROM emo_edw.ibm_marketscan.all_medical_claims AC
    #         WHERE svcscat IN ('svcscat', '10120', '10220', '10320', '10420', '10520', '12220', '20120', '20220', '21120',
    #                             '21220', '22120', '22320', '30120', '30220', '30320', '30420', '30520', '30620', '31120',
    #                             '31220', '31320', '31420', '31520', '31620')
    #         """
    #     return q