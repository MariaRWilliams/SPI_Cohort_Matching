class QueryClass():

    def __init__(self):
        """
        no variables to initialize yet 
        """
        
    def query_schema(self):

        q = f"""
              SELECT distinct
                     t.table_schema as table_schema,
                     split_part(t.table_schema,'_',3) as acronym
              FROM information_schema.tables t
              join information_schema.tables e on t.table_schema = e.table_schema and e.table_name='eligibility'
              join information_schema.tables m on t.table_schema = m.table_schema and m.table_name='medical'
              join information_schema.tables p on t.table_schema = p.table_schema and p.table_name='pharmacy'
              join information_schema.tables u on t.table_schema = u.table_schema and u.table_name='utilization'
              join information_schema.tables q on t.table_schema = q.table_schema and q.table_name='accoladequalitymetrics'
              join information_schema.tables g on t.table_schema = g.table_schema and g.table_name='genericqualitymetrics'
              WHERE t.table_schema like 'stage1_acl_%_extract'
              and t.table_schema != 'stage1_acl_acl_extract'
              """

        return q

    def query_customers(self, schema):

        q = f"""
              select distinct ins_emp_group_name as cg_cust
              from {schema}.eligibility
              """

        return q
    
    def query_demographics(self, schema, svc_year):

        q = f"""
            select dw_member_id                       as member_id
                , ins_emp_group_name                  as dw_customer_nm
                , udf26_eligibility                   as person_id
                , to_char(mbr_dob, 'YYYY')            as birth_year
                , mbr_gender                          as sex
                , mbr_state                           as state_abr
                , mbr_zip                             as zip_code
                , ins_carrier_name                    as carrier
                --, case when mbr_msa != 'Blank'
                --    then mbr_msa
                --    else None end                     as msa_4
                , mbr_msa                               as msa_4
                , max(case when mbr_msa = '0000' then 1
                    else 0 end)                       as rural
                , max(case when mbr_msa is null 
                    or mbr_msa = 'Blank' then 0
                    else 1 end)                       as msa_provided
                , min(ins_med_eff_date)               as start_date
                , case
                    when max(ins_med_term_date) > current_date
                        then current_date
                    else max(ins_med_term_date) end as end_date
            from {schema}.eligibility
            where to_char(ins_med_term_date, 'YYYY') >= '{svc_year}'
            and to_char(ins_med_eff_date, 'YYYY') <= '{svc_year}'
            group by 1,2,3,4,5,6,7,8,9
            """
              
        return q
           
    def query_med_claims(self, schema, svc_year):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM')  as service_month
                , dw_member_id                              as member_id
                , sum(rev_allowed_amt)                      as med_total
                , sum(rev_paid_amt)                         as med_total_net
            from {schema}.medical
            where to_char(svc_service_frm_date, 'YYYY') = '{svc_year}'
            and rev_allowed_amt != 0
            group by 1, 2
            """
              
        return q

    def query_rx_claims(self, schema, svc_year):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM')  as service_month
                , dw_member_id                              as member_id
                , sum(rev_allowed_amt)                      as pharma_total
                , sum(rev_paid_amt)                         as pharma_total_net
            from {schema}.pharmacy	
            where to_char(svc_service_frm_date, 'YYYY') = '{svc_year}'
            and rev_allowed_amt != 0
            group by 1, 2
            """
              
        return q

    #query all codes and limit with tables pulled elsewhere
    def query_med_codes(self, schema, svc_year):

        q = f""" select med.dw_member_id                                                        as member_id
                        , med.svc_service_frm_date                                              as svc_start
                        , med.svc_service_to_date                                               as svc_end
                        , med.svc_cpt_code                                                      as code_cpt
                        , med.svc_hcpcs_code                                                    as code_hcpcs
                        , med.svc_pos_code                                                      as code_pos
                        , med.svc_rev_code                                                      as code_rev 
                        , med.svc_diag_1_code                                                   as dx1
                        , med.svc_diag_2_code                                                   as dx2
                        , med.svc_diag_3_code                                                   as dx3
                        , med.svc_diag_4_code                                                   as dx4
            from {schema}.medical med
            where to_char(med.svc_service_frm_date, 'YYYY') = '{svc_year}'
            """  

        return q
    
    def query_rx_codes(self, schema, svc_year):

        q = f"""
            select distinct dw_member_id as member_id
                        , svc_ndc_code
            from {schema}.pharmacy
            where to_char(svc_service_frm_date, 'YYYY') = '{svc_year}'
            """
        return q
        
    # #old
    # def query_utilization_3(self, schema, svc_year):

    #     q = f"""
    #         select med.dw_member_id
    #             , to_char(med.svc_service_frm_date, 'YYYYMM')                                       as service_month
    #             , count(distinct case
    #                                 when med.svc_rev_code in ('R450', 'R451', 'R452', 'R459', 'R981')
    #                                     or med.svc_cpt_code between '99281' and '99285'
    #                                     or med.svc_hcpcs_code between 'G0380' and 'G0384'
    #                                     then dw_member_id || svc_service_frm_date end)              as er_days
    #             , count(distinct case
    #                                 when (med.svc_rev_code in ('R450', 'R451', 'R452', 'R459', 'R981')
    #                                     or med.svc_cpt_code between '99281' and '99285'
    #                                     or med.svc_hcpcs_code between 'G0380' and 'G0384')
    #                                     and avoidable_flag = 1
    #                                     then dw_member_id || svc_service_frm_date end)              as avoidable_er_days
    #             , count(distinct case
    #                                 when med.svc_rev_code between 'R100' and 'R169'
    #                                     or med.svc_rev_code between 'R200' and 'R229'
    #                                     or med.svc_rev_code between 'R720' and 'R722'
    #                                     or med.svc_rev_code between 'R800' and 'R804'
    #                                     or med.svc_rev_code = 'R987'
    #                                     or med.svc_cpt_code between '99221' and '99223'
    #                                     or med.svc_cpt_code between '99231' and '99233'
    #                                     or med.svc_cpt_code between '99238' and '99239'
    #                                     or med.svc_cpt_code between '99251' and '99255'
    #                                     or med.svc_cpt_code between '99291' and '99292'
    #                                     or med.svc_cpt_code between '99356' and '99357'
    #                                     then dw_member_id || svc_service_frm_date end)              as ip_days
    #             , count(distinct case
    #                                 when (med.svc_rev_code between 'R100' and 'R169'
    #                                     or med.svc_rev_code between 'R200' and 'R229'
    #                                     or med.svc_rev_code between 'R720' and 'R722'
    #                                     or med.svc_rev_code between 'R800' and 'R804'
    #                                     or med.svc_rev_code = 'R987'
    #                                     or med.svc_cpt_code between '99221' and '99223'
    #                                     or med.svc_cpt_code between '99231' and '99233'
    #                                     or med.svc_cpt_code between '99238' and '99239'
    #                                     or med.svc_cpt_code between '99251' and '99255'
    #                                     or med.svc_cpt_code between '99291' and '99292'
    #                                     or med.svc_cpt_code between '99356' and '99357')
    #                                     and thirtydayreadmit > 0
    #                                     then dw_member_id || svc_service_frm_date end)              as ip_readmit_days
    #             , count(distinct case
    #                                 when med.svc_rev_code = 'R456'
    #                                     or med.svc_hcpcs_code in ('S9083', 'S9088')
    #                                     then dw_member_id || svc_service_frm_date end)              as uc_days
    #             , count(distinct case
    #                                 when med.svc_rev_code in ('R403')
    #                                     then dw_member_id || svc_service_frm_date end)              as mamm_screening
    #             , count(distinct case
    #                                 when med.svc_rev_code in ('R360', 'R361', 'R362', 'R369')
    #                                     then dw_member_id || svc_service_frm_date end)              as ip_surgical_days
    #             , count(distinct case
    #                                 when med.svc_rev_code in ('R500', 'R509')
    #                                     then dw_member_id || svc_service_frm_date end)              as gen_outpatient_days
    #         from {schema}.medical med
    #         where to_char(svc_service_frm_date, 'YYYY') = '{svc_year}'
    #         group by 1, 2
    #         """
              
    #     return q
    
    # #old
    # def query_utilization_2(self, schema, svc_year):

    #     q = f"""
    #            SELECT   dw_member_id,
    #                     to_char(servicedate, 'YYYYMM') as service_month,
    #                     count(distinct case when utilizationcategory = 'ER Visit' then visitid end) as er_visits,
    #                     count(distinct case when utilizationcategory = 'ER Visit' and avoidableflag = 1 then visitid end) as avoidable_er_visits,
    #                     count(distinct case when utilizationcategory = 'Inpatient Admission' then visitid end) as ip_admits,
    #                     count(distinct case when utilizationcategory = 'Inpatient Admission' and readmissionflag = 'Y' then visitid end) as ip_readmits,
    #                     count(distinct case when utilizationcategory = 'Inpatient Admission' and eradmissionflag = 'true' then visitid end) as ip_er_admits,
    #                     count(distinct case when utilizationcategory = 'Outpatient Surgery' then visitid end) as op_surgery,
    #                     count(distinct case when utilizationcategory = 'Office Visit' then visitid end) as office_visits,
    #                     count(distinct case when utilizationcategory = 'Urgent Care Visit' then visitid end) as uc_visits
    #            FROM     {schema}.utilization
    # 	       WHERE    to_char(servicedate, 'YYYY') = '{svc_year}'
    #             GROUP BY 1, 2
    #         """
              
    #     return q
    
    # #old
    # def query_utilization_original(self, schema, svc_year):

    #     q = f"""
    #            SELECT   dw_member_id,
    #                     to_char(servicedate, 'YYYYMM') as service_month,
    #                     categorydescription,
    #                     eventtype,
    # 	                case when eventtype = 'Neither' then count(distinct dw_member_id || servicedate || primaryprovidernpi)
    # 	                     when eventtype = 'Reversal' then -count(distinct dw_member_id || servicedate || primaryprovidernpi)
    # 	                     else 0 end as count_units
    #            FROM     {schema}.utilization
    # 	       WHERE    to_char(servicedate, 'YYYY') = '{svc_year}'
    #                     AND categorydescription in ('Emergency Room','Physician-Specialist Visit',
    #                                                 'Physician-PCP Visit','Physician-Preventive','Outpatient Urgent Care',
    #                                                 'Physician-Telehealth', 'Inpatient Medical','Inpatient Surgical',
    #                                                 'Office Procedures', 'Outpatient Services'
    #                                             )
    #             GROUP BY 1, 2, 3, 4
    #         """
              
    #     return q