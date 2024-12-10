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
              WHERE t.table_schema like 'stage1_%_%_extract'
              """

        return q

    def query_customers(self, schema):

        q = f"""
              select distinct ins_emp_group_name as cg_cust
              from {schema}.eligibility
              """

        return q
           
    def query_med_claims(self, schema, svc_year):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM')  as service_month
                , dw_member_id
                , sum(rev_allowed_amt)                      as med_total
                , sum(rev_paid_amt)                         as med_total_net
            from {schema}.medical
            where to_char(svc_service_frm_date, 'YYYY') = '{svc_year}'
            and rev_allowed_amt != 0
            group by 1, 2
            """
              
        return q

    def query_pharma_claims(self, schema, svc_year):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM')  as service_month
                , dw_member_id
                , sum(rev_allowed_amt)                      as pharma_total
                , sum(rev_paid_amt)                         as pharma_total_net
            from {schema}.pharmacy	
            where to_char(svc_service_frm_date, 'YYYY') = '{svc_year}'
            and rev_allowed_amt != 0
            group by 1, 2
            """
              
        return q
    
    def query_utilization(self, schema, svc_year):

        q = f"""
               SELECT   dw_member_id,
                        to_char(servicedate, 'YYYYMM') as service_month,
                        count(distinct case when utilizationcategory = 'ER Visit' then visitid end) as er_visits,
                        count(distinct case when utilizationcategory = 'ER Visit' and avoidableflag = 1 then visitid end) as avoidable_er_visits,
                        count(distinct case when utilizationcategory = 'Inpatient Admission' then visitid end) as ip_admits,
                        count(distinct case when utilizationcategory = 'Inpatient Admission' and readmissionflag = 'Y' then visitid end) as ip_readmits,
                        count(distinct case when utilizationcategory = 'Inpatient Admission' and eradmissionflag = 'true' then visitid end) as ip_er_admits,
                        count(distinct case when utilizationcategory = 'Outpatient Surgery' then visitid end) as op_surgery,
                        count(distinct case when utilizationcategory = 'Office Visit' then visitid end) as office_visits,
                        count(distinct case when utilizationcategory = 'Urgent Care Visit' then visitid end) as uc_visits
               FROM     {schema}.utilization
    	       WHERE    to_char(servicedate, 'YYYY') = '{svc_year}'
                GROUP BY 1, 2
            """
              
        return q
    
    def query_utilization_original(self, schema, svc_year):

        q = f"""
               SELECT   dw_member_id,
                        to_char(servicedate, 'YYYYMM') as service_month,
                        categorydescription,
                        eventtype,
    	                case when eventtype = 'Neither' then count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     when eventtype = 'Reversal' then -count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     else 0 end as count_units
               FROM     {schema}.utilization
    	       WHERE    to_char(servicedate, 'YYYY') = '{svc_year}'
                        AND categorydescription in ('Emergency Room','Physician-Specialist Visit',
                                                    'Physician-PCP Visit','Physician-Preventive','Outpatient Urgent Care',
                                                    'Physician-Telehealth', 'Inpatient Medical','Inpatient Surgical',
                                                    'Office Procedures', 'Outpatient Services'
                                                )
                GROUP BY 1, 2, 3, 4
            """
              
        return q
    
    def query_demographics(self, schema, svc_year):

        q = f"""
            select dw_member_id
                , ins_emp_group_name                  as dw_customer_nm
                , udf26_eligibility                   as person_id
                , to_char(mbr_dob, 'YYYY')            as birth_year
                , mbr_gender                          as sex
                , mbr_msa                             as msa_4
                , mbr_state                           as state_abr
                , mbr_zip                             as zip_code
                , ins_carrier_name                    as carrier
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