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
        
    def query_demographics(self, schema, start_year):

        q = f"""
            select dw_member_id
                , udf26_eligibility        as person_id
                , to_char(mbr_dob, 'YYYY') as birth_year
                , mbr_gender               as sex
                , mbr_msa                  as msa
                , mbr_state                as state
                , mbr_region_name          as region
                , ins_plan_type_desc       as plan_type
                , ins_emp_group_name       as customer_nm
                , min(ins_med_eff_date)    as start_date
                , max(ins_med_term_date)   as end_date
            from {schema}.eligibility
            where to_char(ins_med_term_date, 'YYYY') >= '{start_year}'
            group by 1,2,3,4,5,6,7,8,9
            """
              
        return q
    
    def query_med_claims(self, schema, start_year):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM') as service_month
                , dw_member_id
                , sum(rev_allowed_amt)                     as med_allowed
            from {schema}.medical
            where to_char(svc_service_frm_date, 'YYYY') >= '{start_year}'
            group by 1, 2
            """
              
        return q

    def query_pharma_claims(self, schema, start_year):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM') as service_month
                , dw_member_id
                , sum(rev_allowed_amt)                     as pharma_allowed
            from {schema}.pharmacy	
            where to_char(svc_service_frm_date, 'YYYY') >= '{start_year}'
            group by 1, 2
            """
              
        return q
    
    def query_utilization(self, schema, start_year):

        q = f"""
               SELECT   dw_member_id,
                        to_char(servicedate, 'YYYYMM') as service_month,
                        categorydescription,
                        eventtype,
    	                case when eventtype = 'Neither' then count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     when eventtype = 'Reversal' then -count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     else 0 end as count_units
               FROM     {schema}.utilization
    	       WHERE    to_char(servicedate, 'YYYY') >= '{start_year}'
                        AND categorydescription in ('Emergency Room','Physician-Specialist Visit',
                                                    'Physician-PCP Visit','Physician-Preventive','Outpatient Urgent Care'
                                                )
                GROUP BY 1, 2,3, 4
            """
              
        return q
    
    def query_conditions(self, schema, start_year):
        
        q = f"""
                select dw_member_id
                , max(case
                        when svc_diag_1_code between 'F32' and 'F33' then 1
                        when svc_diag_2_code between 'F32' and 'F33' then 1
                        when svc_diag_3_code between 'F32' and 'F33' then 1
                        when svc_diag_4_code between 'F32' and 'F33' then 1
                        else 0
                end) as depression
                , max(case
                        when svc_diag_1_code between 'E78' and 'E785' then 1
                        when svc_diag_2_code between 'E78' and 'E785' then 1
                        when svc_diag_3_code between 'E78' and 'E785' then 1
                        when svc_diag_4_code between 'E78' and 'E785' then 1
                        else 0
                end) as hyperlipidemia
                , max(case
                        when svc_diag_1_code between 'M15' and 'M19' then 1
                        when svc_diag_2_code between 'M15' and 'M19' then 1
                        when svc_diag_3_code between 'M15' and 'M19' then 1
                        when svc_diag_4_code between 'M15' and 'M19' then 1
                        else 0
                end) as osteoarthritis
                , max(case
                        when svc_diag_1_code between 'I502' and 'I5043' then 1
                        when svc_diag_2_code between 'I502' and 'I5043' then 1
                        when svc_diag_3_code between 'I502' and 'I5043' then 1
                        when svc_diag_4_code between 'I502' and 'I5043' then 1
                        else 0
                end) as CHF
                , max(case
                        when svc_diag_1_code between 'C00' and 'C7A' then 1
                        when svc_diag_2_code between 'C00' and 'C7A' then 1
                        when svc_diag_3_code between 'C00' and 'C7A' then 1
                        when svc_diag_4_code between 'C00' and 'C7A' then 1
                        else 0
                end) as cancer
                , max(case
                        when svc_diag_1_code between 'E08' and 'E13' then 1
                        when svc_diag_2_code between 'E08' and 'E13' then 1
                        when svc_diag_3_code between 'E08' and 'E13' then 1
                        when svc_diag_4_code between 'E08' and 'E13' then 1
                        else 0
                end) as diabetes
                , max(case
                        when svc_diag_1_code between 'I25.1' and 'I25.119' then 1
                        when svc_diag_2_code between 'I25.1' and 'I25.119' then 1
                        when svc_diag_3_code between 'I25.1' and 'I25.119' then 1
                        when svc_diag_4_code between 'I25.1' and 'I25.119' then 1
                        else 0
                end) as CAD
                , max(case
                        when svc_diag_1_code between 'J44' and 'J44.9' then 1
                        when svc_diag_2_code between 'J44' and 'J44.9' then 1
                        when svc_diag_3_code between 'J44' and 'J44.9' then 1
                        when svc_diag_4_code between 'J44' and 'J44.9' then 1
                        else 0
                end) as COPD
            from {schema}.medical
            where to_char(svc_service_frm_date, 'YYYY') >= '{start_year}'
            group by 1
            having  depression+hyperlipidemia+osteoarthritis+cancer+CHF+diabetes+CAD+COPD >= 1
            """
            
        return q