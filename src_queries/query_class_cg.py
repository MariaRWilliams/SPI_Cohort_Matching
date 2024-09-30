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
        
    def query_demographics(self, start_year):

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
            from stage1_acl_fcb_extract.eligibility
            where to_char(ins_med_term_date, 'YYYY') >= '{start_year}'
            group by 1,2,3,4,5,6,7,8
            """
              
        return q
    
    def query_med_claims(self, start_year, schema):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM') as service_month
                , dw_member_id
                , sum(rev_allowed_amt)                     as allowed
            from {schema}.medical
            where to_char(svc_service_frm_date, 'YYYY') >= '{start_year}'
            group by 1, 2
            """
              
        return q

    def query_pharma_claims(self, start_year, schema):

        q = f"""
            select to_char(svc_service_frm_date, 'YYYYMM') as service_month
                , dw_member_id
                , sum(rev_allowed_amt)                     as allowed
            from {schema}.pharmacy	
            where to_char(svc_service_frm_date, 'YYYY') >= '{start_year}'
            group by 1, 2
            """
              
        return q
    
    def query_utilization(self, start_year, schema):

        q = f"""
               SELECT   dw_member_id,
                        to_char(servicedate, 'YYYYMM') as service_month,
                        CASE WHEN categorydescription = 'Outpatient Services' Then 'Outpatient Surgery'
                             WHEN categorydescription = 'Office Procedures' THEN 'Office Surgery'
                             WHEN categorydescription in ('Inpatient Medical','Inpatient Surgical') THEN categorydescription
                             WHEN categorydescription = 'Emergency Room' THEN 'ER_visit'
                             WHEN categorydescription = 'Physician-Specialist Visit' THEN 'Spec_visit'
                             WHEN categorydescription = 'Physician-PCP Visit' THEN 'PCP_visit'
                             WHEN categorydescription = 'Physician-Preventive' THEN 'Preventive_visit'
                             WHEN categorydescription = 'Outpatient Urgent Care' THEN 'Urgent_care_visit'
                             WHEN categorydescription = 'Physician-Telehealth' THEN 'Telehealth_visit'
                             END as description,
                        eventtype,
    	                case when eventtype = 'Neither' then count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     when eventtype = 'Reversal' then -count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     else 0 end as count_units
               FROM     {schema}.utilization
    	       WHERE    to_char(servicedate, 'YYYY') >= '{start_year}'
    	                AND categorydescription in ('Inpatient Medical','Inpatient Surgical','Emergency Room','Physician-Specialist Visit',
                                                    'Outpatient Surgery -11','Outpatient Surgery -22','Outpatient Surgery -14',
                                                    'Physician-PCP Visit','Physician-Preventive','Physician-Telehealth','Outpatient Urgent Care'
                                                 ) 
               GROUP BY dw_member_id, service_month, description, eventtype;
            """
              
        return q