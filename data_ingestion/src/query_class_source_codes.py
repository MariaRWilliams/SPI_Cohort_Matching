#contains queries used as temp tables to run cross-database queries
class QueryClass():

    def __init__(self):
        """
        no variables to initialize yet 
        """
  
    #edw: for chronic conditions
    def query_cond(self):

        q = f"""
            SELECT DISTINCT value_set_nm as condition,
                            replace(cd_sys_concept_cd, '.', '') as ms_diag,
                            cd_sys_concept_cd,
                            replace(cd_sys_concept_display, CHR(39), '')  as description,
                            '%' || replace(cd_sys_concept_cd, '.', '%') || '%' as ilike_joiner
            FROM acp_edw.info_layer.terminology_value_set_compose_dtl
            WHERE value_set_nm IN ('MALIGNANT_NEOPLASM', 'BLOOD_DYSCRASIAS', 'DIABETES_MELLITUS', 'SUBSTANCE_DEPENDENCE', 
                                    'RHEUMATOID_ARTHRITIS', 'PERIPHERAL_VASCULAR_DISEASE', 'CARDIOVASCULAR_DISEASE', 'CORONARY_ARTERY_DISEASE', 
                                    'OSTEOARTHRITIS', 'INFLAMMATORY_BOWEL_DISEASE', 'HYPERTENSION', 'LIVER_DISEASE_ALL', 
                                    'IMMUNODEFICIENCY_DISORDERS', 'HEART_FAILURE', 'ASTHMA', 'DEPRESSION', 'PSYCHOSIS_AFFECTIVE', 
                                    'CKD3TO5_ESRD_DIALYSIS', 'COPD', 'EATING_DISORDERS', 'HYPERLIPIDEMIA', 'ATRIAL_FIBRILLATION', 
                                    'OBESITY_MORBID', 'CHRONIC_PAIN')
            AND cd_sys_sts != 'retired'
            AND cd_sys_concept_sts != 'retired'
            AND value_set_sts != 'retired'
            AND value_Set_compose_sts != 'retired'
            ORDER BY value_set_nm, ilike_joiner
            """
              
        return q
    
    #edw: for chronic conditions (diabetes)
    def query_d_med(self):

        q = f"""
            SELECT 'DIABETES_MELLITUS' as condition,
                    cd_sys_concept_cd as ndc
            FROM acp_edw.info_layer.terminology_value_set_compose_dtl 
            WHERE value_set_nm IN ('INSULIN', 'DIABETES_MEDS_NOT_INSULIN')
            AND cd_sys_sts != 'retired'
            AND cd_sys_concept_sts != 'retired'
            AND value_set_sts != 'retired'
            AND value_Set_compose_sts != 'retired'
            """
              
        return q
    
    #edw: for chronic conditions: exclusions for diabetes, but none of these are results from the query above, so not necessary?
    def query_d_med_excl(self):

        q = f"""
            SELECT DISTINCT cd_sys_concept_cd 
            FROM acp_edw.info_layer.terminology_value_set_compose_dtl 
            WHERE value_set_nm IN ('GLP1_RA')
            AND cd_sys_sts != 'retired'
            AND cd_sys_concept_sts != 'retired'
            AND value_set_sts != 'retired'
            AND value_Set_compose_sts != 'retired';
            """
              
        return q
    
    #edw: for utilization: avoidable er
    def query_avoidable_er(self):

        q = f"""
            SELECT DISTINCT value_set_nm as condition,
                                cd_sys_concept_cd,
                                replace(cd_sys_concept_display, CHR(39), '')  as description,
                                '%' || replace(cd_sys_concept_cd, '.', '%') || '%' as ilike_joiner,
                                replace(cd_sys_concept_cd, '.', '') as ms_joiner,
                                'Avoidable ER' as category
                FROM acp_edw.info_layer.terminology_value_set_compose_dtl
                WHERE value_set_nm in ('ALLERGIC_CONDITIONS', 'BRONCHITIS_ACUTE','CONJUNCTIVITIS',
                                        'CONSTIPATION', 'HEADACHE', 'LOWER_BACK_PAIN', 'OTITIS_MEDIA',
                                        'PRESCRIPTION_REFILL', 'RASH_DERMATITIS_INSECT_BITES', 'SPRAINS_STRAINS',
                                        'UPPER_RESPIRATORY_INFECTION', 'URINARY_INFECTION')
                AND cd_sys_sts != 'retired'
                AND cd_sys_concept_sts != 'retired'
                AND value_set_sts != 'retired'
                AND value_Set_compose_sts != 'retired'
              """

        return q
    
    #emo: main utilization table
    def query_utilization_codes(self):

        q = f"""
            --no temp tables so can use through DB
            select p_code
                , o_code
                , r_code
                , description
                , category
            from (select ''                            as p_code
                    , code                             as o_code
                    , 'R' + right(code, len(code) - 1) as r_code
                    , description
                    , case
                            when description like '%Emergency Room%' then 'ER'
                            when description like '%Urgent Care%' then 'UC'
                            when description like '%Screening Mammography%' then 'Mammogram'
                            when description like '%Room & Board%' then 'Inpatient'
                            when description like '%room and board%' then 'Inpatient'
                            when description like '%Labor Room%' then 'Inpatient'
                            when description like '%Inpatient Renal Dialysis%' then 'Inpatient'
                            when description like '%Hospital Visit%' then 'Inpatient'
                            when description like '%Intensive Care%' then 'Inpatient'
                            when description like '%Coronary Care%' then 'Inpatient'
                            when description like '%Operating Room Services%' then 'Surgery'
                            else 'x' end                 as category
                from emo_edw.ibm_marketscan.lookup_revcode_2024
                where description like '%Emergency Room%'
                    or description like '%Screening Mammography%'
                    or description like '%Room & Board%'
                    or description like '%room and board%'
                    or description like '%Labor Room%'
                    or description like '%Inpatient Renal Dialysis%'
                    or description like '%Hospital Visit%'
                    or description like '%Intensive Care%'
                    or description like '%Coronary Care%'
                    or description like '%Operating Room Services%'

                    UNION

                select stdplac as p_code
                        , '' as o_code
                        , '' as r_code
                        , description
                        , case
                    when description like '%Emergency Room%' then 'ER'
                    when description like '%Office%' then 'Office Visits'
                    when description like '%Inpatient Hospital%' then 'Inpatient'
                    else 'x' end as category
                from emo_edw.ibm_marketscan.lookup_stdplac
                where description like '%Emergency Room%'
                    or description like '%Office%'
                    or description like '%Inpatient Hospital%'
                ) tbl
              """

        return q