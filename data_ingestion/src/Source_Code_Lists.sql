SELECT DISTINCT value_set_nm as condition,
                cd_sys_concept_cd,
                replace(cd_sys_concept_display, CHR(39), '')  as description,
                '%' || replace(cd_sys_concept_cd, '.', '%') || '%' as ilike_joiner
FROM info_layer.terminology_value_set_compose_dtl
WHERE value_set_nm IN ('MALIGNANT_NEOPLASM', 'BLOOD_DYSCRASIAS', 'DIABETES_MELLITUS', 'SUBSTANCE_DEPENDENCE', 'RHEUMATOID_ARTHRITIS', 'PERIPHERAL_VASCULAR_DISEASE', 'CARDIOVASCULAR_DISEASE', 'CORONARY_ARTERY_DISEASE', 'OSTEOARTHRITIS', 'INFLAMMATORY_BOWEL_DISEASE', 'HYPERTENSION', 'LIVER_DISEASE_ALL', 'IMMUNODEFICIENCY_DISORDERS', 'HEART_FAILURE', 'ASTHMA', 'DEPRESSION', 'PSYCHOSIS_AFFECTIVE', 'CKD3TO5_ESRD_DIALYSIS', 'COPD', 'EATING_DISORDERS', 'HYPERLIPIDEMIA', 'ATRIAL_FIBRILLATION', 'OBESITY_MORBID', 'CHRONIC_PAIN')
AND cd_sys_sts != 'retired'
AND cd_sys_concept_sts != 'retired'
AND value_set_sts != 'retired'
AND value_Set_compose_sts != 'retired'
ORDER BY value_set_nm, ilike_joiner;

SELECT cd_sys_concept_cd
FROM info_layer.terminology_value_set_compose_dtl WHERE value_set_nm IN ('INSULIN', 'DIABETES_MEDS_NOT_INSULIN')
AND cd_sys_sts != 'retired'
AND cd_sys_concept_sts != 'retired'
AND value_set_sts != 'retired'
AND value_Set_compose_sts != 'retired'
;

SELECT DISTINCT cd_sys_concept_cd FROM info_layer.terminology_value_set_compose_dtl WHERE value_set_nm IN ('GLP1_RA')
AND cd_sys_sts != 'retired'
AND cd_sys_concept_sts != 'retired'
AND value_set_sts != 'retired'
AND value_Set_compose_sts != 'retired';
;
