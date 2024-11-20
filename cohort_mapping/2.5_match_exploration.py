# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Exploration
# MAGIC Exploring matched pairs after they have already been selected

# COMMAND ----------

# MAGIC %pip install faiss-cpu

# COMMAND ----------

from src import prep_class
from src import matching_class
import pyspark.sql.functions as F

pc = prep_class.Data_Prep()
mc = matching_class.Cohort_Matching()

# COMMAND ----------

#matched_df has matching variables, details_df has additional information
matched_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts_matched')
details_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

#check sample
sample_exposed_df, sample_control_df = mc.sample_matches(matched_df, 3)
sample_exposed_df.display()
sample_control_df.display()

# COMMAND ----------

matched_df.groupby('category').agg(F.count('person_id').alias('member_count'), 
                                F.round(F.mean('total_allowed0'), 2).alias('avg spend at period 0'), 
                                F.round(F.mean('age'), 2).alias('avg age'),
                                F.min('utc_period').alias('min_period'),
                                F.max('utc_period').alias('max_period'),
                                ).show()

# COMMAND ----------

cust_list = matched_df.select('edw_cust').distinct().toPandas()['edw_cust'].to_list()

print('Customer Count: '+str(len(cust_list)))

# COMMAND ----------

# MAGIC %md
# MAGIC ####optimizing some other logic

# COMMAND ----------

tester = matched_df.limit(10000)
tester2 = matched_df.limit(10000)


# COMMAND ----------

fixed_cols = ['cancer', 'hyperlipidemia', 'diabetes', 'osteoarthritis', 'depression']
n_list = 2
cohort = 'High Cost Claimants (HCC)'

# COMMAND ----------

#checking
control = matched_df.filter(matched_df['category']=='High Cost Claimants (HCC) control')
exposed = matched_df.filter(matched_df['category']==cohort)

c_agg = control.groupby(*fixed_cols).agg(F.count('person_id').alias('control_count'))
ex_agg = exposed.groupby(*fixed_cols).agg(F.count('person_id').alias('exposed_count'))

print('cohorts discarded: ')
demo_combos = c_agg.join(ex_agg, on=fixed_cols, how='left')
demo_combos.filter(F.col('control_count') <= n_list*40).show()
demo_combos = demo_combos.filter(F.col('control_count') > n_list*40)

dc_num = len(demo_combos.collect())
counter = 1

for row in demo_combos.rdd.toLocalIterator():
#for row in demo_combos.limit(2).rdd.toLocalIterator():
    print('Processing '+cohort+' '+str(counter)+' of '+str(dc_num))

    this_control = control
    this_exposed = exposed
    print(control.count())

    for x in fixed_cols:
        this_control = this_control.filter(this_control[x] == row[x])
        this_exposed = this_exposed.filter(this_exposed[x] == row[x])

    print('datasets prepared')

    control = control.join(this_control, on=control.columns, how='anti')
    exposed = exposed.join(this_exposed, on=control.columns, how='anti')
    print(control.count())

    counter = counter+1

# COMMAND ----------

#optimized version
control = ready_df.filter(ready_df['category']=='control')
exposed = ready_df.filter(ready_df['category']==cohort)

c_agg = control.groupby(*self.fixed_cols).agg(F.count('person_id').alias('control_count'))
ex_agg = exposed.groupby(*self.fixed_cols).agg(F.count('person_id').alias('exposed_count'))

demo_combos = c_agg.join(ex_agg, on=self.fixed_cols, how='left')
disc_demos = demo_combos.filter(F.col('control_count') <= self.n_list*40)
demo_combos = demo_combos.filter(F.col('control_count') > self.n_list*40)

print('cohorts discarded due to control size: ')
disc_demos.show()

dc_num = len(demo_combos.collect())
counter = 1

for row in demo_combos.rdd.toLocalIterator():
#for row in demo_combos.limit(2).rdd.toLocalIterator():
    print('Processing '+cohort+' '+str(counter)+' of '+str(dc_num))

    this_control = control
    this_exposed = exposed

    for x in self.fixed_cols:
        this_control = this_control.filter(this_control[x] == row[x])
        this_exposed = this_exposed.filter(this_exposed[x] == row[x])

    print('datasets prepared')

    # does this add more time than useful?
    # control = control.join(this_control, on=self.id_columns, how='anti')
    # exposed = exposed.join(this_exposed, on=self.id_columns, how='anti')

    #make index
    this_control = this_control.select(*self.id_columns, *self.scale_columns).toPandas()
    control_ids = this_control[self.id_columns]
    control_vars = this_control[self.scale_columns]
    index = self.create_index(control_vars)

    print('index created')

