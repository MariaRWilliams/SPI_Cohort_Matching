# Databricks notebook source
# MAGIC %md
# MAGIC ###Run Matching Model
# MAGIC - load cohorts and display stats
# MAGIC - choose matching variables
# MAGIC - scale, etc
# MAGIC - run matching algorithm
# MAGIC - export matching index

# COMMAND ----------

from src import prep_class
from src import matching_class
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder
import pyspark.sql.functions as F
import pyspark.sql.types as T

pc = prep_class.Data_Prep()
mc = matching_class.Cohort_Matching()
sc = StandardScaler()

# COMMAND ----------

full_df = pc.query_data(spark, dbutils, 'cohort_matching_cohorts')

# COMMAND ----------

#in each category: count of members, mean value, stddev, min, max of total_allowed?
#full_df.columns
print('Category sample size:')
full_df.select('category', 'person_id').groupby('category').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Choose Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scale and Vectorize

# COMMAND ----------

full_df.columns

# COMMAND ----------

unchanged_columns = ['person_id', 'category','depression', 'hyperlipidemia', 'osteoarthritis', 'chf', 'cancer', 'diabetes', 'cad', 'copd']
scale_columns = ['total_allowed-1', 'total_allowed-2', 'total_allowed-3', 'total_allowed0']
cat_columns = ['sex']


# COMMAND ----------


#change scale columns to vector and scale
scale_assembler = VectorAssembler().setInputCols(scale_columns).setOutputCol("vector")
df_with_vector = scale_assembler.transform(full_df)

scaler = StandardScaler(inputCol="vector", outputCol="scaledFeatures")
scaler_model = scaler.fit(df_with_vector.select("vector"))
full_df_scaled = scaler_model.transform(df_with_vector)

full_df_scaled = mc.unpack_vector(full_df_scaled, unchanged_columns, scale_columns, 'scaledFeatures')

# COMMAND ----------

#change cat_columns to individual columns

new_df = (full_df
          .select(*cat_columns, *unchanged_columns)
          .groupBy(*unchanged_columns)
          .pivot(*cat_columns)
          .agg(F.first(*cat_columns))
)




# COMMAND ----------

new_df = new_df.na.fill(0)

new_df.display()

# COMMAND ----------

#reload
import importlib
from src import matching_class

importlib.reload(matching_class)
mc = matching_class.Cohort_Matching()
