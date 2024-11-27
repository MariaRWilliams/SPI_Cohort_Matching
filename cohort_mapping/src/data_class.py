import pyspark.sql.functions as F
import pyspark.sql.types as T
from functools import reduce

class Data_Processing():

    def __init__(self):
        """
        no variables to initialize yet 
        """
    
    def query_data(self, spark, dbutils, table):

        df = spark.sql(f"""SELECT * FROM dev.`clinical-analysis`.{table}""")

        return df

    def num_col_stats(self, df, threshold_multiplier):

        num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (T.IntegerType, T.NumericType))]
        df_stats = df.select(*num_cols).describe()
        df_stats = df_stats.toPandas()
        df_stats = df_stats.transpose().reset_index()
        df_stats.columns = df_stats.iloc[0]
        df_stats = df_stats[1:]
        df_stats = df_stats.rename(columns={'summary':'column_name'})

        df_stats = df_stats[df_stats['max'].astype(float)>1]

        df_stats['outlier threshold'] = df_stats['stddev'].astype(float)*threshold_multiplier
        df_stats['outlier threshold'] = df_stats['outlier threshold'].round(2)

        # outlier_dict = df_stats[['column_name','outlier threshold']].set_index('column_name').to_dict('index')
        # filter_col = reduce(lambda x, y: x & y, [F.col(k) > v for (k, v) in outlier_dict.items()])
        # print(outlier_dict)
        # print(filter_col)

        return df_stats.sort_values(by=['column_name'])
    
    def sample_matches(self, final_matched, num_sample, category=0):

        if category != 0:
            final_matched = final_matched.filter(F.col('category').contains(category))

        exposed_df = final_matched.filter(~F.col('category').contains('control'))
        control_df = final_matched.filter(F.col('category').contains('control'))

        sample_exposed = exposed_df.withColumn('key',F.rand()).orderBy('key').limit(num_sample).cache()
        sample_exposed = sample_exposed.drop('key')

        sample_list = sample_exposed.toPandas()
        sample_list = sample_list['match_key'].to_list()

        sample_control = control_df.filter(F.col('match_key').isin(sample_list))

        return sample_exposed, sample_control