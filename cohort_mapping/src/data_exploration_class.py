import pyspark.sql.functions as F
import pyspark.sql.types as T
from functools import reduce

class Data_Stats():

    def __init__(self):
        """
        no variables to initialize yet 
        """

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