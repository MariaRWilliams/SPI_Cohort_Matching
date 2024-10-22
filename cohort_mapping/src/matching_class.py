import pandas as pd
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder
import pyspark.sql.functions as F
import pyspark.sql.types as T
from faiss import IndexFlatL2, IndexIVFFlat

class Cohort_Matching():

    def __init__(self):
        """
        no variables to initialize yet 
        """

    def create_vector(self, df, scale_columns):

        assembler = VectorAssembler().setInputCols(scale_columns).setOutputCol("vector")
        df_with_vector = assembler.transform(df)

        return df_with_vector
    
    def unpack_vector(self, df, unchanged_columns, scale_columns, vector_col):

        def split_array_to_list(col):
            def to_list(v):
                return v.toArray().tolist()
            return F.udf(to_list, T.ArrayType(T.DoubleType()))(col)

        df_split = df.withColumn("split_int", split_array_to_list(F.col(vector_col)))
        df_split = df_split.select(*unchanged_columns, *[F.col("split_int")[i].alias(scale_columns[i]) for i in range(len(scale_columns))])

        return df_split
    
    def scale_vector(self, df_with_vector):

        scaler = StandardScaler(inputCol="vector", outputCol="scaledFeatures")
        scaler_model = scaler.fit(df_with_vector.select("vector"))
        full_df_scaled = scaler_model.transform(df_with_vector)

        return full_df_scaled
    
    def hot_encode(self, df, to_binary_columns, id_columns):

        df_transformed = df.select(*id_columns)

        for c in to_binary_columns:
            df_cat = (df
                    .select(*id_columns, c)
                    .groupBy(*id_columns)
                    .pivot(c)
                    .agg(F.when(F.first(c).isNotNull(), 1).otherwise(0))
            )

            df_transformed = df_transformed.join(df_cat, on=id_columns)

        df_transformed = df_transformed.na.fill(0)

        return df_transformed
    
    def full_transformation(self, id_columns, binary_columns, scale_columns, to_binary_columns, full_df):

        #change scale columns to vector and scale
        df_with_vector = self.create_vector(full_df, scale_columns)
        full_df_scaled = self.scale_vector(df_with_vector)
        full_df_scaled = self.unpack_vector(full_df_scaled, id_columns, scale_columns, 'scaledFeatures')

        #one-hot-encode categorical columns
        full_df_cat = self.hot_encode(full_df, to_binary_columns, id_columns)
        full_df_cat = full_df_cat.drop('Blank')

        #add scaled columns, binary columns in one dataset
        ready_df = full_df.select(*id_columns, *binary_columns)
        ready_df = ready_df.join(full_df_cat, [*id_columns], how='outer')
        ready_df = ready_df.join(full_df_scaled, [*id_columns], how='outer')
        ready_df = ready_df.na.fill(0)

        #adjust column names
        ready_df = ready_df.select([F.col(x).alias(x.replace(' ', '_')) for x in ready_df.columns])
        ready_df = ready_df.select([F.col(x).alias(x.replace('[^a-zA-Z0-9]', '')) for x in ready_df.columns])
        ready_df = ready_df.select(*[x.lower() for x in ready_df.columns])

        #return
        return ready_df
    
    def create_index(self, control, nlist):

        d = control.shape[1]
        quantizer = IndexFlatL2(d)                   
        index = IndexIVFFlat(quantizer, d, nlist)
        index.train(control)  
        index.add(control)

        return index
    
    def search_index_test(self, index, control, k):

        #search the index for the first 5 members that were used to build it
        D, I = index.search(control[:5], k)

        #second return (I) is an array of matching indices
        #since the search was for the first 5 members, it should return the first 5 indices as best matches
        print('Matching Indices:')
        print(I)

        #first return (D) is an array of possible distances
        #since the search was for members that are part of the index already, it should return the best match at distance 0
        print('Distances to Matching Indices:')
        print(D)

        return 0

    def search_index(self, index, subset, k, n_probe):

        index.nprobe = n_probe        
        distances, neighbor_indexes = index.search(subset, k)            

        return distances, neighbor_indexes
  
    def pick_matches(self, distances, neighbor_indexes, subset, max_distance, num_matches):

        taken = []

        matched_record = pd.DataFrame(subset.index)
        matched_record.columns = ['subset_index']

        for c in range(1, num_matches+1):
            mtch_col_nm = 'control_index_'+str(c)
            matched_record[mtch_col_nm] = None

            #for each member of the exposed subset
            for current_index, row in matched_record.iterrows():
                i = 0
                    
                #record the closest match that has not already been claimed
                for idx in neighbor_indexes[current_index, :]:
                    if idx not in taken:
                        if distances[current_index,i] > max_distance:
                            matched_record.loc[current_index, mtch_col_nm] = None
                        else:
                            matched_record.loc[current_index, mtch_col_nm] = idx
                            taken.append(idx)
                        break

                    i = i+1

                #if no match was found, release all previous matches and remove row from matched_record
                if matched_record.loc[current_index, mtch_col_nm] == None:
                    for x in range(1,c+1):
                        x_col_nm = 'control_index_'+str(x)
                        val = matched_record.loc[current_index, x_col_nm]

                        if val is not None:
                            taken.remove(val)
                            matched_record.loc[current_index, x_col_nm] = None     

                    matched_record.drop([current_index], inplace=True)                             
        
        return matched_record
    
    def tag_matches(self, matches, control, exposed, num_matches):

        control_matched = pd.DataFrame()
        for c in range(1, num_matches+1):
            mtch_col_nm = 'control_index_'+str(c)

            final_matched = matches[~matches[mtch_col_nm].isnull()].reset_index(drop=True)
            exposed_matched = exposed.loc[final_matched.subset_index].reset_index().rename(columns={'index':'match_index'})

            c_matched = control.loc[final_matched[mtch_col_nm]].reset_index()
            c_matched = pd.concat([c_matched, final_matched['subset_index']], axis=1).rename(columns={'subset_index':'match', 'index':'match_index'})

            control_matched = pd.concat([control_matched, c_matched])

        return exposed_matched, control_matched
    
    def index_details(self, index_df, bridge_df, detail_df, id_columns, match_col):

        df = index_df.withColumn('match_index', F.col(match_col)).join(bridge_df, on='match_index', how='inner')
        df = df.join(detail_df, on=[*id_columns], how='inner')

        return df
    
    def detail_matches(self, spark_c, matched_record, exposed_matched, control_matched, full_df, final_columns, id_columns, num_final_matches):

        exposed = matched_record.select('subset_index')
        control = matched_record.select(list(set(matched_record.columns) - set(['subset_index'])))

        exposed_deets = self.index_details(matched_record, exposed_matched, full_df[final_columns], id_columns, 'subset_index')
        control_deets =  spark_c.createDataFrame(spark_c.sparkContext.emptyRDD(), exposed_deets.schema)

        for c in range(1, num_final_matches+1):
            col_nm = 'control_index_'+str(c)
            deets = self.index_details(matched_record, control_matched, full_df[final_columns], id_columns, col_nm)
            control_deets = control_deets.unionByName(deets, allowMissingColumns=True)

        exposed_deets = exposed_deets.withColumnRenamed('match_index', 'match_key')
        control_deets = control_deets.withColumnRenamed('match', 'match_key')

        exposed_deets = exposed_deets[['match_key']+final_columns].orderBy('subset_index')
        control_deets = control_deets[['match_key']+final_columns].orderBy('subset_index')

        final_matched = exposed_deets.union(control_deets)

        return final_matched
    
    def sample_matches(self, final_matched, num_sample):

        exposed_df = final_matched.filter(F.col('category') != 'control')
        control_df = final_matched.filter(F.col('category') == 'control')

        sample_exposed = exposed_df.withColumn('key',F.rand()).orderBy('key').limit(num_sample).orderBy('match_key').drop('key')

        sample_list = sample_exposed.toPandas()
        sample_list = sample_list['match_key'].to_list()

        sample_control = control_df.filter(F.col('match_key').isin(sample_list))

        return sample_exposed, sample_control
    











