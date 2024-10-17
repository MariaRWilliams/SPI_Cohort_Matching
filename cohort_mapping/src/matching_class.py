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

        df_cat = (df
                .select(*to_binary_columns, *id_columns)
                .groupBy(*id_columns)
                .pivot(*to_binary_columns)
                .agg(F.when(F.first(*to_binary_columns).isNotNull(), 1).otherwise(0))
        )

        df_cat = df_cat.na.fill(0)

        return df_cat
    
    def create_index(self, control, nlist):

        d = control.shape[1]
        quantizer = IndexFlatL2(d)                   
        index = IndexIVFFlat(quantizer, d, nlist)
        index.train(control)  
        index.add(control)

        return(index)
    
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

    def pick_matches(self, distances, neighbor_indexes, subset, max_distance):

        matched_record = pd.DataFrame(subset.index)
        matched_record.columns = ['subset_index']
        matched_record['control_index'] = 0

        taken = []

        #for each member of the exposed subset
        for current_index, row in matched_record.iterrows():

            #if the closest match is not within an acceptable distance, skip it
            if distances[current_index,0] > max_distance:
                matched_record.loc[current_index, 'control_index'] = 0
                
            #otherwise, record the closest match that has not already been claimed
            else:
                for idx in neighbor_indexes[current_index, :]:
                    if idx not in taken:
                        matched_record.loc[current_index, 'control_index'] = idx
                        taken.append(idx)
                        break
                    
        return matched_record
    
    def tag_matches(self, tracking_df, control, exposed):

        final_matched = tracking_df[tracking_df['control_index']!=0]
        control_matched = control.loc[final_matched.control_index]
        control_matched = pd.concat([control_matched, final_matched['subset_index']])
        # control_matched['match'] = final_matched.subset_index
        # # exposed_matched = exposed.loc[final_matched.subset_index]

        # #return control_matched, exposed_matched
        return control_matched











