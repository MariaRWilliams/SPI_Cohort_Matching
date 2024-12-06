import pandas as pd
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder
import pyspark.sql.functions as F
import pyspark.sql.types as T
from faiss import IndexFlatL2, IndexIVFFlat

class Cohort_Matching():

    def __init__(self):

        #dynamic lists of columns used for matching, etc.
        id_columns = []
        scale_columns = []
        binary_columns = []
        to_binary_columns = []
        final_columns = []
        weights = {}

        #model specifications
        num_possible_matches = 10
        num_final_matches = 1
        n_list = 5
        n_probe = 5
        max_distance = 5

    def create_vector(self, df):

        assembler = VectorAssembler().setInputCols(self.scale_columns).setOutputCol("vector")
        df_with_vector = assembler.transform(df)

        return df_with_vector
    
    def unpack_vector(self, df, vector_col):

        def split_array_to_list(col):
            def to_list(v):
                return v.toArray().tolist()
            return F.udf(to_list, T.ArrayType(T.DoubleType()))(col)

        df_split = df.withColumn("split_int", split_array_to_list(F.col(vector_col)))
        df_split = df_split.select(*self.id_columns, *[F.col("split_int")[i].alias(self.scale_columns[i]) for i in range(len(self.scale_columns))])

        return df_split
    
    def scale_vector(self, df_with_vector):

        scaler = StandardScaler(inputCol="vector", outputCol="scaledFeatures")
        scaler_model = scaler.fit(df_with_vector.select("vector"))
        full_df_scaled = scaler_model.transform(df_with_vector)

        return full_df_scaled
    
    def hot_encode(self, df):

        df_transformed = df.select(*self.id_columns)

        for c in self.to_binary_columns:
            df = df.withColumn(c, F.regexp_replace(c, '[^a-zA-Z0-9]', ''))
            df = df.withColumn(c, F.regexp_replace(c, ' ', '_'))
            df = df.withColumn(c, F.lower(F.col(c)))

            df_cat = (df
                    .select(*self.id_columns, c)
                    .groupBy(*self.id_columns)
                    .pivot(c)
                    .agg(F.when(F.first(c).isNotNull(), 1).otherwise(0))
            )

            df_transformed = df_transformed.join(df_cat, on=self.id_columns)

        df_transformed = df_transformed.na.fill(0)

        return df_transformed
    
    def full_transformation(self, full_df):

        #change scale columns to vector and scale
        df_with_vector = self.create_vector(full_df)
        full_df_scaled = self.scale_vector(df_with_vector)
        full_df_scaled = self.unpack_vector(full_df_scaled, 'scaledFeatures')

        #one-hot-encode categorical columns
        full_df_cat = self.hot_encode(full_df)
        full_df_cat = full_df_cat.drop('Blank')

        #add scaled columns, binary columns in one dataset
        ready_df = full_df.select(*self.id_columns, *self.binary_columns)
        ready_df = ready_df.join(full_df_cat, [*self.id_columns], how='outer')
        ready_df = ready_df.join(full_df_scaled, [*self.id_columns], how='outer')
        ready_df = ready_df.na.fill(0)

        #adjust column names
        # ready_df = ready_df.select([F.col(x).alias(x.replace(' ', '_')) for x in ready_df.columns])
        # ready_df = ready_df.select([F.col(x).alias(x.replace('[^a-zA-Z0-9]', '')) for x in ready_df.columns])
        # ready_df = ready_df.select(*[x.lower() for x in ready_df.columns])

        #weight variables
        for col in self.weights:
            ready_df = ready_df.withColumn(col, self.weights[col] * ready_df[col])

        #return
        return ready_df.distinct()
    
    def create_index(self, control):

        d = control.shape[1]
        quantizer = IndexFlatL2(d)                   
        index = IndexIVFFlat(quantizer, d, self.n_list)
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

    def search_index(self, index, subset):

        index.nprobe = self.n_probe        
        distances, neighbor_indexes = index.search(subset, self.num_possible_matches)            

        return distances, neighbor_indexes
  
    def pick_matches(self, distances, neighbor_indexes, subset):

        taken = []

        matched_record = pd.DataFrame(subset.index)
        matched_record.columns = ['subset_index']

        for c in range(1, self.num_final_matches+1):
            mtch_col_nm = 'control_index_'+str(c)
            matched_record[mtch_col_nm] = None

            #for each member of the exposed subset
            for current_index, row in matched_record.iterrows():
                i = 0
                    
                #record the closest match that has not already been claimed
                for idx in neighbor_indexes[current_index, :]:
                    if idx not in taken:
                        if distances[current_index,i] > self.max_distance:
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
    
    def tag_matches(self, matches, control, exposed):

        control_matched = pd.DataFrame()
        for c in range(1, self.num_final_matches+1):
            mtch_col_nm = 'control_index_'+str(c)

            final_matched = matches[~matches[mtch_col_nm].isnull()].reset_index(drop=True)

            exposed_matched = exposed.loc[final_matched.subset_index].reset_index().rename(columns={'index':'match_index'})
            c_matched = control.loc[final_matched[mtch_col_nm]].reset_index()
            c_matched = pd.concat([c_matched, final_matched['subset_index']], axis=1).rename(columns={'subset_index':'match', 'index':'match_index'})

            c_matched = control.loc[final_matched[mtch_col_nm]].reset_index()
            c_matched = pd.concat([c_matched, final_matched['subset_index']], axis=1).rename(columns={'subset_index':'match', 'index':'match_index'})

            control_matched = pd.concat([control_matched, c_matched])

        return exposed_matched, control_matched
    
    def index_details(self, index_df, bridge_df, detail_df, match_col):

        df = index_df.withColumn('match_index', F.col(match_col)).join(bridge_df, on='match_index', how='inner')
        df = df.join(detail_df, on=[*self.id_columns], how='inner')

        return df
    
    def detail_matches(self, spark_c, matched_record, exposed_matched, control_matched, full_df, prefix):

        exposed = matched_record.select('subset_index')
        control = matched_record.select(list(set(matched_record.columns) - set(['subset_index'])))

        exposed_deets = self.index_details(matched_record, exposed_matched, full_df[self.final_columns], 'subset_index')
        control_deets =  spark_c.createDataFrame(spark_c.sparkContext.emptyRDD(), exposed_deets.schema)

        for c in range(1, self.num_final_matches+1):
            col_nm = 'control_index_'+str(c)
            deets = self.index_details(matched_record, control_matched, full_df[self.final_columns], col_nm)
            control_deets = control_deets.unionByName(deets, allowMissingColumns=True)

        exposed_deets = exposed_deets.withColumnRenamed('match_index', 'match_key')
        control_deets = control_deets.withColumnRenamed('match', 'match_key')

        control_deets = control_deets.withColumn('category', F.concat(F.lit(exposed_matched.select('category').first()[0]), F.lit(' '), control_deets.category))

        exposed_deets = exposed_deets[['match_key']+self.final_columns].orderBy('subset_index')
        control_deets = control_deets[['match_key']+self.final_columns].orderBy('subset_index')

        final_matched = exposed_deets.union(control_deets)
        final_matched = final_matched.withColumn('match_key', F.concat(F.lit(prefix), F.lit('_'), final_matched.match_key))

        return final_matched

    def main_match(self, spark, cohort, full_df, ready_df):

        print('Collecting processing details...')
        #prep to loop through demographic combos
        fixed_cols = list(set(ready_df.columns) - set(self.id_columns) - set(self.scale_columns))

        exposed = ready_df.filter(ready_df['category']==cohort)
        control = ready_df.filter(ready_df['category']=='control')

        ex_agg = exposed.groupby(*fixed_cols).agg(F.count('person_id').alias('exposed_count'))
        c_agg = control.groupby(*fixed_cols).agg(F.count('person_id').alias('control_count'))
        
        demo_combos_full = ex_agg.join(c_agg, on=fixed_cols, how='left')
        # disc_demos = demo_combos_full.filter(F.col('control_count') <= self.n_list*40)
        #demo_combos = demo_combos_full.filter(F.col('control_count') > self.n_list*40)
        demo_combos = demo_combos_full.filter(F.col('control_count') > self.n_list)

        dc_num = len(demo_combos.collect())
        counter = 1

        for row in demo_combos.rdd.toLocalIterator():
        #for row in demo_combos.limit(1).rdd.toLocalIterator():
            print('Processing '+cohort+' '+str(counter)+' of '+str(dc_num))

            this_control = control
            this_exposed = exposed

            this_control = this_control.join(spark.createDataFrame([row]), on=fixed_cols, how='leftsemi')
            this_exposed = this_exposed.join(spark.createDataFrame([row]), on=fixed_cols, how='leftsemi')
            
            print('datasets prepared')

            #make index
            this_control = this_control.select(*self.id_columns, *self.scale_columns).toPandas()
            control_ids = this_control[self.id_columns]
            control_vars = this_control[self.scale_columns]
            index = self.create_index(control_vars)

            print('index created')

            #search index
            this_exposed = this_exposed.select(*self.id_columns, *self.scale_columns).toPandas()
            exp_ids = this_exposed[self.id_columns]
            exp_vars = this_exposed[self.scale_columns]
            distances, neighbor_indexes = self.search_index(index, exp_vars)

            print('search complete')

            #collect matches
            matched_record = self.pick_matches(distances, neighbor_indexes, exp_vars)
            exposed_matched, control_matched = self.tag_matches(matched_record, control_ids, exp_ids)
            

            if matched_record.empty:
                print('no suitable matches found')
                counter = counter+1
            else:

                #switch back to spark
                matched_record = spark.createDataFrame(matched_record)
                exposed_matched = spark.createDataFrame(exposed_matched)
                control_matched = spark.createDataFrame(control_matched)

                #detail matches
                matched_details = self.detail_matches(spark, matched_record, exposed_matched, control_matched, full_df, counter)

                print('cohorts defined')

                #end matching for this combo
                try:
                    final_matched
                except NameError:
                    final_matched = matched_details
                else:
                    final_matched = final_matched.union(matched_details)
                
                counter = counter+1

        print(cohort + ' Matching Complete')
        return final_matched.distinct(), demo_combos_full






