import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
#from faiss import IndexFlatL2, IndexIVFFlat

class Cohort_Matching():

    def __init__(self):
        """
        no variables to initialize yet 
        """

    def unpack_vector(self, df, unchanged_columns, scale_columns, vector_col):

        def split_array_to_list(col):
            def to_list(v):
                return v.toArray().tolist()
            return F.udf(to_list, T.ArrayType(T.DoubleType()))(col)

        df_split = df.withColumn("split_int", split_array_to_list(F.col(vector_col)))
        df_split = df_split.select(*unchanged_columns, *[F.col("split_int")[i].alias(scale_columns[i]) for i in range(len(scale_columns))])

        return df_split
        
    #original matching model
    def FAISS(self, control, intervention):
                            
        # build the index
        nlist = 50
        d = control.shape[1]
        quantizer = IndexFlatL2(d)                       
        index = IndexIVFFlat(quantizer, d, nlist)
        #print(index.is_trained)
        assert not index.is_trained
        index.train(control)                  # add vectors to the index
        assert index.is_trained
        index.add(control)
        #print(index.is_trained)
        #print(index.ntotal)
        print('Matching index built')
        #print(intervention[:5])

        # searching
        k = 10
        index.nprobe = 10
        
        #distances, neighbor_indexes = index.search(intervention[:5], k)     # sanity check           
        #print(neighbor_indexes)
        #print(distances)
        
        distances, neighbor_indexes = index.search(intervention, k)            
        #print(neighbor_indexes[:5])
        #print(neighbor_indexes[-5:])
        print ('nearest neighbor distances determined')

        tracking_df = pd.DataFrame(intervention.index)
        tracking_df.columns = ['int_index']
        tracking_df['matched'] = 0

        matched_control = []
        percent_complete_threshhold = [.1,.25,.5,.75,.9,1.01]
        pct_ind = 0
        rows = tracking_df.shape[0]
        r = 0
        for current_index, row in tracking_df.iterrows():  # iterate over the dataframe
            #used to track progress
            r +=1
            if (r/rows>percent_complete_threshhold[pct_ind]):
                print('Matching is '+ str(percent_complete_threshhold[pct_ind]*100).zfill(0) +
                    '% complete')
                pct_ind +=1
            # check distances before checking to see if index has been matched or not
            if distances[current_index,0] > 50:
                tracking_df.loc[current_index, 'matched'] = 0 # don't match
            else:
                for idx in neighbor_indexes[current_index, :]:
                    if idx not in matched_control:                       # this control has not been matched yet
                        tracking_df.loc[current_index, 'matched'] = idx  # record the matching
                        matched_control.append(idx)                      # add the matched to the list
                        break
                    
        return tracking_df, matched_control