import pandas as pd

class CG_Helper():

    def __init__(self):
        """
        no variables to initialize yet 
        """

    def map_customers(self, customer_df, column_nm):
                                        
        mapping = {"7-ELEVEN, INC.": "SEVEN ELEVEN",
                    "ABA EMPLOYEE SERVICES": "LIGHTHOUSE AUTISM CENTER",
                    "ADVANTAGE SALES AND MARKETING": "ADVANTAGE SOLUTIONS",
                    "AERA ENERGY SERVICES COMPANY": "AERA ENERGY",
                    "API GROUP, INC": "API GROUP",
                    "CALIFORNIA HIGHWAY PATROL": "CALIFORNIA ASSOCIATION OF HIGHWAY PATROLMEN",
                    "CENTERWELL": "CENTERWELL HOME HEALTH",
                    "COREWELL": "COREWELL HEALTH",
                    "EMD": "BENEFITS4ME",
                    "ENERCON SERVICES, INC.": "ENERCON",
                    "ENLYTE (FORMERLY MITCHELL-GENEX SERVICES)": "ENLYTE",
                    "FIRST AMERICAN FINANCIAL CORP": "FIRST AMERICAN FINANCIAL",
                    "FULL BLOOM": "CATAPULT",
                    "GENERAL MILLS, INC.": "GENERAL MILLS",
                    "GREIF PACKAGING": "GREIF INC",
                    "INTUIT INC.": "INTUIT",
                    "L3HARRISTECHNOLOGIESINC": "L3HARRIS",
                    "LAND O'LAKES, INC.": "LAND O'LAKES",
                    "LANDRY'S AND GOLDEN NUGGET": "LANDRYS INC",
                    "LASSONDE PAPPAS": "LASSONDE PAPPAS AND COMPANY",
                    "LOWES": "LOWE'S",
                    "NEW ARCLIN U.S. HOLDING CORP.": "ARCLIN",
                    "PEPSI-COLA AND NATIONAL BRAND BEVERAGES": "HONICKMAN",
                    "PRISM/CITY OF REDDING": "CITY OF REDDING",
                    "PRISM/COUNTY OF SANTA BARBARA": "COUNTY OF SANTA BARBARA",
                    "RACETRAC PETROLEUM, INC.": "RACETRAC",
                    "RED RIVER TECHNOLOGY LLC": "RED RIVER TECHNOLOGY",
                    "SA RECYCLING, LLC": "SA RECYCLING",
                    "TEMPLE UNIVERSITY HEALTH SYSTEM": "TEMPLE",
                    "TUTOR PERINI CORPORATION": "TUTOR PERINI",
                    "UGI": "AMERIGAS",
                    "UNISYS CORPORATION": "UNISYS"
                }
        
        customer_df['edw_cust']  = customer_df[column_nm].str.upper().map(mapping).fillna(customer_df[column_nm].str.upper())

        return customer_df
                
    def map_customers_schema(self, customer_df, schema_df):
               
        customer_df = self.map_customers(customer_df, 'cg_cust')
        customer_df = customer_df.merge(schema_df, on='acronym')
        
        #remove the extra lines
        customer_df = customer_df[~customer_df['edw_cust'].isin(['BK', 'NON-ACCOLADE', 'ACCOLADE', 'BLANK'])]
        
        #mark the schemas with multiple customers
        schema_cust = customer_df.groupby('table_schema')['edw_cust'].nunique().reset_index().rename(columns={'edw_cust':'num_cust'})
        customer_df = customer_df.merge(schema_cust, on='table_schema')
        
        #return
        return customer_df[['table_schema', 'acronym', 'edw_cust', 'cg_cust', 'num_cust']]
    
    def map_customers_member(self, member_df, customer_df):
        
        member_df = self.map_customers(member_df, 'dw_customer_nm')
        cdf = customer_df[customer_df['num_cust']==1]
        
        member_df = member_df.merge(cdf, on='table_schema', how='left')
        member_df.loc[member_df['edw_cust_x'].isin(['BK', 'NON-ACCOLADE', 'ACCOLADE', 'BLANK']), 'edw_cust_x'] = None
        member_df['edw_cust'] = member_df['edw_cust_x'].fillna(member_df['edw_cust_y'])
        
        member_df = member_df.drop(columns = ['edw_cust_x', 'edw_cust_y', 'table_schema', 'acronym', 'cg_cust', 'num_cust', 'dw_customer_nm'])
                
        return member_df
    
    def map_industry(self, member_df):
        
        c_df = pd.read_csv('data_ingestion/src_data/Industry_Map.csv')
        member_df = pd.merge(member_df, c_df[['edw_cust', 'industry']], on ='edw_cust', how='left')
        member_df['industry'] = member_df['industry'].fillna('unknown')
        
        return member_df
    
    def map_state(self, member_df):

        mapping = {"Alabama": "AL",
                    "Alaska": "AK",
                    "Arizona": "AZ",
                    "Arkansas": "AR",
                    "American Samoa": "AS",
                    "California": "CA",
                    "Colorado": "CO",
                    "Connecticut": "CT",
                    "Delaware": "DE",
                    "District of Columbia": "DC",
                    "Florida": "FL",
                    "Georgia": "GA",
                    "Guam": "GU",
                    "Hawaii": "HI",
                    "Idaho": "ID",
                    "Illinois": "IL",
                    "Indiana": "IN",
                    "Iowa": "IA",
                    "Kansas": "KS",
                    "Kentucky": "KY",
                    "Louisiana": "LA",
                    "Maine": "ME",
                    "Maryland": "MD",
                    "Massachusetts": "MA",
                    "Michigan": "MI",
                    "Minnesota": "MN",
                    "Mississippi": "MS",
                    "Missouri": "MO",
                    "Montana": "MT",
                    "Nebraska": "NE",
                    "Nevada": "NV",
                    "New Hampshire": "NH",
                    "New Jersey": "NJ",
                    "New Mexico": "NM",
                    "New York": "NY",
                    "North Carolina": "NC",
                    "North Dakota": "ND",
                    "Northern Mariana Islands": "MP",
                    "Ohio": "OH",
                    "Oklahoma": "OK",
                    "Oregon": "OR",
                    "Pennsylvania": "PA",
                    "Puerto Rico": "PR",
                    "Rhode Island": "RI",
                    "South Carolina": "SC",
                    "South Dakota": "SD",
                    "Tennessee": "TN",
                    "Texas": "TX",
                    "Trust Territories": "TT",
                    "Utah": "UT",
                    "Vermont": "VT",
                    "Virginia": "VA",
                    "Virgin Islands": "VI",
                    "Washington": "WA",
                    "West Virginia": "WV",
                    "Wisconsin": "WI",
                    "Wyoming": "WY"
                }
        
        map_col = F.create_map([F.lit(x) for i in mapping.items() for x in i])
        customer_df.withColumn('state_abv', map_col[F.col('state')])

        return customer_df

        