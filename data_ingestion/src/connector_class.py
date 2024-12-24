import pandas as pd
import sqlalchemy as sql
import os as os
from dotenv import load_dotenv
load_dotenv()

class SQLConnector:
    def __init__(self, server: str ='edw'):
        self.port = 5439
        if server.lower() in ['edw', 'cg', 'ms', 'emo']:
            self.host = os.environ[f'{server.upper()}_HOST']
            self.user = os.environ[f'{server.upper()}_USER']
            self.db_name = os.environ[f'{server.upper()}_DB']
            self.password = os.environ[f'{server.upper()}_PASSWORD']
            self.engine = sql.create_engine(url="postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"\
                .format(user=self.user, host=self.host, pw=self.password, port=self.port, db=self.db_name))
            self.connection = self.engine.connect()

        else:
            KeyError("Such database does not exist")
    
    def query_data(self, query):

        try:
            results = pd.read_sql(query, self.connection.connection)
            return results
        except TypeError as e:
            print("Error", e)
            return None

    def create_table(self, query):
        query = sql.text(query)

        try:
            self.connection.execute(query)
        except TypeError as e:
            print("Error", e)
            return None
        
    def dispose(self):
        self.engine.dispose()