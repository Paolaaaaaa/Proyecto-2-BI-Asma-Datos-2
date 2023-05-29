
import pyodbc
import pandas as pd 
import cs
from sqlalchemy import create_engine
import os 

pwd = "password"
uid = "etl"

driver = "{SQL Server }"
server="UNE-PC02"
database = "Bi_ETL"

def extract():
    try:
        src_conn = pyodbc.connect('Driver={SQL Server};' + 
                                  'SERVER=' + server + '\SQLEXPRESS' + 
                                  ';DATABASE=' + database + 
                                  ';Trusted_Connection=yes;')
        # execute query
       
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute(""" select  t.name as table_name
        from sys.tables t where t.name in ('Datos_2017_asma') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            #query and load save data to dataframe
            df_2017_asma = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn) 

        src_cursor.execute(""" select  t.name as table_name
        from sys.tables t where t.name in ('Datos_2021_asma') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            #query and load save data to dataframe
            df_2021_asma = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)        
            print (df_2021_asma)


            
        #load(df, "dbo.Datos_2021_sin_asma")
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()





def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/'+database)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))