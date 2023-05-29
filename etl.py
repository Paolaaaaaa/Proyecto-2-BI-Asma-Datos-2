
import pyodbc
import pandas as pd 
import cs
from sqlalchemy import create_engine
import os 
import transformation as ts
import Tables 
from sqlalchemy.orm import Session

encuestado = {}
ubicacion = {}
fecha = {}
pregunta = {}
respuesta = {}

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

        print ("\n Comenzando la transformación de DATOS: \n ")
        df_2021=ts.transform_data2021(df_2021_asma)
        print ("2021 ha sido transformada \n")
        df_2017=ts.transform_data2017(df_2017_asma)
        print ("df_2017 ha sido transformada \n ")
  

        concat_data = ts.join_df(df_2017,df_2021)
        print (concat_data)



        #load(df, "dbo.Datos_2021_sin_asma")
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()





def load(df):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/'+database)
        Tables.Base.metadata.create_all(engine)
        create_ubicacion(df, engine)
        create_fecha(df,engine)
        create_encuestado(df, engine)
        create_respuesta(df, engine)
        create_pregunta(df, engine)
    

        
        
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))



def create_ubicacion(df, engine):
    session = Session(bind=engine)

    localidades=df['localidad'].unique()
    municipio=df['municipio'].unique()

    for i in localidades:
        for j in municipio:
            nuevo_dato=Tables.Ubicacion(localidad= i, municipio = j)
            session.add(nuevo_dato)
    session.commit()

    session.close()



def create_fecha(df, engine):
    session = Session(bind=engine)

    localidades=df['ANIO'].unique()

    for i in localidades:
            nuevo_dato=Tables.Fecha(anio= i)
            session.add(nuevo_dato)
    session.commit()

    session.close()


def create_encuestado(df, engine):
    session = Session(bind=engine)
    edades=df['edad'].unique()
    sexos=df['sexo'].unique()

    for i in edades:
        for j in sexos:
            nuevo_dato=Tables.Encuestado(edad= i, sexo = j)
            session.add(nuevo_dato)
    session.commit()

    session.close()



def create_pregunta(engine):
    session = Session(bind=engine)

    pregunta = ['Transmilenio',
       'SITP', 'buseta_o_colectivo', 'automovil_particular', 'Taxi',
       'Motocicleta', 'Bicicleta', 'Ruta_escolar', 'A_pie', 'NPCHP18J',
       'bicitaxi_o_mototaxi', 'caballo', 'otros', 'ANIO', 'no_se_desplaza',
       'vehículo_patineta_moto_electicos',
       'particulares_plataformas_o_aplicaciones', 'Bus_intermunicipal']
    for i in pregunta:
            nuevo_dato=Tables.Pregunta(pregunta = i)
            session.add(nuevo_dato)
    session.commit()

    session.close()

def create_respuesta(engine):
    session = Session(bind=engine)

    
    for i in ["1","2","NAN"]:
            nuevo_dato=Tables.Respuesta(respuesta= i)
            session.add(nuevo_dato)
    session.commit()

    session.close()