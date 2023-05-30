
import pyodbc
import pandas as pd 
import cs
from sqlalchemy import create_engine
import os 
import transformation as ts
import Tables 
from sqlalchemy.orm import Session

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Sequence
from sqlalchemy.sql import text

Base = declarative_base()

class Ubicacion(Base):
    __tablename__ = 'ubicacion'
    pk_ubicacion = Column(Integer, Sequence('ubicacion_seq'),primary_key=True )
    localidad = Column(String)
    municipio = Column(Integer)

class Fecha(Base):
    __tablename__ = 'fecha'
    
    pk_fecha = Column(Integer, Sequence('fecha_seq'),primary_key=True )
    anio = Column(Integer)


class Encuestado(Base):
    __tablename__ = 'encuestado'
    
    pk_encuestado = Column(Integer, Sequence('encuestado_seq'),primary_key=True )
    edad = Column(Integer)
    sexo = Column(Integer)


class Pregunta(Base):
    __tablename__ = 'pregunta'
    pk_pregunta = Column(Integer, Sequence('pregunta_seq'),primary_key=True )
    pregunta = Column(String)

class Respuesta (Base):
    __tablename__ = 'respuesta'
    
    pk_respuesta = Column(Integer, Sequence('respuesta_seq'),primary_key=True )
    respuesta = Column(String)

class hecho_respuesta (Base):
    __tablename__ = 'hecho_respuesta'

    id = Column(Integer, primary_key=True)
    pk_fecha = Column(Integer, ForeignKey('fecha.pk_fecha'))
    pk_ubicacion = Column(Integer, ForeignKey('ubicacion.pk_ubicacion'))
    pk_encuestado = Column (Integer, ForeignKey('encuestado.pk_encuestado'))
    pk_pregunta = Column(Integer, ForeignKey('pregunta.pk_pregunta'))
    pk_respuesta = Column(Integer,ForeignKey('respuesta.pk_respuesta'))

    numero_personas = Column(Integer)




def create_ubicacion(df, engine):
    session = Session(bind=engine)

    localidades=df['"localidad"'].unique()
    municipio=df['"municipio"'].unique()

    for i in localidades:
        for j in municipio:
            nuevo_dato=Ubicacion(localidad= i, municipio = j)
            session.add(nuevo_dato)
    session.commit()

    session.close()



def create_fecha(df, engine):
    session = Session(bind=engine)

    localidades=df['"ANIO"'].unique()

    for i in localidades:
            nuevo_dato=Fecha(anio= i)
            session.add(nuevo_dato)
    session.commit()

    session.close()


def create_encuestado(df, engine):
    session = Session(bind=engine)
    edades=df['"edad"'].unique()
    sexos=df['"sexo"'].unique()

    for i in edades:
        for j in sexos:
            nuevo_dato=Encuestado(edad= i, sexo = j)
            session.add(nuevo_dato)
    session.commit()

    session.close()



def create_pregunta(engine):
    session = Session(bind=engine)

    pregunta = ['"Transmilenio"',
       '"SITP"', '"buseta_o_colectivo"', '"automovil_particular"', '"Taxi"',
       '"Motocicleta"', '"Bicicleta"', 'Ruta_escolar"', '"A_pie"',
       'bicitaxi_o_mototaxi', 'caballo', 'otros', 'ANIO', 'no_se_desplaza',
       '"vehículo_patineta_moto_electicos"',
       '"particulares_plataformas_o_aplicaciones"', '"Bus_intermunicipal"']
    for i in pregunta:
            nuevo_dato=Pregunta(pregunta = i)
            session.add(nuevo_dato)
    session.commit()

    session.close()

def create_respuesta(engine):
    session = Session(bind=engine)

    
    for i in ["1","2","NAN"]:
            nuevo_dato=Respuesta(respuesta= i)
            session.add(nuevo_dato)
    session.commit()

    session.close()


def create_respuesta_h (df, engine):
    pregunta = ['"Transmilenio"',
       '"SITP"', '"buseta_o_colectivo"', '"automovil_particular"', '"Taxi"',
       '"Motocicleta"', '"Bicicleta"', 'Ruta_escolar"', '"A_pie"',
       'bicitaxi_o_mototaxi', 'caballo', 'otros', 'ANIO', 'no_se_desplaza',
       '"vehículo_patineta_moto_electicos"',
       '"particulares_plataformas_o_aplicaciones"', '"Bus_intermunicipal"']
    session = Session(bind=engine)
    for index, row in df.iterrows():
        for i in pregunta:
            fecha = row['"ANIO"']
            edad = row['"edad"']
            sexo = row['"sexo"']
            localidad = row['"localidad"']
            municipio = row['"municipio"']
            respuesta = row[i]
            pkubicacion = pk_ubicacion(engine,localidad,municipio )
            pkfecha = pk_fecha(engine,fecha)
            pkencuestado = pk_encuestado(engine, edad,sexo)
            if i is not None and  respuesta is not None:
                pkpregunta = pk_pregunta(engine, i)
                pkrespuesta = pk_respuesta(engine, respuesta)

                exists = find_pairs(engine,pkfecha[0],pkubicacion[0],pkencuestado[0],pkpregunta[0],pkrespuesta[0])
            else:
                exists = find_pairs(engine,pkfecha[0],pkubicacion[0],pkencuestado[0],3,3)


                if (exists is None):
                    nuevo_dato = hecho_respuesta(pk_fecha= pkfecha[0], pk_ubicacion = pkubicacion[0], pk_encuestado= pkencuestado[0], pk_respuesta = pkrespuesta[0], pk_pregunta = pkpregunta[0], numero_personas = 1)
                    session.add(nuevo_dato)
                else:
                    update_count_people(engine,id=exists)
            session.commit()

            session.close()









def pk_ubicacion(engine, localidad, municipio):
    print ("Buscando ubicación")
    localidad = localidad.replace('"', '\'')
    municipio = municipio.replace('"', '\'')

    query = text("SELECT pk_ubicacion FROM ubicacion WHERE localidad = "+str(localidad)+" and municipio = "+str(municipio))
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk

def pk_fecha(engine, anio):
    print ("Buscando fecha")

    anio = anio.replace('"', '\'')
    query = text("SELECT pk_fecha FROM fecha WHERE anio = "+str(anio))
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk

def pk_encuestado(engine,edad, sexo) :
    print ("Buscando encuestado")

    edad = edad.replace('"', '\'')
    sexo = sexo.replace('"', '\'')

    query = text("SELECT pk_encuestado FROM encuestado WHERE edad = "+str(edad)+" and sexo ="+str(sexo))
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk


def pk_pregunta(engine, pregunta):
    print ("Buscando pregunta")

    pregunta = pregunta.replace('"', '\'')
    pregunta = pregunta.strip('"')

    query = text("SELECT pk_pregunta FROM pregunta WHERE pregunta = "+str(pregunta))
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk

def pk_respuesta(engine,respuesta) :
    print ("Buscando respuesta")
    if respuesta is None:
        pk = 3
    else:
        respuesta = respuesta.replace('"', '\'')

        query = text("SELECT pk_respuesta FROM respuesta WHERE respuesta = '"+str(respuesta)+"'")
        conn = engine.connect()
        pk = conn.execute(query).fetchone()
        conn.close()
    return  pk

def find_pairs(engine, pk_fecha ,  pk_ubicacion ,pk_encuestado, pk_pregunta, pk_respuesta):
    print ("Buscando pk")

    query = text("SELECT id FROM hecho_respuesta WHERE pk_fecha = "+str(pk_fecha)+ " and pk_ubicacion =  "+str(pk_ubicacion)+ " and pk_encuestado= "+str(pk_encuestado)+" and pk_pregunta= "+str(pk_pregunta)+" and pk_respuesta ="+str(pk_respuesta))
    conn = engine.connect()
    print ("hu")
    pk = conn.execute(query).fetchone()
    print (pk)
    conn.close()
    return  pk

def update_count_people (engine, id):
    print ("Update pepople")

    query = text("UPDATE hecho_respuesta WHERE id = "+str(id)+ "values numero_personas = numero_personas + 1 ")
    conn = engine.connect(query)
    conn.close()


rows_imported = 0

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



        load(concat_data)
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()





def load(df):
    try:
        engine = create_engine(f'postgresql://{uid}:{pwd}@localhost:5432/'+database)

        with engine.connect() as connection:
            print("¡Conexión exitosa!")

            Base.metadata.create_all(engine)

            print ("Tablas creadas")
            create_ubicacion(df, engine)

            create_fecha(df,engine)
            create_encuestado(df, engine)
            create_respuesta( engine)
            create_pregunta( engine)
            #create_respuesta_h(df, engine)

        # La conexión se estableció correctament

        
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))









