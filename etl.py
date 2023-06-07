
import psycopg2
import pyodbc
import pandas as pd 
import cs
from sqlalchemy import create_engine
import os 
import transformation as ts
import Tables 
from sqlalchemy.orm import Session
import math
from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import exc
from sqlalchemy import Column, Integer, String, Sequence, UniqueConstraint

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
    __table_args__ = (
        UniqueConstraint('localidad', 'municipio', name='uq_localidad_municipio'),
    )

class Fecha(Base):
    __tablename__ = 'fecha'
    
    pk_fecha = Column(Integer, Sequence('fecha_seq'),primary_key=True )
    anio = Column(Integer)
    __table_args__ = (
        UniqueConstraint('anio', name='uq_unio'),
    )


class Encuestado(Base):
    __tablename__ = 'encuestado'
    
    pk_encuestado = Column(Integer, Sequence('encuestado_seq'),primary_key=True )
    edad = Column(Integer)
    sexo = Column(Integer)
    id = Column(Integer)
    __table_args__ = (
        UniqueConstraint('edad', 'sexo','id', name='uq_edad_anio'),
    )

class Pregunta(Base):
    __tablename__ = 'pregunta'
    pk_pregunta = Column(Integer, Sequence('pregunta_seq'),primary_key=True )
    pregunta = Column(String)
    __table_args__ = (
        UniqueConstraint('pregunta', name='uq_pregunta'),
    )

class Respuesta (Base):
    __tablename__ = 'respuesta'
    
    pk_respuesta = Column(Integer, Sequence('respuesta_seq'),primary_key=True )
    respuesta = Column(Integer)
    __table_args__ = (
        UniqueConstraint('respuesta', name='uq_respuesta'),
    )


class Hecho_respuesta (Base):
    __tablename__ = 'hecho_respuesta'

    id = Column(Integer, Sequence('h_res_seq'),primary_key=True)
    pk_fecha = Column(Integer, ForeignKey('fecha.pk_fecha'))
    pk_ubicacion = Column(Integer, ForeignKey('ubicacion.pk_ubicacion'))
    pk_encuestado = Column (Integer, ForeignKey('encuestado.pk_encuestado'))
    pk_pregunta = Column(Integer, ForeignKey('pregunta.pk_pregunta'))
    pk_respuesta = Column(Integer,ForeignKey('respuesta.pk_respuesta'))

    numero_personas = Column(Integer)





def create_ubicacion(df, engine):
    session = Session(bind=engine)

    localidades=df['localidad'].unique()
    municipio=df['municipio'].unique()

    for i in localidades:
        for j in municipio:
            i = i.replace('"', "")
            i= i.lower()
            nuevo_dato=Ubicacion(localidad= i, municipio = j)
            try:
                session.add(nuevo_dato)
                session.commit()

                session.close()
            except:
                pass




def create_fecha(df, engine):
    session = Session(bind=engine)

    localidades=df['ANIO'].unique()
    try:
        for i in localidades:
                nuevo_dato=Fecha(anio= i)
                session.add(nuevo_dato)
        session.commit()

        session.close()
    except:
        pass


def create_encuestados(df, engine):
    session = Session(bind=engine)
    edades=df['edad'].unique()
    sexos=df['sexo'].unique()
    ids = df['id'].unique()
    for k in ids:
    
        for i in edades:
            for j in sexos:
                j = j.replace('"', '')
                i = i.replace('"', '')
                k = k.replace('"', '')

                try:

                    nuevo_dato=Encuestado(edad= int(i), sexo = int(j),id=int(k))
                    session.add(nuevo_dato)
                    session.commit()
                    session.close()
                except:
                    pass
    
def create_encuestado(engine, edad, sexo, id):
    session = Session(bind=engine)
    edad = edad.replace('"', '')
    sexo = sexo.replace('"', '')
    id = id.replace('"', '')
    try:

        nuevo_dato=Encuestado(edad= int(edad), sexo = int(sexo),id=int(id))
        print(nuevo_dato.pk_encuestado)
        session.add(nuevo_dato)
        session.commit()
        session.close()
    except:
                pass


def create_pregunta(engine):
    session = Session(bind=engine)

    pregunta = ['Transmilenio',
       'SITP', 'buseta_o_colectivo', 'automovil_particular', 'Taxi',
       'Motocicleta', 'Bicicleta', 'Ruta_escolar', 'A_pie',
       'bicitaxi_o_mototaxi', 'caballo', 'otros', 'no_se_desplaza',
       'vehículo_patineta_moto_electicos',
       'particulares_plataformas_o_aplicaciones', 'Bus_intermunicipal']
    for i in pregunta:
            nuevo_dato=Pregunta(pregunta = i)
            session.add(nuevo_dato)
    session.commit()

    session.close()

def create_respuesta(engine):
    session = Session(bind=engine)

    
    for i in [1,2,3]:
            nuevo_dato=Respuesta(respuesta= i)
            session.add(nuevo_dato)
    session.commit()

    session.close()


def create_respuesta_h (df, engine):
    pregunta = ['Transmilenio',
       'SITP', 'buseta_o_colectivo', 'automovil_particular', 'Taxi',
       'Motocicleta', 'Bicicleta', 'Ruta_escolar', 'A_pie',
       'bicitaxi_o_mototaxi', 'caballo', 'otros', 'no_se_desplaza',
       'vehículo_patineta_moto_electicos',
       'particulares_plataformas_o_aplicaciones', 'Bus_intermunicipal']
    pk_nill = get_null(engine)
    session = Session(bind=engine)
    for index, row in df.iterrows():
        for i in pregunta:
            fecha = row['ANIO']
            edad = row['edad']
            sexo = row['sexo']
            localidad = row['localidad']
            municipio = row['municipio']
            respuesta = row[i]
            id_enc = row['id']
            pkubicacion = pk_ubicacion(engine,localidad,municipio )
            pkfecha = pk_fecha(engine,fecha)

            create_encuestado(engine,edad, sexo,id_enc)
            pkencuestado = pk_encuestado(engine, edad,sexo,id_enc)
            if i is not None and  respuesta is not None:
                pkpregunta = pk_pregunta(engine, i)
                try:
             
                    pkrespuesta = pk_respuesta(engine, respuesta)
                except:
                    pass
                if (pkrespuesta == 3):
                        if (pk_ubicacion is not None or pk_encuestado is not None):
                            try:
                                find_pairs(engine,pkfecha[0],pkubicacion[0],pkencuestado[0],pkpregunta[0],pk_nill[0])
                            except:
                                pass
                else:
                        if (pk_ubicacion is not None or pk_encuestado is not None):
                            try:

                                find_pairs(engine,pkfecha[0],pkubicacion[0],pkencuestado[0],pkpregunta[0],pkrespuesta[0])

                            except:
                                pass






def get_null(engine):
    print ("Buscando nulo")

    query = text("SELECT pk_respuesta FROM respuesta WHERE respuesta = 3")
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk

def pk_ubicacion(engine, localidad, municipio):
    print ("Buscando ubicación")

    localidad = localidad.replace('"', "'")
    municipio = municipio.replace('"', "")
    localidad = localidad.lower()
    

    query = text("SELECT pk_ubicacion FROM ubicacion WHERE localidad = "+localidad+" AND municipio = "+str(municipio)+";")
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    print(pk)
    return  pk

def pk_fecha(engine, anio):
    print ("Buscando fecha")

    anio = anio.replace('"', '')
    query = text("SELECT pk_fecha FROM fecha WHERE anio = "+str(anio))
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk

def pk_encuestado(engine,edad, sexo, id) :
    print ("Buscando encuestado")

    edad = edad.replace('"', '')
    sexo = sexo.replace('"', '')
    id = id.replace('"','')
    query = text("SELECT pk_encuestado FROM encuestado WHERE id = "+str(int(id))+";")
    print(query)
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    print(pk)
    return  pk


def pk_pregunta(engine, pregunta):
    print ("Buscando pregunta")

    pregunta = pregunta.replace('"', "'")
    print(pregunta)
    query = text("SELECT pk_pregunta FROM pregunta WHERE pregunta = '"+pregunta+"'")
    conn = engine.connect()
    pk = conn.execute(query).fetchone()
    conn.close()
    return  pk

def pk_respuesta(engine, respuesta):
    print("Buscando respuesta")

    if respuesta is None :
        pk = 3
    else:
        try:
            respuesta = respuesta.replace('"', '')
            query = text("SELECT pk_respuesta FROM respuesta WHERE respuesta = " + str(respuesta))
            conn = engine.connect()
            pk = conn.execute(query).fetchone()
            conn.close()

        except Exception as e:
            return 3

    return pk


def find_pairs(engine, pk_fecha ,  pk_ubicacion ,pk_encuestado, pk_pregunta, pk_respuesta):
    print ("Buscando pk")

    query = text("SELECT id FROM hecho_respuesta WHERE pk_fecha = "+str(int(pk_fecha))+ " and pk_ubicacion =  "+str(int(pk_ubicacion))+ " and pk_encuestado= "+str(int(pk_encuestado))+" and pk_pregunta= "+str(int(pk_pregunta))+" and pk_respuesta ="+str(int(pk_respuesta)))
    conn = engine.connect()
    try:
        pk = conn.execute(query).fetchone()

    except Exception as e:
            print("nuevo dato")
            session = Session(bind=engine)


            nuevo_dato = Hecho_respuesta(pk_fecha= int(pk_fecha), pk_ubicacion = int(pk_ubicacion), pk_encuestado= int(pk_encuestado), pk_respuesta = int(pk_pregunta), pk_pregunta = int(pk_pregunta), numero_personas = 1)
            session.add(nuevo_dato)
            session.commit()
            session.close()

            return None
    
    if (pk is None):
            print("nuevo dato")
            session = Session(bind=engine)


            nuevo_datos = Hecho_respuesta(pk_fecha= pk_fecha, pk_ubicacion = pk_ubicacion, pk_encuestado= pk_encuestado, pk_respuesta = pk_respuesta, pk_pregunta = pk_pregunta, numero_personas = 1)
            print(nuevo_datos)
            session.add(nuevo_datos)
            session.commit()
            session.close()

            return None
    else:

        print("update")

        update_count_people(engine,id=pk)
        conn.commit()
        conn.close()
    return  pk

def update_count_people (engine, id):
    print (id)

    query = text("UPDATE hecho_respuesta SET numero_personas = numero_personas + 1 WHERE id = "+str(id[0]) )
    print(query)
    conn = engine.connect()
    pk = conn.execute(query)
    conn.commit()

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
        engine = create_engine(f'postgresql://{uid}:{pwd}@localhost:5432/'+database,pool_size=10000)

        with engine.connect() as connection:
            print("¡Conexión exitosa!")

            Base.metadata.create_all(engine)

            print ("Tablas creadas")
            create_ubicacion(df, engine)

            create_fecha(df,engine)
            #create_encuestado(df, engine)
            create_respuesta( engine)
            create_pregunta( engine)
            create_respuesta_h(df, engine)

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









