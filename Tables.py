from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Sequence

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
    
    pk_Respuesta = Column(Integer, Sequence('respuesta_seq'),primary_key=True )
    respuesta = Column(String)

class hecho_respuesta (Base):
    __tablename__ = 'hecho_respuesta'

    id = Column(Integer, primary_key=True)
    pk_fecha = Column(Integer, ForeignKey('fecha.pk_fecha'))
    pk_ubicacion = Column(Integer, ForeignKey('ubicacion.pk_ubicacion'))
    pk_encuestado = Column (Integer, ForeignKey('encuestado.pk_encuestado'))
    pk_Respuesta = Column(Integer,ForeignKey('respuesta.pk_Respuesta'))
    pk_pregunta = Column(Integer, ForeignKey('pregunta.pregunta'))
    numero_personas = Column(Integer)



