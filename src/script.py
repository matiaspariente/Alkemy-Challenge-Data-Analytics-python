import psycopg2
import logging
from datetime import datetime
from decouple import config

def main():

    logging.basicConfig(filename="script.log",level='INFO')

    try:
        con = psycopg2.connect("dbname={0} user={1} password={2}".format(config("DB_NAME"),config("DB_SERVER_USER"),config("DB_SERVER_PASSWORD"))) #Conexion con servidor
        cursor = con.cursor()
    except:
        logging.critical("[{0}]error de conexion con DB".format(datetime.today().strftime('%d/%m/%y %H:%M')))

    ##Arrays de nombre de tablas y de scripts SQL para la creacion de las tablas     
    sqlTables=["SQL_TABLE_INFORMACIONCULTURAL","SQL_TABLE_REGISTROSTOTALES","SQL_TABLE_INFORMACIONCINES"]     
    query=[
        'CREATE TABLE public.{0} (index bigint NOT NULL, cod_localidad bigint, id_provincia bigint, categoría text, provincia text,localidad text, nombre text, domicilio text, "código postal" text, "número de teléfono" text, mail text, web text)'.format(config(sqlTables[0])),
        'CREATE TABLE public.{0} (index text, "Registros Totales" bigint)'.format(config(sqlTables[1])),
        'CREATE TABLE public.{0} (Provincia text, Pantallas bigint, Butacas bigint, espacio_INCAA bigint)'.format(config(sqlTables[2]))
        ]
    
    ##CREACION DE TABLAS
    for i in range(len(query)):
        try:   
            cursor.execute(query[i])
            con.commit()
        except psycopg2.errors.DuplicateTable:
            logging.error("[{0}]Error:Tabla {1} ya existe".format(datetime.today().strftime('%d/%m/%y %H:%M'),config(sqlTables[i])))
        except:
            logging.error("[{0}]Error:No es posible generar tabla {1}".format(datetime.today().strftime('%d/%m/%y %H:%M'),config(sqlTables[i])))    
        else:    
            logging.info("[{0}]Se creo la tabla {1} la DB con exito".format(datetime.today().strftime('%d/%m/%y %H:%M'),config(sqlTables[i])))
    
    con.close()

main()