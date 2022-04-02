import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from googletrans import Translator
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from decouple import config


df = ['','','',''] #Array de data frames [Museos,Cines,Bibliotecas,copia de cines sin normalizar] 
dfDest =['','',''] #Array de data frames de tablas generadas[informacion General,registros totales,informacion cines]           
ultimoDestino= ['','',''] #Array con destinos de los ultimos archivos fuente guardamos

translator = Translator()

logging.basicConfig(filename="app.log",level='INFO')

def archivos_fuente():

    fuentes=['museos','salasDeCine','bibliotecasPopulares'] 
    urlFuentes=[config("URL_MUSEOS"),config("URL_CINES"),config("URL_BIBLIOTECASPOPULARES")]

    for i in range(len(fuentes)): 
        try:
            res = requests.get(urlFuentes[i])
            if(res.status_code>=400) : raise Exception("[{0}]No se ha podido acceder a la pagina: {1}".format(datetime.today().strftime('%d/%m/%y %H:%M'),urlFuentes[i])) #si tengo un estado mayor a 400 no tengo status del get correcto, lanzo excepcion
            soup = BeautifulSoup(res.text,"html.parser")
            url = soup.find("div",class_="resource-actions").find("a").attrs.get("href") # Se busca en la web el link del archivo csv a descargar a traves de las librerias request y beautifulsoap
            file = requests.get(url, allow_redirects=True) 
            if(res.status_code>=400) : raise Exception("No se ha podido acceder al archivo descargable de {0}".format(fuentes[i])) 
            carpetaDestino='{fuentes}/{año}-{mes}/'.format(fuentes=fuentes[i],año=datetime.today().strftime('%Y'),mes=translator.translate(datetime.today().strftime('%B'),dest='es').text) 
            path = Path(carpetaDestino)
            path.mkdir(parents=True,exist_ok=True) # se la carpeta de destino con la direccion generada en las lineas anteriores
            ultimoDestino[i] = carpetaDestino+'{fuentes}-{fecha}.csv'.format(fuentes=fuentes[i],fecha=datetime.today().strftime('%Y-%m-%d'))
            open(ultimoDestino[i], 'wb').write(file.content) #se genera el archivo descargado 
        except AttributeError:
            logging.error("[{0}]No se ha encontrado el archivo descargable: {1}".format(datetime.today().strftime('%d/%m/%y %H:%M'),fuentes[i]))
        except FileNotFoundError:
            logging.error("[{0}]Error al generar el archivo {1}".format(datetime.today().strftime('%d/%m/%y %H:%M'),ultimoDestino[i]))    
        except Exception as error:
            logging.error(error.args[0])    
        else:
            logging.info("[{0}]El archivo {1} se ha guardado correctamente".format(datetime.today().strftime('%d/%m/%y %H:%M'),ultimoDestino[i]))    

    
def actualizacion(): # a traves de pandas se generan los dataframes correspondiente y se los normaliza con la informacion solicitada

    elementosANormalizar= [config("NORMALIZAR_MUSEOS",cast=lambda v:[s.strip() for s in v.split(',')]), 
                      config("NORMALIZAR_CINES",cast=lambda v:[s.strip() for s in v.split(',')]),
                      config("NORMALIZAR_BIBLIOTECAS",cast=lambda v:[s.strip() for s in v.split(',')])]
    elementosNormalizados= config("NORMALIZADOS",cast=lambda v:[s.strip() for s in v.split(',')])


    for i in range(len(ultimoDestino)):  
        try:
            df[i] = pd.read_csv(ultimoDestino[i], encoding="latin-1")
            if (i==1) : df[3] = df[1]
            df[i]= df[i].loc[:,elementosANormalizar[i]] #Se filtran solo los items que a normalizar
            df[i].columns = elementosNormalizados
        except FileNotFoundError:
            logging.error("[{0}]No se ha encontrado el archivo {1}".format(datetime.today().strftime('%d/%m/%y %H:%M'),ultimoDestino[i]))
        except KeyError:
            logging.error("[{0}]Uno o mas elementos a normalizar no existe/n en la tabla {1}".format(datetime.today().strftime('%d/%m/%y %H:%M'),ultimoDestino[i]))   


def get_tabla_informacionCultural(): # Se crea una unica tabla con la informacion de todas las tablas normalizadas

    try:
        informacionCultural = pd.concat([df[0],df[1],df[2]],ignore_index=True)
        informacionCultural['provincia'] = informacionCultural['provincia'].replace({"CÃ³rdoba": "Córdoba", "Ciudad AutÃ³noma de Buenos Aires": "Ciudad Autónoma de Buenos Aires","Entre RÃ­os":"Entre Rios",
                                            "RÃ­o Negro":"Rio Negro","NeuquÃ©n":"Neuquén","NeuquÃ©nÂ ":"Neuquén","TucumÃ¡n":"Tucumán","Tierra del Fuego, AntÃ¡rtida e Islas del AtlÃ¡ntico Sur":"Tierra del Fuego, Antártidad e Islas del Atlántico Sur",
                                            "Tierra del Fuego":"Tierra del Fuego, Antártidad e Islas del Atlántico Sur","Santa FÃ©":"Santa Fe"})# se corrigen los elementos de las provincias para luego eliminar repetidas
        fecha=datetime.today().strftime('%Y-%m-%d') 
        informacionCultural['Fecha de Modificación'] = fecha # se agrega una columna con la fecha
        dfDest[0]=informacionCultural
    except:
        logging.error("[{0}]Ocurrio un error en la creacion de la tabla informacionCultural".format(datetime.today().strftime('%d/%m/%y %H:%M')))
    else:
        logging.info("[{0}]La tabla informacionCultural fue generada".format(datetime.today().strftime('%d/%m/%y %H:%M')))

         
def get_tabla_registrosTotales():
    try:
        registroCategoria = pd.DataFrame({"Registros Totales":dfDest[0].groupby(['categoría']).size()}) # se agrupa por categoria y se toma la cantidad por dicha agrupacion
        registroProvinciaCategoria = pd.DataFrame({"Registros Totales":dfDest[0].groupby(['provincia', 'categoría']).size()}) # se agrupa por categoria-provincia y se toma la cantidad por dicha agrupacion
        registrosTotales = pd.concat([registroCategoria,registroProvinciaCategoria]) # se concatena para dejarlo en una unica tabla
        registrosTotales
        fecha=datetime.today().strftime('%Y-%m-%d')
        registrosTotales['Fecha de Modificación'] = fecha # se agrega una columna con la fecha
        dfDest[1]=registrosTotales
    except:
        logging.error("[{0}]Ocurrio un error en la creacion de la tabla registrosTotales".format(datetime.today().strftime('%d/%m/%y %H:%M')))
    else:
        logging.info("[{0}]La tabla registrosTotales fue generada".format(datetime.today().strftime('%d/%m/%y %H:%M')))


def get_tabla_cines():

    try:
        informacionCines = df[3]
        informacionCines['Provincia']=informacionCines['Provincia'].replace({"CÃ³rdoba": "Córdoba", "Ciudad AutÃ³noma de Buenos Aires": "Ciudad Autónoma de Buenos Aires","Entre RÃ­os":"Entre Rios",
                                        "RÃ­o Negro":"Rio Negro","NeuquÃ©n":"Neuquén","NeuquÃ©nÂ ":"Neuquén","TucumÃ¡n":"Tucumán","Tierra del Fuego, AntÃ¡rtida e Islas del AtlÃ¡ntico Sur":"Tierra del Fuego, Antártidad e Islas del Atlántico Sur",
                                        "Tierra del Fuego":"Tierra del Fuego, Antártidad e Islas del Atlántico Sur","Santa FÃ©":"Santa Fe"})# se corrigen los elementos de las provincias para luego eliminar repetidas
        informacionCines_2=informacionCines.loc[:,['Provincia','Pantallas','Butacas','espacio_INCAA']] # Se filtra por elementos solicitados y se hace una copia para trabajar por separado con espacioINCAA ya que tiene distinto formato
        informacionCines=informacionCines_2.groupby(['Provincia']).sum() # se agrupoa por provincia y se toma el total de la suma.
        informacionCines['espacio_INCAA'] = informacionCines_2.groupby(['Provincia']).count()['espacio_INCAA'] # se agrupa por categoria-provincia y se toma la cantidad por dicha agrupacion
        fecha=datetime.today().strftime('%Y-%m-%d')
        informacionCines['Fecha de Modificación'] = fecha # se agrega una columna con la fecha
        dfDest[2]=informacionCines
    except:
        logging.error("[{0}]Ocurrio un error en la creacion de la tabla registrosTotales".format(datetime.today().strftime('%d/%m/%y %H:%M')))
    else:
        logging.info("[{0}]La tabla registrosTotales fue generada".format(datetime.today().strftime('%d/%m/%y %H:%M')))


def actualizacion_DB():
    try:
        engine = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(config("DB_SERVER_USER"),config("DB_SERVER_PASSWORD"),config("DB_HOST"),config("DB_PORT"),config("DB_NAME"))) # se hace la conexion con postgresSQL
        dfDest[0].to_sql(config("SQL_TABLE_INFORMACIONCULTURAL"),con=engine,if_exists="replace") #Se actualizan las tablas con los dataFrames generados
        dfDest[1].to_sql(config("SQL_TABLE_REGISTROSTOTALES"),con=engine,if_exists="replace")
        dfDest[2].to_sql(config("SQL_TABLE_INFORMACIONCINES"),con=engine,if_exists="replace")
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        if "port" in error: logging.critical("[{0}]Error de comunicacion, verificar HOST:PUERTO".format(datetime.today().strftime('%d/%m/%y %H:%M')))
        else :logging.critical("[{0}]Error de autenticacion con la base de datos verificar Usuario DB y password".format(datetime.today().strftime('%d/%m/%y %H:%M')))
    except:
        logging.error("[{0}]Ocurrio un error en la actualizacion de la base de datos".format(datetime.today().strftime('%d/%m/%y %H:%M')))    
    else:
        logging.info("[{0}]Las bases de datos se actualizaron correctamente".format(datetime.today().strftime('%d/%m/%y %H:%M')))

archivos_fuente()
actualizacion()
get_tabla_informacionCultural()
get_tabla_registrosTotales()
get_tabla_cines()
actualizacion_DB()