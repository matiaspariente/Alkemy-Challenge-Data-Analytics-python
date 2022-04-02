Challenge Data Analytics - Python 

Autor: Matias Pariente

El archivo configurable .env contiene los siguientes parámetros:

DB_SERVER_USER --> Usuario de la base de datos
DB_SERVER_PASSWORD --> Password de la base de datos
DB_NAME --> Nombre de la base de datos
DB_HOST --> Host de la base de datos
DB_PORT --> Puerto de la base de datos
URL_MUSEOS --> URL a la web del gobierno con acceso a el csv de Museos
URL_CINES --> URL a la web del gobierno con acceso a el csv de Cines
URL_BIBLIOTECASPOPULARES --> URL a la web del gobierno con acceso a el csv de Bibliotecas Populares
NORMALIZAR_MUSEOS --> Elementos de la tabla Museos a normalizar
NORMALIZAR_CINES --> Elementos de la tabla Cines a normalizar
NORMALIZAR_BIBLIOTECAS --> Elementos de la tabla Bibliotecas Populares a normalizar
NORMALIZADOS --> Elementos normalizados 
SQL_TABLE_INFORMACIONCULTURAL --> Nombre de la tabla de Informacion Cultural para la base de datos
SQL_TABLE_REGISTROSTOTALES --> Nombre de la tabla de Registros Totales para la base de datos
SQL_TABLE_INFORMACIONCINES --> Nombre de la tabla de Informacion Cines para la base de datos

La creación de las tablas en la base de datos se realiza con la ejecución de script.py

La carga de los archivos fuente, procesamiento de los datos y actualización de tablas de la base de datos se realiza con la ejecion de app.py