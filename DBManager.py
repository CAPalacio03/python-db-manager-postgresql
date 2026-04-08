"""
Fecha: 10 de Mayo de 2025
Autor: Carlos A. Palacio A.

DBManager es un progama que se conceta a cualquier base de datos dadas las 
credenciales que se encuentran en un archivo config.ini. El programa tambien es
capaz de ejecutar quueries y guardarlos en objetos DataFrame, así como subir tablas
a la base.
"""

import configparser

import psycopg2

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

import pandas as pd

class DBManager():
    """
        Data Base Manager

        Inicializa una instancia del administrador de base de datos
        leyendo los parámetros de conexión desde un archivo de configuración.
    """

    def __init__(self, config_file='config.ini'):
        """"
        Parámetros
        ----------
        config_file : str, opcional
            Ruta del archivo .ini donde se encuentran definidas las 
            credenciales para acceder a la base de datos y otras configuraciones. 
            Valor por defecto: 'config.ini'.

        Atributos
        ---------
        config : configparser.ConfigParser
            Objeto que contiene la configuración leída del archivo .ini.
        
        conn : psycopg2.extensions.connection
            Objeto de conexión activa a la base de datos PostgreSQL.
        """
        # Cargar configuración
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Configurar conexión
        db_params = {
            'host': self.config['database']['host'],
            'port': self.config['database']['port'],
            'dbname': self.config['database']['dbname'],
            'user': self.config['database']['user'],
            'password': self.config['database']['password']
        }

        # Crear la URL segura
        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port'],
            database=db_params['dbname']
        )

        # Crear conexión con psycopg2 (opcional, si necesitas usar cursor manualmente)
        self.conn = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )

        # Crear engine de SQLAlchemy
        self.engine = create_engine(db_url)

        # Crear conexión
        print('Conexión Exitosa!!')

    def upload_dataframe(self, df, table_name, if_exists='replace', chunksize=1000, index=False):
        """"
        Sube un DataFrame a la base de datos PostgreSQL.
        
        Parámetros:
        -----------
        df : pandas.DataFrame
            DataFrame a subir a la base de datos
        table_name : str
            Nombre de la tabla destino en la base de datos
        if_exists : str, opcional ('fail', 'replace', 'append')
            Qué hacer si la tabla ya existe (por defecto 'append')
        chunksize : int, opcional
            Número de filas a insertar en cada lote (por defecto 1000)
        index : bool, opcional
            Si se debe incluir el índice del DataFrame como columna (por defecto False)
        """

        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                chunksize=chunksize,
                index=index,
                method='multi'
            )
            print(f"DataFrame subido exitosamente a la tabla '{table_name}'")
            return True
        except ImportError as e:
            print(f"Error al subir el DataFrame: {e}")
            return False

    def get_query(self, query_name):
        """
        Obtiene la consulta SQL correspondiente a un nombre definido en el archivo de configuración.

        Este método busca en la sección [queries] del archivo `config.ini` el query asociado 
        al nombre proporcionado.

        Parámetros
        ----------
        query_name : str
            Clave del diccionario de consultas definida en el archivo `config.ini`.

        Retorna
        -------
        str
            Consulta SQL en formato de texto plano.
        """

        return self.config['queries'][query_name]

    def fetch_dataframe(self, query_name):
        """
        Ejecuta una consulta SQL almacenada y la devuelve como un DataFrame.

        Parámetros
        ----------
        query_name : str
            Nombre de la consulta definida en el archivo de configuración.

        Retorna
        -------
        pandas.DataFrame
            Resultado de la consulta SQL convertido en un DataFrame de pandas.
        """
        query = self.get_query(query_name)
        return pd.read_sql_query(query, self.conn)


    def execute_sql(self, query):
        """
        Ejecuta una consulta SQL arbitraria (no almacenada en el archivo de configuración)
        y la devuelve como un DataFrame.

        Parámetros
        ----------
        query : str
            Consulta SQL en texto plano.

        Retorna
        -------
        pandas.DataFrame
            Resultado de la consulta convertido en un DataFrame.
        """
        return pd.read_sql_query(query, self.conn)
    
    def close(self):
        """
        Desconoxion a la base de datos
        """
        self.conn.close()
        return print('Desconexión de la base')
