import pandas as pd
import sqlalchemy as db
from sqlalchemy import text
import logging
import csv
import sys
import random
from datetime import datetime, timedelta
from config_etl import DATABASE_CONFIG, DATABASE_CONFIG_DW, CSV_FILES, LOG_FILE

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(config):
    """
    Funcion que permite la conexión a la base de datos MySQL.
    """
    try:
        engine= db.create_engine(f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        conn = engine.connect()
        logging.info("Conexion a la base de datos exitosa")
        return conn
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        sys.exit(1)

def read_sql_db(query,database):
    """
    Funcion que permite la lectura de la base de datos MySQL ingresando una consulta.
    """
    conn = create_db_engine(database)
    try:
        df=pd.read_sql(query, con=conn)
        logging.info("Lectura de la base de datos exitosa")
        return df
    except Exception as e:
        logging.error(f"Error al intentar leer la base de datos: {e}")
        sys.exit(1)

def insert_sql_db(df: pd.DataFrame, tabla: str, tipo_insercion: str, database):
    """
    Funcion que permite la lectura de la base de datos MySQL ingresando una consulta.
    """
    conn = create_db_engine(database)

    query = f"""
        SELECT * FROM {tabla}; 
        """
    df2 = read_sql_db(query,database)
    df = df.astype(str)
    df2 = df2.astype(str)
    df1 = df.merge(df2, how='left', indicator=True)
    df1 = df1[df1['_merge'] == 'left_only'].drop(columns='_merge')
    
    try:
        df1.to_sql(tabla, conn, if_exists=tipo_insercion, index=False)
        logging.info(f"Inserción de datos a la tabla {tabla} de manera exitosa")
    except Exception as e:
        logging.error(f"Error al intentar escribir datos a la tabla {tabla}: {e}")
        sys.exit(1)

def transform_column_int(df: pd.DataFrame, columna: str):
    """
    Funcion que analiza una columna específica de un DataFrame y cambia el tipo de dato a integer.
    """
    if columna not in df.columns:
        logging.error(f"La columna '{columna}' no existe en el DataFrame")
        sys.exit(1)
    df[columna]=df[columna].astype('int')
    return df

def rename_column(df: pd.DataFrame, columna: str,nuevo_nombre: str):
    """
    Funcion que renombra una columna de un dataframe al nuevo nombre indicado.
    """
    if columna not in df.columns:
        logging.error(f"La columna '{columna}' no existe en el DataFrame")
        sys.exit(1)
    df.rename(columns={columna:nuevo_nombre}, inplace=True)
    return df

def drop_column(df: pd.DataFrame, columna: str):
    """
    Funcion que quita una columna de un dataframe.
    """
    if columna not in df.columns:
        logging.error(f"La columna '{columna}' no existe en el DataFrame")
        sys.exit(1)
    df.drop(columns={columna:columna}, inplace=True)
    return df

def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame,columna_df1: str, columna_df2: str):
    """
    Funcion que combina dos dataframes en uno a través de las columnas que se indiquen.
    """
    if columna_df1 not in df1.columns or columna_df2 not in df2.columns:
        logging.error(f"Una de las columnas ingresadas no existe en los dataframes")
        sys.exit(1)
    df=pd.merge(df1,df2, left_on=columna_df1, right_on=columna_df2)
    return df

def read_csv(file_path):
    """
    Función que realiza la lectura de un archivo en formato CSV (delimitado por ',' o '|') y regresa un DataFrame con la información.
    """
    try:
        with open(file_path, 'r') as file:
            sample = file.read(1024)
            delimiter = csv.Sniffer().sniff(sample).delimiter
        
        df = pd.read_csv(file_path, sep=delimiter)
        logging.info(f"Archivo {file_path} leído correctamente con delimitador '{delimiter}'")
        return df

    except Exception as e:
        logging.error(f"Error al leer archivo {file_path}: {e}")
        sys.exit(1)

def gen_rating():
    """
    Funcion que genera un numero aleatorio entre 0 y 5 con 1 decimal.
    """   
    numero_aleatorio = round(random.uniform(0, 5), 1)
    return numero_aleatorio

def gen_timestamp():
    """
    Funcion que genera un timestamp aleatorio dentro de un rango específico.
    """
    start_date = datetime(2024, 1, 15)
    end_date = datetime(2024, 4, 6)
    random_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
    return random_date

def gen_fact_watchs(df1: pd.DataFrame, df2: pd.DataFrame):
    """
    Funcion que genera el dataframe para el fact_watchs a partir de los dataframe de user y movies.
    """
    if "userID" not in df1.columns or "movieID" not in df2.columns:
        logging.error(f"Una de las columnas necesarias para la generacion de fact_watchs no existe en los dataframes")
        sys.exit(1)
    users_id=df1["userID"]
    movies_id=df2["movieID"]
    watchs_data=pd.merge(users_id,movies_id, how="cross")
    watchs_data["rating"]=watchs_data["movieID"].apply(lambda x: gen_rating())
    watchs_data["timestamp"]=watchs_data["userID"].apply(lambda x: gen_timestamp())
    return watchs_data

def main():
    """
    Funcion que ejecuta el etl completo para poblar datos en todas las tablas del datawarehouse.
    """
    # 1) Poblar data en tabla dimMovie
    query = """
            SELECT 
                movie.movieID as movieID, movie.movieTitle as title, movie.releaseDate as releaseDate, 
                gender.name as gender , person.name as participantName, participant.participantRole as roleparticipant 
            FROM movie 
            INNER JOIN participant ON movie.movieID=participant.movieID
            INNER JOIN person ON person.personID = participant.personID
            INNER JOIN movie_gender ON movie.movieID = movie_gender.movieID
            INNER JOIN gender ON movie_gender.genderID = gender.genderID
            """
    movies_data = read_sql_db(query, DATABASE_CONFIG)
    movies_data = transform_column_int(movies_data,'movieID')
    movies_award = read_csv(CSV_FILES['awards_movie'])
    movies_award = transform_column_int(movies_award,'movieID')
    movies_award = rename_column(movies_award,"Aware","Award")
    movie_data = merge_dataframes(movies_data,movies_award,"movieID", "movieID")
    movie_data = rename_column(movie_data,"releaseDate","releaseMovie")
    movie_data = rename_column(movie_data,"Award","awardMovie")
    movie_data = drop_column(movie_data,'IdAward')
    insert_sql_db(movie_data, 'dimMovie', 'append', DATABASE_CONFIG_DW)

    # 2) Poblar data en tabla dimUser
    users = read_csv(CSV_FILES['users'])
    users = rename_column(users,"idUser","userID")
    insert_sql_db(users, 'dimUser', 'append', DATABASE_CONFIG_DW)

    # 3) Poblar data en tabla factWatchs
    watchs_data = gen_fact_watchs(users,movie_data)
    insert_sql_db(watchs_data, 'FactWatchs', 'append', DATABASE_CONFIG_DW)

    logging.info("Proceso etl se ejecuto correctamente")

if __name__ == "__main__":
    main()