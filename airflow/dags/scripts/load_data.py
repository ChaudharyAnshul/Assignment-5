import configparser
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import boto3
from io import BytesIO, StringIO

config = configparser.ConfigParser()
config.read('/opt/airflow/dags/scripts/configuration.properties')

def getSnowflakeEngine():

  # Define Snowflake connection parameters
  snowflake_username = config['snowflake']['snowflake_username']
  snowflake_password = config['snowflake']['snowflake_password']
  snowflake_account = config['snowflake']['snowflake_account']
  snowflake_database = config['snowflake']['snowflake_database']
  snowflake_schema = config['snowflake']['snowflake_schema']
  snowflake_warehouse = config['snowflake']['snowflake_warehouse']

  engine = create_engine(URL(
    account=snowflake_account,
    user=snowflake_username,
    password=snowflake_password,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse
  ))

  return engine

def loadData(file_name):
  print("------- Starting Loaging -------")  
  copy_file = f"""copy into PDF_PYPDF_CONTENT_TABLE from @PDF_PYPDF_CONTENT_STAGE
                FILES = ('{file_name}')
                FILE_FORMAT = (FORMAT_NAME = PDF_DATA_FF);"""
  engine = getSnowflakeEngine()
  print(copy_file)
  connection = engine.connect()
  results = connection.execute('select current_version()').fetchone()
  print(results[0])
  with engine.connect() as connection:
    connection.execute(copy_file)
  
  print("succesfully loaded data")
  print("------- Ending Loaging -------")
