import snowflake.connector
import configparser

# Function to read configurations
def get_snowflake_connection():
    config = configparser.ConfigParser()
    config.read('configuration.properties')
    print(config['snowflake'])
    snowflake_config = {
        "user": config['snowflake']['user'],
        "password": config['snowflake']['password'],
        "account": config['snowflake']['account'],
        "warehouse": config['snowflake']['warehouse'],
        "database": config['snowflake']['database'],
        "schema": config['snowflake']['schema']
    }
    return snowflake.connector.connect(** snowflake_config)

def execute_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


