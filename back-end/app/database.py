from pymongo import MongoClient
import pymongo
import urllib 

import configparser

# Initialize ConfigParser
config = configparser.ConfigParser()
config.read('configuration.properties')

# Get MongoDB URI components from the config file under the [mongodb] section
mongo_username = config['mongodb']['mongo_username']
mongo_password = config['mongodb']['mongo_password']
mongo_cluster = config['mongodb']['mongo_cluster']

# Construct MongoDB URI with proper escaping
mongo_url = f'mongodb+srv://{urllib.parse.quote_plus(mongo_username)}:{urllib.parse.quote_plus(mongo_password)}@{mongo_cluster}/?retryWrites=true&w=majority'

# Initialize the MongoClient with the MongoDB URI
client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)

try:
    conn = client.server_info()
    print(f'Connected to MongoDB {conn.get("version")}')
except Exception:
    print("Unable to connect to the MongoDB server.")

db = client['BigDataAssignment4']
User = db.users
User.create_index([("email", pymongo.ASCENDING)], unique=True)

User_token = db.user_tokens