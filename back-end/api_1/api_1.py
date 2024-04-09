from fastapi import FastAPI, Depends, HTTPException, Response, UploadFile, status
from fastapi.security import OAuth2PasswordBearer
from datetime import datetime, timedelta
from jose import JWTError, jwt
import hashlib
import requests
from uuid import uuid4
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import magic
import uvicorn
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pymongo
import urllib
import certifi
from m_database import User_token
import random 

load_dotenv()

mongo_username = os.getenv('mongo_username')
mongo_password = os.getenv('mongo_password')
mongo_cluster = os.getenv('mongo_cluster')

mongo_url = f'mongodb+srv://{urllib.parse.quote_plus(mongo_username)}:{urllib.parse.quote_plus(mongo_password)}@{mongo_cluster}/?retryWrites=true&w=majority'

session = boto3.Session(
    aws_access_key_id= os.getenv("aws_access_key_id"),
    aws_secret_access_key= os.getenv("aws_secret_access_key")
)

def print_tokens_in_db():
    tokens_from_mongo = list(User_token.find())
    # print("Number of tokens in the database:", len(tokens_from_mongo))
    # print("Tokens in the database:")
    users_and_tokens=[]
    for each in tokens_from_mongo:
        temp=[]
        user=each['email']
        token=each['token']
        temp.append(user)
        temp.append(token)
        users_and_tokens.append(temp)
    return users_and_tokens

users_and_tokens = print_tokens_in_db()

file_details=[]

app = FastAPI() 

supported_file_types={
    'application/pdf': 'pdf'
}

AWS_Bucket = 'file-storage-assignment-4'
s3 = boto3.resource('s3')
bucket = s3.Bucket(AWS_Bucket)

def check_valid_user(token):
    found =[]
    for each in users_and_tokens:
        if each[1]==token:
            found = each
            return found
    if found==[]:
        return False

async def s3_upload(contents: bytes, key: str, folder: str):
    s3_client = boto3.client('s3',
    aws_access_key_id= os.getenv("aws_access_key_id"),
    aws_secret_access_key= os.getenv("aws_secret_access_key"))
    full_key = os.path.join(folder, key)
    s3_client.put_object(Bucket="file-storage-assignment-4", Key=full_key, Body=contents)


def check_s3_connection():
    try:
        s3_client = session.client('s3')
        print("Connected to S3 successfully")
        return True
    except NoCredentialsError:
        print("Credentials not found")
        return False
    except ClientError as e:
        print(f"Connection error: {e}")
        return False
    
def check_exists(file_md5):
    found = []
    try:
        for each in file_details:
            if each["md5"] == file_md5:
                found = each
    except:
        return False
    
    if found==[]:
        return False
    else:
        return found

@app.post('/upload')
async def upload(token: str, file: UploadFile):

    user_details = check_valid_user(token)
    if user_details==False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Operation not authorised'
        )
    
    if not file:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='No file found!'
        )
    contents = await file.read()

    file_type = magic.from_buffer(buffer=contents, mime=True)
    if file_type not in supported_file_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Unsupported file type'
        )
    result = str((hashlib.md5(contents)).digest())
    found = check_exists(result)
    if found == False:
        unique_identifier = uuid4()
        file_name = file.filename
        await s3_upload(contents=contents, key=file_name, folder='raw_pdf_files/')
        s3_location = "s3://" + AWS_Bucket + "/" + "raw_pdf_files/"+file_name
        details = {
            "_id": str(datetime.utcnow()),
            "user": user_details[0],
            "file_name": file.filename,
            "unique_identifier": str(unique_identifier),
            "md5": result,
            "location": s3_location,
            "status": "queued"
        }
        file_details.append(details)
        client = pymongo.MongoClient(mongo_url,tlsCAFile=certifi.where())
        try:
            conn = client.server_info()
            print(f'Connected to MongoDB {conn.get("version")}')
        except Exception:
            print("Unable to connect to the MongoDB server.")

        db = client['BigDataAssignment4']
        user_files = db.user_files
        temp=[]
        temp.append(details)
        user_files.insert_many(temp)
        return details
    else:
        return found


AIRFLOW_API_BASE_URL = "http://34.139.115.254:8080/api/v1"

@app.post('/trigger_airflow')
async def trigger_dag(token: str, s3_location: str):
    current_time = datetime.now()
    user_details = check_valid_user(token)
    if user_details==False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Operation not authorised'
        )
    endpoint = f"{AIRFLOW_API_BASE_URL}/dags/pdf_dag/dagRuns"
    username = os.getenv("airflow_un")
    password = os.getenv("airflow_pas")
    rand1 = random.randint(1,1000)
    rand2 = random.randint(1,1000)
    dag_run_id = str("id_run_" +str(rand1)+str(rand2))
    response = requests.post(
        endpoint,
        auth=(username, password),
        json={"conf": {"s3_uri": s3_location}, "dag_run_id": dag_run_id},
        headers={"Content-Type": "application/json"},
    )
    print(response.text)
    if response.status_code == 200:
        return {"message": "DAG triggered successfully"}
    else:
        return {"error": "Failed to trigger DAG"}

@app.post('/status_of_uploaded_file')
async def status_check(token):
    user_docs = []
    user_details = check_valid_user(token)
    if user_details==False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Operation not authorised'
        )   
    else:
        for each in file_details:
            each['status'] = "queued" # logic to hit airflow 
            if user_details[0] == each['user']:
                user_docs.append(each)
    return user_docs
    