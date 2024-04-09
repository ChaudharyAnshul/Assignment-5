from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from datetime import datetime, timedelta
from m_database import User_token
import snowflake_helper as snowflake_helper
from pydantic import BaseModel
from mongo_extracted_data import print_tokens_in_db

class Snowflake_data(BaseModel):
    token: str

app = FastAPI()

@app.post("/get_data")
async def get_data_from_snowflake(payload: Snowflake_data):
    all_tokens = print_tokens_in_db()
 
    if payload.token in all_tokens:
        data = pdf_data()
        return data
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Please provide correct token')

def pdf_data():
    connection = snowflake_helper.get_snowflake_connection()  
    cursor = connection.cursor()
    query = "SELECT CURRICULUM_TOPIC, CURRICULUM_REFRESHER_READING, CURRICULUM_YEAR, CFA_LEVEL, LEARNING_OUTCOMES FROM ASSIGNMENT4.ASSIGNMENT4_SCHEMA.PDF_PYPDF_CONTENT_TABLE"
    cursor.execute(query)
    results = cursor.fetchall()
    connection.close()
    data = [{'CURRICULUM_TOPIC': row[0], 'CURRICULUM_REFRESHER_READING': row[1], 'CURRICULUM_YEAR': row[2], 'CFA_LEVEL': row[3], 'LEARNING_OUTCOMES': row[4]} for row in results]
    return data

