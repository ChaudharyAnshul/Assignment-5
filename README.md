# Assignment-4

## Live application Links
[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=1YvvKu38ZeIrlWY-Pgls1Gwes7ZaCuarZ1gx1VVb-qKI#0)
- Airflow: https://34.139.115.254:8080
- Authentication Service: https://34.23.189.28:8000
- Fast API_1: https://34.23.189.28:8000
- Fast API_2: https://34.73.83.102:8001
- Streamlit Application: https://35.229.36.63:8051

## Problem Statement 
Build an end-to-end pipeline using Airflow to automate the data extraction and storing of content of pdf files into Snowflake
 
## Project Goals
Build an API services using Fast API for getting input file and trigger airflow pipeline, and another one is to retrieve data from snowflake.
Develop an end-to-end pipeline for automated extraction and loading of PDF metadata into Snowflake via Airflow, Create a user-friendly Streamlit interface.
Dockerize API services and streamlit application.

## Technologies Used
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)
[![FastAPI](https://img.shields.io/badge/fastapi-109989?style=for-the-badge&logo=FASTAPI&logoColor=white)](https://fastapi.tiangolo.com/)
[![Amazon AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-%232496ED?style=for-the-badge&logo=Docker&color=blue&logoColor=white)](https://www.docker.com)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![MongoDB](https://img.shields.io/badge/MongoDB-%234169E1?style=for-the-badge&logo=MongoDB&logoColor=%234169E1&color=black)](https://www.postgresql.org)
[![Snowflake](https://img.shields.io/badge/snowflake-%234285F4?style=for-the-badge&logo=snowflake&link=https%3A%2F%2Fwww.snowflake.com%2Fen%2F%3F_ga%3D2.41504805.669293969.1706151075-1146686108.1701841103%26_gac%3D1.160808527.1706151104.Cj0KCQiAh8OtBhCQARIsAIkWb68j5NxT6lqmHVbaGdzQYNSz7U0cfRCs-STjxZtgPcZEV-2Vs2-j8HMaAqPsEALw_wcB&logoColor=white)
](https://www.snowflake.com/en/?_ga=2.41504805.669293969.1706151075-1146686108.1701841103&_gac=1.160808527.1706151104.Cj0KCQiAh8OtBhCQARIsAIkWb68j5NxT6lqmHVbaGdzQYNSz7U0cfRCs-STjxZtgPcZEV-2Vs2-j8HMaAqPsEALw_wcB)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](https://streamlit.io/)

## Pre requisites
1. Python Knowledge
2. Snowflake Account
3. AWS S3 bucket
4. Docker Desktop
5. FastaAPI knowledge
6. MongoDB database knowledge
7. Postman knowledge
8. Stremlit implementation
9. Airflow pipeline knowledge
10. Google Cloud Platform account and hosting knowledge

## Project Structure
```
📦 
├─ .gitignore
├─ README.md
├─ airflow
│  ├─ Dockerfile
│  ├─ dags
│  │  ├─ pdf_dags.py
│  │  └─ scripts
│  │     ├─ extraction_pypdf.py
│  │     ├─ load_data.py
│  │     ├─ models
│  │     │  └─ pdf_model.py
│  │     └─ validate.py
│  ├─ db
│  │  └─ airflow.db
│  ├─ docker-compose.yaml
│  ├─ grobid
│  │  ├─ config.json
│  │  ├─ grobid_extraction.py
│  │  └─ requirements.txt
│  └─ requirements.txt
├─ back-end
│  ├─ api_1
│  │  ├─ Dockerfile
│  │  ├─ api_1.py
│  │  ├─ docker-compose.yaml
│  │  ├─ env
│  │  ├─ gitignore
│  │  ├─ m_database.py
│  │  └─ requirements.txt
│  ├─ api_2
│  │  ├─ Dockerfile
│  │  ├─ api2.py
│  │  ├─ docker-compose.yaml
│  │  ├─ m_database.py
│  │  ├─ mongo_extracted_data.py
│  │  ├─ requirements.txt
│  │  └─ snowflake_helper.py
│  └─ app
│     ├─ Dockerfile
│     ├─ database.py
│     ├─ docker-compose.yaml
│     ├─ main.py
│     ├─ oauth2.py
│     ├─ requirements.txt
│     ├─ routers
│     │  └─ auth.py
│     ├─ schemas.py
│     ├─ serializers
│     │  └─ userSerializers.py
│     └─ utils.py
└─ front-end
   ├─ requirements.txt
   └─ src
      ├─ main.py
      └─ pages
         ├─ account
         │  └─ account.py
         ├─ auth_pages
         │  ├─ auth_page.py
         │  ├─ login.py
         │  └─ signup.py
         ├─ navbar
         │  └─ navigation.py
         └─ upload
            └─ file_upload.py
```
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)

## How to run Application Locally
1. Clone repository
2. Go to path each folder where requirementss.txt is present.
3. Create configurations.example files where requirements is present and add your respsctive creadentials for
   - AWS S3 bucket:
     Access key = "",
     Secret Key = "",
     Bucket = ""
 
   - MongoDB:
     mongo-username= "",
     mongo-password = "",
     mongo-cluster = ""
 
   - Snowflake:
     user = "",
     password = "",
     account = "",
     warehouse = "",
     database = "",
     schema = ""
 
   - Airflow:
    airflow_un = "",
    airflow_pas = ""
5. Create code env and activate it.
6. run pip install -r requirements.txt
7. Since each service is dockerised, just run docker compose up --build

## Project run outline

CodeLab - [Documentation](https://docs.google.com/document/d/1YvvKu38ZeIrlWY-Pgls1Gwes7ZaCuarZ1gx1VVb-qKI/edit#heading=h.iq9nlyp04yle) 

## References

- https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#sort=%40refreadingcurriculumyear%20descending
- https://pypdf.readthedocs.io/en/stable/
- https://github.com/kermitt2/grobid
- https://diagrams.mingrammer.com/
- https://aws.amazon.com/
- https://app.snowflake.com/
- https://www.sqlalchemy.org/

  
  Name | Contribution %| Contributions |
  --- |--- | --- |
  Anshul Chaudhary  | 35% | Streamlit, Airflow, Hosting, Docker|
  Agash Uthayasuriyan | 32% | API_1, Data extraction, Docker|
  Narayani Arun Patil | 33% | API_2, Authentication service, MongoDB setup, Docker|
