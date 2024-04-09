import pandas as pd
import numpy as np
from dags.scripts.models.pdf_model import PDFDataModel
import re
import configparser
import boto3
from io import BytesIO, StringIO
import pandas as pd

config = configparser.ConfigParser()
config.read('/opt/airflow/dags/scripts/configuration.properties')

def find_bucket_key(s3_path):
  """
  This is a helper function that given an s3 path such that the path is of
  the form: bucket/key
  It will return the bucket and the key represented by the s3 path
  """
  s3_components = s3_path.split('/')
  bucket = s3_components[0]
  s3_key = ""
  if len(s3_components) > 1:
    s3_key = '/'.join(s3_components[1:])
  return bucket, s3_key

def split_s3_bucket_key(s3_path):
  """Split s3 path into bucket and key prefix.
  This will also handle the s3:// prefix.
  :return: Tuple of ('bucketname', 'keyname')
  """
  if s3_path.startswith('s3://'):
    s3_path = s3_path[5:]
  return find_bucket_key(s3_path)

def convertRomanToInt(s):
  map = {'I':1, 'II': 2, 'III':3}
  return map[s]

def validate_function(model, max_attempts=3, **kwargs):
  ''' function to validate the raw data, and then try to fix the data if any validation is hit '''
  attempts = 0
  if "curriculum_refresher_reading" not in kwargs:
    return None
  print("Validating {} --------------".format(kwargs["curriculum_refresher_reading"]))
  while attempts < max_attempts:
    try:
      m = model(**kwargs)
      print("Succesfully validated {}".format(kwargs["curriculum_refresher_reading"]))
      return m.model_dump()
    except Exception as e:
      for error in e.errors():
        field_name = error['loc'][0]
        msg = error["msg"]
        print("Attempting to fix the error {}".format(field_name))
        if field_name == "curriculum_refresher_reading":
          if "Test RR page" in msg:
            break
          else:
            kwargs["curriculum_refresher_reading"] = kwargs["curriculum_refresher_reading"].strip()
        elif field_name == "cfa_level":
          kwargs["cfa_level"] = convertRomanToInt(kwargs["cfa_level"])
        elif field_name in ["learning_outcomes"]:
          temp = kwargs[field_name].strip(" ")
          temp_list = [s.strip() for s in temp.split("\n")]
          temp = ' '.join(temp_list)
          temp = re.sub(r'\s+', ' ', temp)
          temp = temp.replace("□", "", 1)
          temp = temp.replace("□", ";")
          kwargs[field_name] = temp
      attempts += 1
      print("Retrying running model...")
  print("could not fix csv entry for {}".format(kwargs['curriculum_refresher_reading']))


def cleanDataPDF(s3_uri):
  ''' function to pass values to validator and clean pdf data'''
  print("------- Starting validation -------")
  print("Validating data using PDFDataModel")
  
  # s3 client
  print("Loading file from S3")
  s3_client = boto3.client('s3', 
    aws_access_key_id=config['AWS']['aws_access_key_id'], 
    aws_secret_access_key=config['AWS']['aws_secret_access_key']
  )
  
  bucket, key_s3 = split_s3_bucket_key(s3_uri)
  
  print(bucket, key_s3)
  response = s3_client.get_object(Bucket=bucket, Key=key_s3)

  object_content = response['Body'].read()
  
  pdfFileObj = BytesIO(object_content)
  
  df = pd.read_csv(pdfFileObj, sep="\t")
  
  df = df.replace(np.nan, None)
  result_list = []
  for _, row in df.iterrows():
    res = validate_function(PDFDataModel, max_attempts=4, **row.to_dict())
    if res:
      result_list.append(res)

  df_clean = pd.DataFrame(result_list)
  print("------- Ending validating dataframe data -------")

  print("-------Starting Writing to CSV -------")
  csv_buffer = StringIO()
  df_clean.to_csv(csv_buffer, sep="\t", index=False)
  
  s3_key = "clean_csv_file/" + str(key_s3.split("/")[1].split(".")[0]) + ".csv"
  
  csv_buffer_encode = BytesIO(csv_buffer.getvalue().encode())
  
  s3_client.upload_fileobj(csv_buffer_encode, bucket, s3_key)
  
  file_name = str(key_s3.split("/")[1])
  print("------- Ending Writing to CSV -------")
  
  print("------- Ending validation -------")
  return file_name
