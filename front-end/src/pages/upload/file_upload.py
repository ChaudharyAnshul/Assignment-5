import streamlit as st
import json
import requests

def file_uploader():
  with st.form(key='upload', clear_on_submit=True):
    uploaded_file = st.file_uploader("Choose a PDF file to Extract and load data", type=["pdf"], accept_multiple_files=False, key="file_upload")
    
    btn1, btn2, btn3 = st.columns([4, 2, 4])
  
    with btn2:
      sub = st.form_submit_button("Upload file")
    
  if sub:  
    if uploaded_file:
      url = 'http://35.227.95.162:8000/upload'
      # Data to be sent with the POST request (as a dictionary)
      payload = {"token": st.session_state["token"]}
      files = {'file': uploaded_file}

      # Making the POST request
      headers = {
        'Content-Type': 'application/json',  # Example header, adjust as needed
      }
      response = requests.post(url, headers=headers, data=payload, files=files)
      if response.status_code == 200:
        st.success("Uploaded File")
      else:
        st.error("Token Expored login again")
    else:
      st.error("Please select a file before uploading")
      
  with st.form(key='upload_s3_uri', clear_on_submit=True):
    st.subheader(':red[S3 URI]')
    
    s3_uri = st.text_input(':blue[S3 URI]', placeholder='Enter Your URI')
    
    btn1, btn2, btn3 = st.columns([4, 2, 4])
  
    with btn2:
      sub = st.form_submit_button("Upload file")
    
  if sub:  
    if s3_uri:
      url = 'http://35.227.95.162:8000/trigger_airflow'
      token = st.session_state["token"]

      url_with_params = f"{url}?token={token}&s3_location={s3_uri}"

      response = requests.post(url_with_params)
      print(response.text)
      if response.status_code == 200:
        st.success("Airflow Dags are triggered! - view the data after some time")
      else:
        st.error("Token Expored login again")
    else:
      st.error("Please select a file before uploading")
      