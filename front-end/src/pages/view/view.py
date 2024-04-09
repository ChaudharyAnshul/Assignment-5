import streamlit as st
import pandas as pd
import requests
import json

def view():
  url = 'http://34.74.71.39:8001/get_data'
  payload = {"token": st.session_state["token"]}
  json_data = json.dumps(payload)
  headers = {
    'Content-Type': 'application/json',
  }
  response = requests.post(url, headers=headers, data=json_data)
  if response.status_code == 200:
    df = pd.DataFrame(response.json())
    st.write(df)
  else:
    st.error("Invalid Token - login again")
