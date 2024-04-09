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
    st.subheader("Apply Filter")
    level = st.text_input(':blue[Level]', placeholder='Enter level')
    topic = st.text_input(':blue[Curriculum Topic]', placeholder='Curriculum Topic')
    year = st.text_input(':blue[Curriculum Year]', placeholder='Curriculum Year')
    rr = st.text_input(':blue[Curriculum RR]', placeholder='Curriculum RR')
    
    btn = st.button("Filter")
    
    if btn:
      filtered_df = df
      if level:
        filtered_df = filtered_df[filtered_df["CFA_LEVEL"]==int(level)]
      if year:
        filtered_df = filtered_df[filtered_df["CURRICULUM_YEAR"]==int(year)]
      if rr:
        filtered_df = filtered_df[filtered_df["CURRICULUM_REFRESHER_READING"]==rr]
      if topic:
        filtered_df = filtered_df[filtered_df["CURRICULUM_TOPIC"]==topic]
      st.write(filtered_df)
  else:
    st.error("Invalid Token - login again")
