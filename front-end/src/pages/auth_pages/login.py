import streamlit as st
import requests
import json

def login():
  with st.form(key='login', clear_on_submit=True):
    st.subheader(':red[Login]')
    username = st.text_input(':blue[Email]', placeholder='Enter Your Email')
    password = st.text_input(':blue[Password]', placeholder='Enter Your Password', type='password')
    btn1, btn2, btn3 = st.columns([3, 1, 3])
    with btn2:
      sub = st.form_submit_button('Login')
      
    if sub:
      if validate_username(username) and validate_password(password):
        url = 'http://34.23.189.28:8080/api/auth/login'
        # Data to be sent with the POST request (as a dictionary)
        payload = {'email': username, 'password': password}
        # Convert the data dictionary to JSON
        json_data = json.dumps(payload)
        # Making the POST request
        headers = {
          'Content-Type': 'application/json',  # Example header, adjust as needed
        }
        response = requests.post(url, headers=headers, data=json_data)
        print(response.json()["status"])
        if response.status_code == 200:
          st.session_state["auth_status"] = True
          st.session_state["token"] = response.json()["access_token"]
          st.session_state["role"] = "user"
          st.rerun()
        else: 
          st.error("Invalid Credential")
      
def validate_username(username):
  if username:
    return True
  else:
    st.warning('Enter an Username')
    return False

def validate_password(password):
  if password:
    return True
  else:
    st.warning('Enter Password')
    return False