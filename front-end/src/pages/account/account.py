import streamlit as st

def account():
  button_clicked = st.button("Log Out")
  
  if button_clicked:
    del st.session_state['token']
    st.session_state["auth_status"] = False
    del st.session_state["role"]
    st.rerun()