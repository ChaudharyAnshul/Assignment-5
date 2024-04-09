import streamlit as st
from pages.auth_pages.auth_page import menu_login
from pages.navbar.navigation import tabs

st.set_page_config(page_title='Streamlit App', page_icon='🤧')

if "auth_status" not in st.session_state:
    st.session_state.auth_status = False

if not st.session_state["auth_status"]:
  menu_login()
else:
  tabs()
