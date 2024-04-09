import streamlit as st
from streamlit_option_menu import option_menu
from pages.auth_pages.signup import sign_up
from pages.auth_pages.login import login


def menu_login():
    ''' navigation menu for login/signup '''
    # st.session_state["login_menu"] = "Login"
    login_menu = option_menu(None, ["Login", "Sign Up"], 
        icons=['person-fill', "person-plus-fill"], 
        menu_icon="cast", 
        key='login_menu',
        default_index=0, 
        orientation="horizontal"
    )

    login_menu

    if st.session_state["login_menu"] == "Login" or st.session_state["login_menu"] == None:
        login()
    elif st.session_state["login_menu"] == "Sign Up":
        sign_up()
