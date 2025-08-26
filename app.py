import streamlit as st

def login_screen():
    st.header("This app is private.")
    st.subheader("Please log in.")
    st.button("Log in with a Microsoft account", on_click=st.login)

if not st.user.is_logged_in:
    login_screen()
else:
    st.user
