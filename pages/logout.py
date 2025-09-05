import streamlit as st

# Log out
def logout():
    if st.button("Log out"):
        st.logout()