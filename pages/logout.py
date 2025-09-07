import streamlit as st

# Log out
def logout(user_role='user'):
    if st.button("Log out"):
        st.logout()