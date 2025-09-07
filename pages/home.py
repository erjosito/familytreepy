import streamlit as st

def show_home(user_role='user'):
    st.header(f"Welcome, {st.user.name} ({user_role})!")
    st.write("Please select a graph implementation from the menu.")