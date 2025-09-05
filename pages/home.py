import streamlit as st

def show_home():
    st.header(f"Welcome, {st.user.name}!")
    st.write("Please select a graph implementation from the menu.")