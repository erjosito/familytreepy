import streamlit as st

def show_pyvis():
    # Verify authentication
    st.header(f"Welcome, {st.user.name}!")

    # Page content
    st.write("This is the Pyvis page content.")