import streamlit as st
from streamlit_navigation_bar import st_navbar
import pages as pg

# Asks for login if no user is logged in
if not st.user.is_logged_in:
    st.header("This app is private, please log in")
    if st.button("Log in with a Microsoft account"):
        st.login()
    st.stop()

# From this point the user is authenticated

# Navigation bar
st.set_page_config(initial_sidebar_state="collapsed")
pages = ["Home", "st-link-analysis", "pyvis", "Logout"]
styles = {
    "nav": {
        "background-color": "royalblue",
        "justify-content": "left",
    },
    "img": {
        "padding-right": "14px",
    },
    "span": {
        "color": "white",
        "padding": "14px",
    },
    "active": {
        "background-color": "white",
        "color": "var(--text-color)",
        "font-weight": "normal",
        "padding": "14px",
    }
}
options = {
    "show_menu": True,
    "show_sidebar": False,
}
page = st_navbar(
    pages,
    styles=styles,
    options=options,
)
# page = st_navbar(pages)
functions = {
    "Home": pg.show_home,
    "st-link-analysis": pg.show_st_link_analysis,
    "pyvis": pg.show_pyvis,
    "Logout": pg.logout
}
go_to = functions.get(page)
if go_to:
    go_to()
