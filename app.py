import streamlit as st
from streamlit_navigation_bar import st_navbar
import pages as pg

# Asks for login if no user is logged in
if not st.user.is_logged_in:
    st.header("This app is private, please log in")
    if st.button("Log in with a Microsoft account"):
        st.login()
    st.stop()

# Verify that the user's email is in the list of allowed users (stored in Azure Storage as a JSON file) and get the level of privilege (user or admin) for that user.
allowed_users = pg.get_allowed_users()
user_role = pg.get_user_role(st.user.email, allowed_users)
if user_role:
    # st.success(f"User '{st.user.email}' is authorized to use this app. Privilege level: {user_role}")
    pass
else:
    # st.error("No users are authorized to use this app. Please contact the administrator.")
    if st.button("Log out"):
        st.logout()
    st.stop()

if user_role == 'admin':
    pages = ["Edit", "View", "Admin", "Logout"]
elif user_role == 'user':
    pages = ["View", "Logout"]
else:
    st.error(f"User role '{user_role}' is not recognized. Please contact the administrator.")
    if st.button("Log out"):
        st.logout()
    st.stop()

# From this point the user is authenticated and authorized

# Navigation bar
logo_path = "treelogo01.svg"
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
    "hide_nav": True,
    "fix_shadow": True
}
page = st_navbar(
    pages,
    logo_path=logo_path,
    styles=styles,
    options=options
)
functions = {
    "Home": pg.show_home,
    "Edit": pg.show_st_link_analysis,
    "View": pg.show_pyvis,
    # "View": pg.show_graphviz,
    "Admin": pg.show_admin,
    "Logout": pg.logout
}
go_to = functions.get(page)

# st.header(f"Welcome to root, {st.user.name}!")
# st.write("Please select a graph implementation from the menu.")

if go_to:
    go_to(user_role=user_role)
