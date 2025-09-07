import streamlit as st
from azure.storage.blob import BlobServiceClient
import json

def get_allowed_users():
    # Constants
    azure_storage_container = "familytreeconfig"
    azure_storage_blob = "allowed_users.json"
    # Get storage account/key from secrets.toml
    try:
        azure_storage_account = st.secrets['storage']["azure_storage_account"]
        azure_storage_key = st.secrets['storage']["azure_storage_key"]
    except Exception as e:
        st.error(f"Error getting Azure Storage credentials from secrets: {e}")
        st.stop()
    # Get the allowed users JSON file from Azure Storage
    try:
        blob_service_client = BlobServiceClient(account_url=f"https://{azure_storage_account}.blob.core.windows.net", credential=azure_storage_key)
        blob_client = blob_service_client.get_blob_client(container=azure_storage_container, blob=azure_storage_blob)
        allowed_users_json = blob_client.download_blob().readall()
        allowed_users = json.loads(allowed_users_json)
        return allowed_users
    except Exception as e:
        st.error(f"Error getting allowed users from secrets: {e}")
        return []
    
def get_user_role(email, allowed_users):
    if 'users' in allowed_users:
        for user in allowed_users['users']:
            if user['email'] == email:
                return user['role']
    return None

def add_user(email, role='user'):
    allowed_users = get_allowed_users()
    if 'users' not in allowed_users:
        allowed_users['users'] = []
    # Check if user already exists
    for user in allowed_users['users']:
        if user['email'] == email:
            st.warning(f"User '{email}' already exists with role '{user['role']}'.")
            return
    allowed_users['users'].append({'email': email, 'role': role})
    # Save back to Azure Storage
    save_allowed_users(allowed_users)

def remove_user(email):
    allowed_users = get_allowed_users()
    if 'users' not in allowed_users:
        st.warning("No users to remove.")
        return
    # Check if user exists
    user_found = False
    for user in allowed_users['users']:
        if user['email'] == email:
            allowed_users['users'].remove(user)
            user_found = True
            break
    if not user_found:
        st.warning(f"User '{email}' not found.")
        return
    # Save back to Azure Storage
    save_allowed_users(allowed_users)

# Save user list back to Azure Storage
def save_allowed_users(allowed_users):
    azure_storage_container = "familytreeconfig"
    azure_storage_blob = "allowed_users.json"
    try:
        azure_storage_account = st.secrets['storage']["azure_storage_account"]
        azure_storage_key = st.secrets['storage']["azure_storage_key"]
        blob_service_client = BlobServiceClient(account_url=f"https://{azure_storage_account}.blob.core.windows.net", credential=azure_storage_key)
        blob_client = blob_service_client.get_blob_client(container=azure_storage_container, blob=azure_storage_blob)
        blob_client.upload_blob(json.dumps(allowed_users), overwrite=True)
        st.success("Allowed users updated.")
    except Exception as e:
        st.error(f"Error saving allowed users: {e}")

def show_admin(user_role='user'):
    st.header(f"Welcome, {st.user.name} ({user_role})!")
    try:
        allowed_users = get_allowed_users()
    except Exception as e:
        st.error(f"Error getting allowed users: {e}")
        allowed_users = {}
    if not 'users' in allowed_users or len(allowed_users['users']) == 0:
        st.warning("No users are currently authorized to use this app.")
    else:
        st.write("List of users authorized to use this app:")
        # Show a streamlit table of allowed users
        st.table(allowed_users.get('users', []))
    # Add buttons to add/remove users in two columns
    left, right = st.columns(2)
    with left:
        st.subheader("Add user")
        new_user_email = st.text_input("User email to add:")
        new_user_role = st.selectbox("Select role for new user:", options=['user', 'admin'])
        if st.button("Add user"):
            if new_user_email:
                add_user(new_user_email, new_user_role)
                st.rerun()
            else:
                st.error("Please enter a valid email address.")
    with right:
        st.subheader("Remove user")
        remove_user_email = st.text_input("User email to remove:")
        if st.button("Remove user"):
            if remove_user_email:
                remove_user(remove_user_email)
                st.rerun()
            else:
                st.error("Please enter a valid email address.")