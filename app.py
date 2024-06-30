import streamlit as st
from client import Client
import os
import tkinter as tk
from tkinter import filedialog

client = Client()


# 页面配置
st.set_page_config(page_title="Distributed File System", layout="wide")

# 选项卡
tabs = ["File Upload", "File Management", "Storage Server Status"]
tab = st.sidebar.selectbox("Navigation", tabs)

# 文件上传页面
if tab == "File Upload":
    st.title("File Upload")
    st.subheader("Upload your files to the Distributed File System")

    uploaded_file = st.file_uploader(
        "Choose a file or drag and drop here", type=None)

    if uploaded_file is not None:

        path = os.path.join(uploaded_file.name)
        with open(path, "wb") as f:
            f.write(uploaded_file.getvalue())

        st.success(f"File {uploaded_file.name} uploaded successfully!")
        with st.spinner(f"Storing {uploaded_file.name} in DFS..."):
            client.store_file(path)
        st.success(
            f"File {uploaded_file.name} stored in DFS successfully!")

        os.remove(path)


# 文件管理页面
elif tab == "File Management":
    st.title("File Management")
    st.subheader("Manage your files in the Distributed File System")

    files = client.get_master_file_namespace()
    if files:
        selected_file = st.selectbox("Select a file to manage", files)
        action = st.radio("Choose an action", ["Download", "Delete"])

        if action == "Download":
            if st.button("Select Folder to Download"):

                root = tk.Tk()
                root.withdraw()  # Hide the main tkinter window
                folder_path = filedialog.askdirectory()

                if folder_path:
                    with st.spinner(f"Downloading {selected_file} ..."):
                        client.retrieve_file(selected_file)
                    with open(os.path.join(folder_path, selected_file), "wb") as f:
                        with open(selected_file, "rb") as src:
                            f.write(src.read())
                    os.remove(selected_file)
                    st.success(
                        f"File {selected_file} downloaded successfully to {folder_path}")

        elif action == "Delete":
            if st.button("Delete"):
                with st.spinner(f"Deleting {selected_file} ..."):
                    client.delete_file(selected_file)
                st.success(
                    f"File {selected_file} deleted successfully from DFS")
                # 刷新文件列表
                files = client.get_master_file_namespace()

    else:
        st.warning("No files found in DFS")

# 存储服务器状态查询页面
elif tab == "Storage Server Status":
    st.title("Storage Server Status")
    st.subheader("Check the status of storage servers")

    server_status = client.get_storage_servers_status()

    for server, status in server_status.items():
        color = "green" if status else "red"
        st.markdown(
            f"<span style='color:{color}'>{server} - {'Online' if status else 'Offline'}</span>", unsafe_allow_html=True)
