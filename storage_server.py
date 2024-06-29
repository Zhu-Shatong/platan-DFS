import socket
import threading
import configparser
import time
import os


class StorageServer:
    def __init__(self, host, port, master_host, master_port, storage_path):
        """_summary_: 存储服务器类

        Args:
            host (str, optional): 服务器地址
            port (int, optional): 服务器端口
        """
        self.server_address = (host, port)
        self.master_address = (master_host, master_port)
        self.storage_path = storage_path
        os.makedirs(self.storage_path, exist_ok=True)

        self.heartbeat_interval = 5

    def handle_client(self, client_socket):
        request = client_socket.recv(1024).decode('utf-8')
        print(f"Received request: {request}")
        command, block_id = request.split('::')

        if command.startswith("STORE_BLOCK"):
            client_socket.send("READY".encode('utf-8'))
            data = bytearray()

            data_length = int(client_socket.recv(
                1024).decode('utf-8'))  # 接收数据长度
            client_socket.send("LENGTH_RECEIVED".encode('utf-8'))

            print(f"1 Storing block {block_id}")

            while len(data) < data_length:
                packet = client_socket.recv(1024*256)
                print("Received packet")
                data.extend(packet)
            print("done")

            # 检查数据长度
            if len(data) != data_length:
                client_socket.send("ERROR".encode('utf-8'))
                print("Error storing block")
                return

            with open(os.path.join(self.storage_path, block_id), 'wb') as file:
                file.write(data)

            client_socket.send("STORED".encode('utf-8'))

        elif command.startswith("RETRIEVE_BLOCK"):
            block_file = os.path.join(self.storage_path, block_id)
            if os.path.exists(block_file):
                client_socket.send("READY".encode('utf-8'))
                ack = client_socket.recv(1024).decode('utf-8')
                if ack == 'READY':
                    file_size = os.path.getsize(block_file)
                    client_socket.send(
                        str(file_size).encode('utf-8'))  # 发送文件大小
                    response = client_socket.recv(1024).decode('utf-8')
                    if response == 'LENGTH_RECEIVED':
                        with open(block_file, 'rb') as file:
                            while (chunk := file.read(1024*256)):
                                client_socket.send(chunk)
            else:
                client_socket.send("NOT_FOUND".encode('utf-8'))

        elif command.startswith("DELETE_BLOCK"):
            block_file = os.path.join(self.storage_path, block_id)
            if os.path.exists(block_file):
                os.remove(block_file)
                client_socket.send("DELETED".encode('utf-8'))
            else:
                client_socket.send("NOT_FOUND".encode('utf-8'))

        client_socket.close()

    def send_heartbeat(self):
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.master_address)
                s.send(
                    f"HEARTBEAT::{self.server_address[0]}::{self.server_address[1]}".encode('utf-8'))
                s.close()
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            time.sleep(self.heartbeat_interval)

    def start(self):
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.server_address)
        server.listen(5)
        print(f"Storage server listening on {self.server_address}")

        while True:
            client_socket, addr = server.accept()
            client_handler = threading.Thread(
                target=self.handle_client, args=(client_socket,))
            client_handler.start()


def start_storage_servers(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)

    servers = config['servers']
    master = config['master']['server']
    for server_name, address in servers.items():
        host, port = address.split(':')
        port = int(port)
        storage_path = config['paths'][server_name]
        storage_server = StorageServer(host, port, master.split(':')[
                                       0], int(master.split(':')[1]), storage_path)
        server_thread = threading.Thread(target=storage_server.start)
        server_thread.daemon = True
        server_thread.start()
        print(f"{server_name} started on {host}:{port}")


if __name__ == "__main__":
    start_storage_servers('servers.conf')
    # 保持主线程运行，以防子线程终止
    while True:
        pass
