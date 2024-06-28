import socket
import threading
import configparser
import time


class StorageServer:
    def __init__(self, host, port, master_host, master_port):
        """_summary_: 存储服务器类

        Args:
            host (str, optional): 服务器地址
            port (int, optional): 服务器端口
        """
        self.server_address = (host, port)
        self.data_store = {}
        self.master_address = (master_host, master_port)
        self.heartbeat_interval = 5

    def handle_client(self, client_socket):
        """_summary_: 处理客户端请求

        Args:
            client_socket (_type_): 客户端套接字
        """
        request = client_socket.recv(1024).decode()
        print(f"Received request: {request}")
        command, block_id, data = request.split('::')

        if command == 'STORE':
            self.data_store[block_id] = data
            client_socket.send(b'STORED')
        elif command == 'RETRIEVE':
            data = self.data_store.get(block_id, 'NOT_FOUND')
            client_socket.send(data.encode())

        client_socket.close()

    def send_heartbeat(self):
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.master_address)
                s.send(
                    f"HEARTBEAT::{self.server_address[0]}::{self.server_address[1]}".encode())
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
        storage_server = StorageServer(host, port, master.split(':')[
                                       0], int(master.split(':')[1]))
        server_thread = threading.Thread(target=storage_server.start)
        server_thread.daemon = True
        server_thread.start()
        print(f"{server_name} started on {host}:{port}")


if __name__ == "__main__":
    start_storage_servers('servers.conf')
    # 保持主线程运行，以防子线程终止
    while True:
        pass
