import socket
import threading
import time
import configparser
import random
import json


class MasterServer:
    def __init__(self, host, port, config_file='servers.conf'):
        """_summary_: Master服务器类

        Args:
            host (str, optional):   主服务器地址. Defaults to 'localhost'.
            port (int, optional):   主服务器端口. Defaults to 5000.
        """
        self.server_address = (host, port)  # Master服务器地址
        self.servers = self.load_servers(config_file)

        self.metadata = {"fileMetadata": []}

        self.heartbeat_timeout = 10  # 心跳检查间隔
        self.server_status = {server: True for server in self.servers}  # 健康状态
        self.last_heartbeat = {server: time.time() for server in self.servers}
        
    def _parse_server_address(self, server):
        host, port = server.split(':')
        return {"host": host, "port": int(port)}

    def load_servers(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return [address for address in config['servers'].values()]

    def heartbeat_check(self):
        """_summary_    心跳检查
        """
        while True:
            current_time = time.time()
            for server, last_time in self.last_heartbeat.items():
                if current_time - last_time > self.heartbeat_timeout:
                    self.server_status[server] = False
                    print(f"Server {server} is down.")
            time.sleep(self.heartbeat_timeout)

    def handle_client(self, client_socket):
        """_summary_    处理客户端请求

        Args:
            client_socket (_type_): 客户端套接字
        """
        request = client_socket.recv(40960).decode('utf-8')
        command, filename, *args = request.split('::')

        if command == 'STORE':
            num_blocks = int(args[0])

            file_info = {
                "fileID": filename,
                "blocks": []
            }

            for block_id in range(num_blocks):
                primary = random.choice(self.servers)
                replica = random.choice(
                    [server for server in self.servers if server != primary])
                block_info = {
                    "blockID": block_id,
                    "primary": self._parse_server_address(primary),
                    "replica": [self._parse_server_address(replica)]
                }
                file_info["blocks"].append(block_info)

            self.metadata["fileMetadata"].append(file_info)

            print(self.metadata)

            client_socket.send(json.dumps(file_info).encode('utf-8'))

        elif command == 'RETRIEVE':
            file_info = next(
                (file_meta for file_meta in self.metadata["fileMetadata"] if file_meta["fileID"] == filename), None)
            if file_info:
                client_socket.send(json.dumps(file_info).encode('utf-8'))
            else:
                client_socket.send(json.dumps(
                    {"error": "File not found"}).encode('utf-8'))

        elif command == 'HEARTBEAT':
            host = filename
            port = int(args[0])
            server_address = f"{host}:{port}"
            self.server_status[server_address] = True
            self.last_heartbeat[server_address] = time.time()

        client_socket.close()

    def start(self):
        heartbeat_thread = threading.Thread(target=self.heartbeat_check)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.server_address)
        server.listen(5)
        print(f"Master server listening on {self.server_address}")

        while True:
            client_socket, addr = server.accept()
            client_handler = threading.Thread(
                target=self.handle_client, args=(client_socket,))
            client_handler.start()


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('servers.conf')

    master_config = config['master']['server']

    master = MasterServer(master_config.split(
        ':')[0], int(master_config.split(':')[1]))
    master.start()
