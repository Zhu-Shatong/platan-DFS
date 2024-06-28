import socket
import threading
import time
import configparser


class MasterServer:
    def __init__(self, host, port, config_file='servers.conf'):
        """_summary_: Master服务器类

        Args:
            host (str, optional):   主服务器地址. Defaults to 'localhost'.
            port (int, optional):   主服务器端口. Defaults to 5000.
        """
        self.server_address = (host, port)  # Master服务器地址
        self.block_index = {}  # 块索引
        self.servers = self.load_servers(config_file)
        self.heartbeat_timeout = 10  # 心跳检查间隔
        self.server_status = {server: True for server in self.servers}  # 健康状态
        self.last_heartbeat = {server: time.time() for server in self.servers}

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
        request = client_socket.recv(1024).decode()
        command, *params = request.split('::')

        if command == 'STORE':
            filename, data = params
            block_ids = self.store_file(filename, data)
            client_socket.send(block_ids.encode())
        elif command == 'RETRIEVE':
            filename, blank = params
            blocks = self.retrieve_file(filename)
            client_socket.send(blocks.encode())
        elif command == 'HEARTBEAT':
            host, port = params
            server_address = f"{host}:{port}"
            self.server_status[server_address] = True
            self.last_heartbeat[server_address] = time.time()

        client_socket.close()

    def store_file(self, filename, data):
        blocks = [data[i:i+1024] for i in range(0, len(data), 1024)]  # 将文件分块
        block_ids = []

        for i, block in enumerate(blocks):  # 存储块
            server_addresses = [self.servers[j %
                                             len(self.servers)] for j in range(i, i+3)]  # 3副本
            block_id = f"{filename}_block_{i}"
            block_ids.append((block_id, server_addresses))
            self.block_index[block_id] = server_addresses

        return str(block_ids)

    def retrieve_file(self, filename):
        blocks = [block for block in self.block_index.keys()
                  if block.startswith(filename)]
        return str(blocks)

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
