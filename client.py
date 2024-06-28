import socket


class Client:
    def __init__(self, master_host='localhost', master_port=5000):
        """_summary_: 安全的客户端类

        Args:
            master_host (str, optional):    主服务器地址. Defaults to 'localhost'.
            master_port (int, optional):    主服务器端口. Defaults to 5000.
        """
        self.master_address = (master_host, master_port)  # Master服务器地址

    def store_file(self, filename, data):
        """_summary_    存储文件

        Args:
            filename (_type_):  文件名
            data (_type_):  文件内容
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 创建TCP套接字
        client.connect(self.master_address)  # 连接Master服务器

        # 获取块索引
        client.send(f"STORE::{filename}::{data}".encode())  # 发送存储请求
        response = client.recv(1024).decode()  # 接收响应
        block_ids = eval(response)  # 解析块索引
        ###

        client.close()  # 关闭连接

        # 根据块索引存储块
        for block_id, server_addresses in block_ids:
            for server_address in server_addresses:
                server_host, server_port = server_address.split(':')
                self.store_block(server_host, int(server_port), block_id, data)
        ###

        print("File stored successfully.")

    def store_block(self, host, port, block_id, data):
        """_summary_    存储块

        Args:
            host (_type_):  服务器地址
            port (_type_):  服务器端口
            block_id (_type_):  块ID
            data (_type_):  块数据
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((host, port))

        # 存储块
        client.send(f"STORE::{block_id}::{data}".encode())  # 发送存储请求
        response = client.recv(1024).decode()  # 接收响应
        ###

        client.close()

    def retrieve_file(self, filename):
        """_summary_    检索文件

        Args:
            filename (_type_): 文件名

        Returns:
            _type_:    文件内容
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(self.master_address)

        # 获取块索引
        client.send(f"RETRIEVE::{filename}::".encode())
        response = client.recv(1024).decode()
        blocks = eval(response)  # 解析块索引
        ###

        client.close()

        # 根据块索引检索块
        data = ""
        for block_id, server_addresses in blocks:
            for server_address in server_addresses:
                server_host, server_port = server_address.split(':')
                data += self.retrieve_block(server_host,
                                            int(server_port), block_id)
        ###

        print("File retrieved successfully.")
        return data

    def retrieve_block(self, host, port, block_id):
        """_summary_    检索块

        Args:
            host (_type_):  服务器地址
            port (_type_):  服务器端口
            block_id (_type_):  块ID

        Returns:
            _type_:     块数据
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((host, port))

        # 检索块
        client.send(f"RETRIEVE::{block_id}::".encode())
        response = client.recv(1024).decode()
        ###

        client.close()
        return response


if __name__ == "__main__":

    client = Client()
    # client.store_file('example.txt', 'This is the content of the file to be stored in blocks.')
    data = client.retrieve_file('example.txt')
    print(f"Retrieved data: {data}")
