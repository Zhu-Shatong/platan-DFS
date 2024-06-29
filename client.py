import socket
import os
import math
import json


class FileSplitter:
    def split_file(self, filename, block_size=1024*1024):
        if not os.path.isfile(filename):
            raise FileNotFoundError(f"The file {filename} does not exist.")

        with open(filename, 'rb') as file:
            while True:
                block = file.read(block_size)
                if not block:
                    break
                yield block

    def get_number_of_blocks(self, filename, block_size=1024*1024):
        file_size = os.path.getsize(filename)
        return math.ceil(file_size / block_size)


class Client:
    def __init__(self, master_host='localhost', master_port=5000):
        """_summary_: 客户端类

        Args:
            master_host (str, optional):    主服务器地址. Defaults to 'localhost'.
            master_port (int, optional):    主服务器端口. Defaults to 5000.
        """
        self.master_address = (master_host, master_port)  # Master服务器地址

    def store_file(self, filename):
        """_summary_    存储文件

        Args:
            filename (_type_):  文件名
            data (_type_):  文件内容
        """

        splitter = FileSplitter()
        try:
            number_of_blocks = splitter.get_number_of_blocks(filename)
            print(f"Number of blocks: {number_of_blocks}")
        except FileNotFoundError as e:
            print(e)
            return

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 创建TCP套接字
        client.connect(self.master_address)  # 连接Master服务器

        # 获取块索引
        client.send(f"STORE::{filename}::{number_of_blocks}".encode(
            'utf-8'))  # 发送存储请求
        response = client.recv(40960).decode('utf-8')  # 接收响应
        block_info = json.loads(response)  # 解析块索引

        print(block_info)
        ###

        client.close()  # 关闭连接

        # 根据块索引存储块
        blocks = list(splitter.split_file(filename))
        for block in block_info['blocks']:
            block_id = block['blockID']
            primary = block['primary']

            print(
                f"Storing block {block_id} on {primary['host']}:{primary['port']}")

            self.store_block(primary['host'], primary['port'], filename,
                             block_id, blocks[block_id])
            # 存储副本
            for replica in block['replica']:
                self.store_block(
                    replica['host'], replica['port'], filename, block_id, blocks[block_id])

        print("File stored successfully.")

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
        client.send(f"RETRIEVE::{filename}::".encode('utf-8'))
        response = client.recv(40960).decode('utf-8')
        block_info = json.loads(response)  # 解析块索引
        ###

        client.close()

        # 根据块索引检索块
        data = bytearray()
        for block in block_info['blocks']:
            block_id = block['blockID']
            primary = block['primary']
            print(
                f"Retrieving block {block_id} from {primary['host']}:{primary['port']}")

            try:
                block_data = self.retrieve_block(
                    primary['host'], primary['port'], filename,  block_id)
                data.extend(block_data)
            except Exception as e:
                print(
                    f"Failed to retrieve block {block_id} from {primary['host']}:{primary['port']}. Error: {e}")
                # 尝试从副本服务器检索
                for replica in block['replica']:
                    try:
                        block_data = self.retrieve_block(
                            replica['host'], replica['port'], filename, block_id)
                        data.extend(block_data)
                        break
                    except Exception as e:
                        print(
                            f"Failed to retrieve block {block_id} from replica {replica['host']}:{replica['port']}. Error: {e}")

        with open(filename, 'wb') as file:
            file.write(data)

        print("File retrieved successfully.")

    def delete_file(self, filename):
        """_summary_    删除文件

        Args:
            filename (_type_): 文件名
        """
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(self.master_address)

        # 发送删除请求
        client.send(f"DELETE::{filename}::".encode('utf-8'))
        response = client.recv(40960).decode('utf-8')
        block_info = json.loads(response)  # 解析块索引
        ###
        client.close()

        for block in block_info['blocks']:
            block_id = block['blockID']
            primary = block['primary']
            print(
                f"Deleting block {block_id} from {primary['host']}:{primary['port']}")

            self.delete_block(
                primary['host'], primary['port'], filename,  block_id)

            for replica in block['replica']:
                self.delete_block(
                    replica['host'], replica['port'], filename, block_id)

        print("File deleted successfully.")

    def store_block(self, server_host, server_port, filename, block_id, block_data):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((server_host, server_port))
        client.send(
            f"STORE_BLOCK::{filename}_block_{block_id}".encode('utf-8'))
        response = client.recv(1024).decode('utf-8')
        if response == 'READY':

            print("start storing block")

            data_length = len(block_data)
            client.send(str(data_length).encode('utf-8'))  # 发送数据长度

            response = client.recv(1024).decode('utf-8')
            if response == 'LENGTH_RECEIVED':

                for i in range(0, len(block_data), 1024*256):
                    client.send(block_data[i:i+1024*256])

                print("block sent")

                ack = client.recv(1024).decode('utf-8')
                if ack == 'STORED':
                    print(
                        f"Block {block_id} stored successfully on {server_host}:{server_port}")
        client.close()

    def retrieve_block(self, server_host, server_port, filename, block_id, block_size=1024*1024):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((server_host, server_port))
        client.send(
            f"RETRIEVE_BLOCK::{filename}_block_{block_id}".encode('utf-8'))
        response = client.recv(1024).decode('utf-8')

        block_data = bytearray()
        if response == 'READY':
            client.send(b'READY')
            file_size = int(client.recv(1024).decode('utf-8'))  # 接收文件大小
            client.send(b'LENGTH_RECEIVED')
            while len(block_data) < file_size:
                packet = client.recv(1024*256)
                block_data.extend(packet)

        client.close()
        return block_data

    def delete_block(self, server_host, server_port, filename, block_id):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((server_host, server_port))
        client.send(
            f"DELETE_BLOCK::{filename}_block_{block_id}".encode('utf-8'))
        response = client.recv(1024).decode('utf-8')

        return


if __name__ == "__main__":

    client = Client()
    # client.store_file('1.rar')
    # client.retrieve_file('1.pdf')
    # client.delete_file('1.rar')
