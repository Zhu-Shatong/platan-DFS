# platan-DFS

1. Master 节点
   功能：

管理存储服务器上的块索引。
接收客户端的存储请求，并指定每个文件块存储的存储服务器。
保存每个文件块的存储位置。
实现：

启动一个 TCP 服务器，监听来自客户端的请求。
使用一个字典存储文件块的索引，格式类似于{filename: [(block_id, server_address), ...]}。
处理存储和读取请求。 2. 存储服务器
功能：

存储实际的数据块。
接收来自客户端的文件块存储请求和读取请求。
实现：

启动一个 TCP 服务器，监听来自客户端的请求。
使用一个字典存储数据块，格式类似于{block_id: data}。
处理存储和读取请求。 3. 客户端
功能：

将要存储的文件切割成固定大小的文件块。
向 Master 节点发送存储文件块的请求。
根据 Master 节点的指示，将文件块发送到指定的存储服务器。
根据文件的切分顺序从各存储服务器读取数据块，并拼接成原文件。
实现：

与 Master 节点和存储服务器建立 TCP 连接。
发送存储和读取请求。

分块存储：文件在客户端被分割成块，每个块被分配到不同的存储服务器。
块索引管理：Master 节点保存所有文件块的索引，并告知客户端块存储的位置。
数据传输：客户端负责将文件块传输到指定的存储服务器并从中读取数据块。

1. 容错机制
   依据：提高系统的可靠性，确保在某个存储服务器故障时，数据不会丢失，并且能够继续正常工作。

优化措施：

数据冗余：每个数据块不仅存储在一个存储服务器上，还需要复制到多个存储服务器上。可以使用常见的副本机制，如 3 副本策略。
心跳检测：Master 节点定期检测存储服务器的健康状态，如果检测到服务器故障，则重新分配该服务器上的数据块到其他可用服务器。
数据恢复：当检测到服务器故障时，Master 节点可以从其他副本中恢复数据，并分配到新的存储服务器。

2. 并发处理
   依据：提高系统的响应速度和吞吐量，支持多个客户端同时进行文件存储和读取操作。

优化措施：

线程池：使用线程池处理客户端请求，减少线程创建和销毁的开销。
异步 IO：使用异步 IO 提高网络 IO 的效率。

使用 Python 的配置文件模块（例如 configparser）来管理存储服务器的配置。

Master 节点发现异常后的处理和报警机制

使用生成器来避免一次性将整个文件读入内存

长度检查

删除机制

json 冷保存

状态正常才分配
