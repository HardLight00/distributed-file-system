import asyncio
import json
import os
import sys
from functools import wraps

import websockets

from node import Node


class DataNode(Node):
    MAX_SPACE = 2 * 2 ** 30  # 2Gb

    def _response(func):
        @wraps(func)
        def wrapper(*args):
            try:
                return {"body": func(*args)}
            except OSError or FileExistsError or FileNotFoundError as error:
                return {"error": type(error).__name__}

        return wrapper

    async def __init_connection(self):
        await self.connect()

    def __init__(self, net_info, master_net_info):
        super().__init__()

        self.data = []
        self.host, self.port = net_info
        self.m_host, self.m_port = master_net_info
        # TODO for production change to "f'{os.path.abspath(os.curdir)}/{self.host}:{self.port}'"
        self.ROOT_FOLDER = f'{os.path.abspath(os.curdir)}/files/{self.host}:{self.port}'

        self.fill_data()
        asyncio.run(self.__init_connection())

    async def connect(self):
        uri = f"ws://{self.m_host}:{self.m_port}"
        async with websockets.connect(uri) as websocket:
            await websocket.send(f'connect {self.host}:{self.port}')

    def fill_data(self):
        if os.path.isdir(self.ROOT_FOLDER):
            for file in os.listdir(self.ROOT_FOLDER):
                if os.path.isfile(os.path.join(self.ROOT_FOLDER, file)):
                    self.data.append(file)
        else:
            os.mkdir(self.ROOT_FOLDER)

    @_response
    def ping(self):
        return 'I\'m alive'

    @_response
    def read(self, chunk_name):
        try:
            chunk_path = f'{self.ROOT_FOLDER}/{chunk_name}'
            with open(chunk_path, 'r') as file:
                return file.read()
        except ValueError:
            raise FileNotFoundError

    @_response
    def write(self, chunk_name, chunk_file):
        try:
            self.data.index(chunk_name)
        except ValueError:
            chunk_path = f'{self.ROOT_FOLDER}/{chunk_name}'
            with open(chunk_path, 'w') as file:
                print('writing: ', chunk_file)
                file.write(chunk_file)
                self.data.append(chunk_name)
                print(self.data)
        else:
            raise FileExistsError

    @_response
    def delete(self, chunk_name):
        try:
            self.data.remove(chunk_name)
            os.remove(f'{self.ROOT_FOLDER}/{chunk_name}')
        except ValueError:
            pass

    @_response
    def get_free_space(self):
        engaged_size = self.get_dir_size(self.ROOT_FOLDER)
        return self.MAX_SPACE - engaged_size

    def get_dir_size(self, directory):
        total_sum = 0
        for file in os.listdir(directory):
            if os.path.isdir(file):
                total_sum += self.get_dir_size(directory)
            else:
                total_sum += os.path.getsize(f'{self.ROOT_FOLDER}/{file}')
        return total_sum

    @_response
    def get_data(self):
        return self.data

    command_map = {
        'ping': ping,
        'free': get_free_space,
        'read': read,
        'write': write,
        'delete': delete,
        'list': get_data
    }


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception('Incorrect number of arguments')
    master = sys.argv[1].split(':')
    node = sys.argv[2].split(':')
    node_host, node_port = node

    data_node = DataNode(node, master)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = websockets.serve(data_node.execute, node_host, node_port)
    asyncio.get_event_loop().run_until_complete(server)
    asyncio.get_event_loop().run_forever()

