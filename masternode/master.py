import asyncio
import os
import random as rm
from functools import wraps

import websockets

from masternode.directorytree import DirectoryTree
from node import Node


def split(word):
    return [char for char in word]


def generate_chunk_name(name_size=10):
    possible_chars = split('abcdefghijkmnlopqrstuvwxyz')
    possible_numbers = split('0123456789')
    return rm.choice(possible_chars) + ''.join([rm.choice(possible_chars + possible_numbers) for i in range(name_size)])


class Master(Node):
    stores = []
    dir_tree = DirectoryTree()
    dir_to_files = {'/': []}
    file_to_chunks = {}
    chunk_to_replicas = {}

    # TODO Delete root path, delete_all etc
    def __init__(self, root_path):
        super().__init__()
        self.root_path = root_path
        self.delete_all(root_path)

    def _file_not_found(func):
        @wraps(func)
        def wrapper(self, *args):
            filename = args[0]
            try:
                self.dir_to_files.get(self.get_current()).index(filename)
            except ValueError:
                raise FileNotFoundError('File does not exist')
            return func(self, *args)

        return wrapper

    def _file_already_exist(func):
        @wraps(func)
        def wrapper(self, *args):
            filename = args[0]
            try:
                self.dir_to_files.get(self.get_current()).index(filename)
                raise FileExistsError('Already exist file with the same name')
            except ValueError:
                pass
            return func(self, *args)

        return wrapper

    def _response(func):
        @wraps(func)
        def wrapper(*args):
            try:
                result = func(*args)
                if result is None:
                    result = 'success'
                return {"body": result}
            except OSError or FileExistsError or FileNotFoundError as error:
                return {"error": type(error).__name__ + ': ' + str(error)}

        return wrapper

    bytes_converter = [
        (30, 'Gb'),
        (20, 'Mb'),
        (10, 'Kb')
    ]

    def convert_to_size(self, chunk_num):
        total_bytes = chunk_num * self.CHUNK_SIZE
        for power, symbol in self.bytes_converter:
            if total_bytes // 2 ** power > 0:
                size = str(total_bytes * 100 // 2 ** power / 100) + f' {symbol}'
                break
        else:
            size = str(total_bytes) + ' Bytes'
        return size

    def get_alive_stores(self):
        return [store for store in filter(lambda store: store[2], self.stores)]

    def get_stores(self):
        alive_stores = self.get_alive_stores()
        if len(alive_stores) < self.REPLICAS_NUM:
            return [(host, port) for (host, port, _) in alive_stores]

        stores = []
        while len(stores) < self.REPLICAS_NUM:
            host, port, is_alive = rm.choice(alive_stores)
            for s_host, s_port in stores:
                if host == s_host and port == s_port:
                    break
            else:
                stores.append((host, port))

        return stores

    def delete_all(self, path):
        dir_list = os.listdir(path)
        for directory in dir_list:
            abs_path = f'{path}/{directory}'
            if os.path.isfile(abs_path):
                os.remove(abs_path)
            elif os.path.isdir(abs_path):
                self.delete_all(abs_path)
                os.rmdir(abs_path)

    def get_current(self):
        return self.dir_tree.current.path

    # def get_abs_path(self, filename=''):
    #     abs_path = self.root_path
    #     current_path = self.get_current()
    #     if current_path != '/':
    #         abs_path += f'/{current_path}'
    #     if filename != '':
    #         abs_path += f'/{filename}'
    #     return abs_path

    async def send_delete(self, chunk, host, port):
        uri = f'ws://{host}:{port}'
        async with websockets.connect(uri) as websocket:
            await websocket.send(f'delete {chunk}')

    async def ask_confirmation(self, action, *params):
        uri = f'ws://{self.remote_address}'
        async with websockets.connect(uri) as websocket:
            await websocket.send(f'Are you sure that you want to {action} {str(*params)}')
            return await websocket.recv()

    @_response
    def connect(self, storage_net_info):
        storage_host, storage_port = storage_net_info.split(':')
        for storage in self.stores:
            host, port, is_alive = storage
            if host == storage_host and port == storage_port:
                # TODO add check is it was dead and update
                ind = self.stores.index(storage)
                self.stores[ind] = (host, port, True)
                print(f'Reconnect to storage: {storage_host}:{storage_port}')
                break
        else:
            self.stores.append((storage_host, storage_port, True))
            print(f'Connect to storage: {storage_host}:{storage_port}')
        return 'Success connect'

    async def ping_store(self, store):
        host, port, is_alive = store
        uri = f"ws://{host}:{port}"
        if is_alive:
            try:
                async with websockets.connect(uri) as websocket:
                    await websocket.send('ping')
                    await asyncio.wait_for(websocket.recv(), 3)
            except ConnectionRefusedError:
                print(f'Storage {host}:{port} died. We regret :(')
                ind = self.stores.index(store)
                self.stores[ind] = (host, port, False)
            except asyncio.TimeoutError:
                print(f'Storage {host}:{port} died. We regret :(')
                ind = self.stores.index(store)
                self.stores[ind] = (host, port, False)

    async def ping(self):
        while True:
            if len(self.stores) == 0:
                await asyncio.sleep(5)
                continue
            for store in self.stores:
                await self.ping_store(store)
            await asyncio.sleep(5)

    @_response
    def create(self, filename):
        self.dir_to_files[self.dir_tree.current.path].append(filename)
        print(f'created empty file: {filename}')

    @_response
    @_file_not_found
    def read(self, filename):
        chunks_locations = []
        chunks = self.file_to_chunks.get(filename, [])
        for chunk in chunks:
            replicas = [replica for replica in self.chunk_to_replicas[chunk]]
            chunks_locations.append((chunk, replicas))
        return chunks_locations

    @_response
    @_file_already_exist
    def write(self, filename, file_size):
        self.dir_to_files[self.get_current()].append(filename)
        self.file_to_chunks[filename] = []

        quotient = int(file_size) // self.CHUNK_SIZE
        chunk_num = quotient if int(file_size) % self.CHUNK_SIZE == 0 else quotient + 1
        print(chunk_num)
        chunks = []
        for i in range(chunk_num):
            chunk_name = generate_chunk_name()
            stores = self.get_stores()
            self.file_to_chunks[filename].append(chunk_name)
            self.chunk_to_replicas[chunk_name] = []
            for store in stores:
                self.chunk_to_replicas[chunk_name].append(store)
            chunks.append((chunk_name, stores))
        return chunks

    @_response
    @_file_not_found
    def info(self, filename):
        chunks = self.file_to_chunks.get(filename, [])
        size = self.convert_to_size(len(chunks))
        return f'File took {size}. Distribute in {len(chunks)} chunks'

    @_response
    @_file_not_found
    def delete(self, filename):
        chunks = self.file_to_chunks.get(filename, [])
        for chunk in chunks:
            for host, port in self.chunk_to_replicas[chunk]:
                asyncio.gather(self.send_delete(chunk, host, port))
            del self.chunk_to_replicas[chunk]
        del self.file_to_chunks[filename]
        self.dir_to_files[self.get_current()].remove(filename)
        print(f'Delete file: {filename}')
        return f'File deleted: {filename}'

    @_response
    @_file_not_found
    def copy(self, filename, copy_dir):
        files_in_copy_dir = self.dir_to_files.get(copy_dir)

        if files_in_copy_dir is None:
            raise FileNotFoundError('Directory does not exist')

        if filename in files_in_copy_dir:
            raise FileExistsError('File already exist in this directory')
        else:
            files_in_copy_dir.append(filename)
            return f'File copied: {filename}'

    @_response
    @_file_not_found
    def move(self, filename, move_dir):
        files_in_move_dir = self.dir_to_files.get(move_dir)

        if files_in_move_dir is None:
            raise FileNotFoundError('Directory does not exist')

        if filename in files_in_move_dir:
            raise FileExistsError('File already exist in this directory')
        else:
            files_in_move_dir.append(filename)
            self.dir_to_files[self.get_current()].remove(filename)
            return f'File moved: {filename} to {move_dir}'

    @_response
    def open_dir(self, new_path):
        paths = new_path.split('/')
        try:
            for path in paths:
                if path == '..':
                    self.dir_tree.go_to_parent()
                elif path != '.' and path != '':
                    self.dir_tree.go_to_child(path)
        except FileNotFoundError:
            return 'This directory does not exist'
        print('Opened dir. New current dir is ', self.dir_tree.current.path)
        return f'Open directory. Current directory is {self.dir_tree.current.path}'

    @_response
    def list_dir(self, flag=''):
        current_dir = self.dir_tree.current.path
        print('current dir: ', current_dir)
        files = self.dir_to_files.get(current_dir, [])
        print('current', current_dir, 'files', files, 'directories', [ch.path for ch in self.dir_tree.current.children])
        if flag == '-a':
            return files + [f'/{ch.path.split("/")[-1]}' for ch in self.dir_tree.current.children]
        return files

    @_response
    def make_dir(self, dir_name):
        already_have = dir_name in [ch.path.split('/')[-1] for ch in self.dir_tree.current.children]
        if not already_have:
            full_path = self.dir_tree.put(dir_name)
            self.dir_to_files[full_path] = []
            print('Made directory:', dir_name)
            return f'Made directory: {dir_name}'
        else:
            print('Directory already exist')
            return f'Directory already exist'

    async def _delete_dir(self, dir_name, recursive=False):
        children = self.dir_tree.current.children
        for index, name in enumerate([ch.path.split('/')[-1] for ch in children]):
            if name == dir_name:
                dir_index = index
                dir_full_name = children[index].path
                break
        else:
            raise FileNotFoundError('Directory does not exist')

        has_files = True
        # has_files = len(self.dir_to_files[dir_full_name]) > 0
        has_sub_dirs = len(children[dir_index].children) > 0
        if not recursive and (has_files or has_sub_dirs):
            answer = await self.ask_confirmation('delete', dir_name)
            answer = str(answer).lower()
            print('answer: ', answer)
            if answer == 'yes' or answer == 'y' or answer == '':
                file_list = self.dir_to_files.get(dir_full_name, [])
                sub_dirs_list = [ch.path.split('/')[-1] for ch in children]
                for sub_dir in sub_dirs_list:
                    self.delete_dir(sub_dir, recursive=True)
                for filename in file_list:
                    self.delete(filename)
            else:
                return 'Canceled deleting'
        del self.dir_to_files[dir_full_name]
        self.dir_tree.remove(dir_full_name)
        if not recursive:
            return f'Success delete directory: {dir_name}'

    @_response
    def delete_dir(self, dir_name, recursive=False):
        asyncio.gather(self._delete_dir(dir_name, recursive))

    command_map = {
        'connect': connect,
        'create': create,
        'read': read,
        'write': write,
        'delete': delete,
        'info': info,
        'copy': copy,
        'move': move,
        'cd': open_dir,
        'ls': list_dir,
        'mk': make_dir,
        'rm': delete_dir
    }


if __name__ == '__main__':
    master_port = 8400

    master = Master('../datanode/files')
    # master = Master()
    is_not_hosted = True

    loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(master.ping(), loop)

    while is_not_hosted:
        try:
            server = websockets.serve(master.execute, "localhost", master_port)
            loop.run_until_complete(server)
            print('masternode hosted at localhost:', master_port)
            loop.run_forever()
            is_not_hosted = False
        except OSError as e:
            print(e)
            master_port += 1
