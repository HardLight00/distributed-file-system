import asyncio
import base64
import json
import os
from functools import wraps

import websockets

from node import Node


class Client(Node):
    def __init__(self):
        super().__init__()
        self.is_die = False

    def server_response(func):
        @wraps(func)
        async def wrapper(*args):
            response = await func(*args)
            return f"[SERVER]: {response}"
        return wrapper

    @server_response
    async def error(self, second_self, websocket, command):
        await websocket.send(command)
        response = json.loads(await websocket.recv())

        if response.get('confirmation') is not None:
            answer = input('[CONFIRMATION]: ' + response['confirmation'])
            await websocket.send(answer)
            response = json.loads(await websocket.recv())

        return response.get('body', response.get('error'))

    @server_response
    async def read(self, websocket, command):
        filename = command.split(' ')[1]
        await websocket.send(f'read {filename}')
        response = json.loads(await websocket.recv())
        locations = response.get('body')
        file_data = b''
        if locations is not None:
            for chunk, replicas in locations:
                for replica in replicas:
                    host, port = replica
                    uri = f"ws://{host}:{port}"
                    try:
                        async with websockets.connect(uri) as data_socket:
                            await data_socket.send(f'read {chunk}')
                            chunk_resp = json.loads(await data_socket.recv())
                            encoded_data = chunk_resp['body']
                            print(f'receive chunk {chunk} : {encoded_data}')
                            chunk_data = base64.b64decode(encoded_data)
                            file_data += chunk_data
                        break
                    except ConnectionRefusedError:
                        print(f'Connection refused at {host}:{port}')
                    except KeyError:
                        print(f'Key Error at {host}:{port}')
            r_filename = '.'.join(filename.split('.')[:-1]) + '-read-from-dfs.' + filename.split('.')[-1]
            with open(r_filename, 'wb') as file:
                file.write(file_data)
            print(f'Read {filename} from dfs: {file_data[:32]}')
        else:
            return response.get('error')

    @server_response
    async def write(self, websocket, command):
        file_path = command.split(' ')[1]
        file_size = os.path.getsize(file_path)
        filename = file_path.split('/')[-1]
        print('file size', file_size)

        file = open(file_path, 'rb')
        await websocket.send(f'write {filename} {file_size}')
        response = json.loads(await websocket.recv())
        chunks = response.get('body')
        if chunks is not None:
            for chunk_name, stores in chunks:
                info = file.read(self.CHUNK_SIZE)
                encoded_info = base64.b64encode(info).decode("utf-8")
                for host, port in stores:
                    uri = f"ws://{host}:{port}"
                    async with websockets.connect(uri) as data_socket:
                        await data_socket.send(f'write {chunk_name} {encoded_info}')
                        print(f'wrote at {host}:{port} a chunk: {chunk_name}')
            return 'success writing'
        else:
            return response.get('error')

    # @server_response
    # async def delete(self, websocket, command):
    #     filename = command.split(' ')[1]
    #     await websocket.send(f'delete {filename}')
    #     response = json.loads(await websocket.recv())
    #     return response.get('body', response.get('error'))

    async def shut_down(self, *args):
        self.is_die = True
        return 'Client was shut down'

    command_map = {
        'read': read,
        'write': write,
        # 'delete': delete,
        'exit': shut_down,
    }


async def main():
    # port = input("Enter a node port: ")
    port = 8400
    uri = f"ws://localhost:{port}"
    cl = Client()
    while not cl.is_die:
        in_cmd = input('Enter a command: ')
        try:
            async with websockets.connect(uri) as websocket:
                print('address: ', websocket.local_address)
                command = in_cmd.split(' ')[0]
                print(await cl.command_map.get(command, cl.error)(cl, websocket, in_cmd))
        except Exception as err:
            print(type(err).__name__, err)
    await websocket.close(1000, 'closed by client')

if __name__ == '__main__':
    client = Client()
    asyncio.get_event_loop().run_until_complete(main())
