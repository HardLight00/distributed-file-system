import json
import re
from abc import ABC
from typing import Dict, Callable


class Node(ABC):
    PATTERN = re.compile(r' (?=(?:[^\'"]|\'[^\']*\'|"[^"]*")*$)')
    REPLICAS_NUM = 3
    CHUNK_SIZE = 64 * 2 ** 10  # 64 Kb
    # CHUNK_SIZE = 64 * 2 ** 5  # 64B
    command_map = Dict[str, Callable]
    remote_address = None

    def error(self, *args):
        print('error during execution')
        return {'error': "Sorry this command does not exist"}

    async def execute(self, websocket, path):
        remote_host, remote_ip = websocket.remote_address
        self.remote_address = f'{remote_host}:{remote_ip}'
        request = await websocket.recv()
        command, *args = request.split(' ')
        try:
            response = self.command_map.get(command, self.error)(self, *args)
            await websocket.send(json.dumps(response))
        except TypeError as error:
            print('error', f'Invalid arguments for {command}. {type(error).__name__}: ' + str(error))
            return json.dumps({'error': f'Invalid arguments for {command}. {type(error).__name__}: ' + str(error)})
