from abc import ABC, abstractclassmethod
from enum import Enum

import aiohttp
from tenacity import retry, stop_after_attempt, retry_if_exception_type


class NodeAction(ABC):

    @abstractclassmethod
    async def get_current_status(self, *args, **kwargs):
        pass

    @abstractclassmethod
    async def fordward(self, *args, **kwargs):
        pass

    @abstractclassmethod
    async def backward(self,  *args, **kwargs):
        pass


class NodeActionState(Enum):
    READY = 1
    NOT_NEEDED = 2
    DONE = 3
    ROLLED_BACK = 4
    ERROR = 5


class NodeGeneralError(Exception):
    def __init__(self, node, code, msg):
        self.node = node
        self.code = code
        self.msg = msg

    def __str__(self):
        return f"ERROR {self.code} in {self.node}: {self.msg}"


class NodeError(NodeGeneralError):
    pass


class NodeGroupNotFound(NodeGeneralError):
    pass


class CreateGroup(NodeAction):

    def __init__(self, node):
        self.node = node
        self.status = None

    @retry(stop=stop_after_attempt(3), retry=retry_if_exception_type(NodeError))
    async def fordward(self, group_name):
        try:
            response = await NodeClient.create_group(self.node, group_name)
        except NodeError as e:
            self.status = NodeActionState.ERROR
            raise e
        else:
            self.status = NodeActionState.DONE
            return response

    @retry(stop=stop_after_attempt(3), retry=retry_if_exception_type(NodeError))
    async def backward(self, group_name):
        try:
            response = await NodeClient.delete_group(self.node, group_name)
        except NodeError as e:
            self.status = NodeActionState.ERROR
            raise e
        else:
            self.status = NodeActionState.ROLLED_BACK
            return response

    @retry(stop=stop_after_attempt(3), retry=retry_if_exception_type(NodeError))
    async def get_current_status(self, group_name):
        try:
            response = await NodeClient.get_group(self.node, group_name)
        except NodeGroupNotFound:
            self.status = NodeActionState.READY
        except NodeError as e:
            self.status = NodeActionState.ERROR
            raise e
        else:
            self.status = NodeActionState.NOT_NEEDED
            return response


class DeleteGroup(NodeAction):

    # @retry(stop=stop_after_attempt(3))
    async def fordward(self, node, group_name):
        return await NodeClient.delete_group(node, group_name)

    # @retry(stop=stop_after_attempt(3))
    async def backward(self, node, group_name):
        return await NodeClient.create_group(node, group_name)

    # @retry(stop=stop_after_attempt(3))
    async def get_current_status(self, node, group_name):
        return await NodeClient.get_group(node, group_name)


class NodeClient():

    @staticmethod
    async def create_group(node, group_name):
        print(f"Creating on {node}")
        async with aiohttp.ClientSession() as session:
            response = await session.post(f"{node}/v1/group", json={"groupId": group_name})
            text = await response.text()
        if not response.status == 201:
            raise NodeError(node, response.status, text)
        print(f"Finishing creation on {node}")
        return response

    @staticmethod
    async def delete_group(node, group_name):
        print(f"Deleting on {node}")
        async with aiohttp.ClientSession() as session:
            response = await session.delete(f"{node}/v1/group", json={"groupId": group_name})
            text = await response.text()
        if not response.status == 200:
            raise NodeError(node, response.status, text)
        print(f"Deleting creation on {node}")
        return response

    @staticmethod
    async def get_group(node, group_name):
        print(f"Getting from {node}")
        async with aiohttp.ClientSession() as session:
            response = await session.get(f"{node}/v1/group/{group_name}")
            text = await response.text()
        if response.status == 404:
            raise NodeGroupNotFound(node, response.status, text)
        elif not response.status == 200:
            raise NodeError(node, response.status, text)
        print(f"Finishing getting from {node}")
        return response
