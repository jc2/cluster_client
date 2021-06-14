import asyncio
from abc import ABC, abstractclassmethod

import aiohttp 
import tenacity


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


class CreateGroup(NodeAction):

    #@tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def fordward(self, http_session, node, group_name):
        return await NodeClient.create_group(http_session, node, group_name)
    
    #@tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def backward(self, http_session, node, group_name):
        return await NodeClient.delete_group(http_session, node, group_name)
    
    #@tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def get_current_status(self, http_session, node, group_name):
        return await NodeClient.get_group(http_session, node, group_name)


class DeleteGroup(NodeAction):

    #@tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def fordward(self, http_session, node, group_name):
        return await NodeClient.delete_group(http_session, node, group_name)
    
    #@tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def backward(self, http_session, node, group_name):
        return await NodeClient.create_group(http_session, node, group_name)
        
    #@tenacity.retry(stop=tenacity.stop_after_attempt(2))
    async def get_current_status(self, http_session, node, group_name):
        return await NodeClient.get_group(http_session, node, group_name)

class NodeError(Exception):
    def __init__(self, node):
        self.node = node

class NodeGroupNotFound(NodeError):
    pass

class NodeClient():

    @staticmethod
    async def create_group(http_session, node, group_name):
        print(f"Creating on {node}")
        response = await http_session.post(f"{node}/v1/group", json={"groupId": group_name})
        if not response.status == 201:
            raise NodeError(node)
        print(f"Finishing creation on {node}")
        return response
    
    @staticmethod
    async def delete_group(http_session, node, group_name):
        print(f"Deleting on {node}")
        response = await http_session.delete(f"{node}/v1/group", json={"groupId": group_name})
        if not response.status == 201:
            raise NodeError(node)
        print(f"Deleting creation on {node}")
        return response
    
    @staticmethod
    async def get_group(http_session, node, group_name):
        print(f"Getting from {node}")
        response = await http_session.get(f"{node}/v1/group/{group_name}")
        if response.status == 404:
            raise NodeGroupNotFound(node)
        elif not response.status == 200:
            raise NodeError(node)
        print(f"Finishing getting from {node}")
        return response