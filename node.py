from abc import ABC, abstractclassmethod

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

    def __init__(self, session, node):
        self.session = session
        self.node = node

    @tenacity.retry(stop=tenacity.stop_after_attempt(3))
    async def fordward(self, group_name):
        return await NodeClient.create_group(self.session, self.node, group_name)

    @tenacity.retry(stop=tenacity.stop_after_attempt(3))
    async def backward(self, group_name):
        return await NodeClient.delete_group(self.session, self.node, group_name)

    @tenacity.retry(stop=tenacity.stop_after_attempt(3))
    async def get_current_status(self, group_name):
        return await NodeClient.get_group(self.session, self.node, group_name)


class DeleteGroup(NodeAction):

    # @tenacity.retry(stop=tenacity.stop_after_attempt(3))
    async def fordward(self, session, node, group_name):
        return await NodeClient.delete_group(session, node, group_name)

    # @tenacity.retry(stop=tenacity.stop_after_attempt(3))
    async def backward(self, session, node, group_name):
        return await NodeClient.create_group(session, node, group_name)

    # @tenacity.retry(stop=tenacity.stop_after_attempt(3))
    async def get_current_status(self, session, node, group_name):
        return await NodeClient.get_group(session, node, group_name)


class NodeError(Exception):
    def __init__(self, node):
        self.node = node


class NodeGroupNotFound(NodeError):
    pass


class NodeClient():

    @staticmethod
    async def create_group(session, node, group_name):
        print(f"Creating on {node}")
        response = await session.post(f"{node}/v1/group", json={"groupId": group_name})
        if not response.status == 201:
            raise NodeError(node)
        print(f"Finishing creation on {node}")
        return response

    @staticmethod
    async def delete_group(session, node, group_name):
        print(f"Deleting on {node}")
        response = await session.delete(f"{node}/v1/group", json={"groupId": group_name})
        if not response.status == 200:
            raise NodeError(node)
        print(f"Deleting creation on {node}")
        return response

    @staticmethod
    async def get_group(session, node, group_name):
        print(f"Getting from {node}")
        response = await session.get(f"{node}/v1/group/{group_name}")
        if response.status == 404:
            raise NodeGroupNotFound(node)
        elif not response.status == 200:
            raise NodeError(node)
        print(f"Finishing getting from {node}")
        return response
