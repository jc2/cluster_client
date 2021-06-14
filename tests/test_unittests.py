import asyncio
import pytest

import aiohttp
from aioresponses import aioresponses

from node import NodeClient, NodeError, NodeGroupNotFound


def test_node_create_group():
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.post(node+"/v1/group", status=201, payload={"groupId": "group_1"})

        resp = loop.run_until_complete(NodeClient.create_group(session, node, "group_1"))
        data = loop.run_until_complete(resp.json())

        assert data == {"groupId": "group_1"}
        assert resp.status == 201


def test_node_create_group_exception():
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.post(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.create_group(session, node, "group_1"))
        mocker.post(node+"/v1/group", status=400, payload={"groupId": "group_1"})
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.create_group(session, node, "group_1"))


def test_node_get_group():
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.get(node+"/v1/group/group_1", status=200, payload={"groupId": "group_1"})

        resp = loop.run_until_complete(NodeClient.get_group(session, node, "group_1"))
        data = loop.run_until_complete(resp.json())

        assert data == {"groupId": "group_1"}
        assert resp.status == 200


def test_node_get_group_exception():
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.get(node+"/v1/group/group_1", status=404, payload={"groupId": "group_1"})
        with pytest.raises(NodeGroupNotFound):
            loop.run_until_complete(NodeClient.get_group(session, node, "group_1"))
        mocker.get(node+"/v1/group/group_1", status=500, payload={"groupId": "group_1"})
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.get_group(session, node, "group_1"))


def test_node_delete_group():
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.delete(node+"/v1/group", status=201, payload={"groupId": "group_1"})

        resp = loop.run_until_complete(NodeClient.delete_group(session, node, "group_1"))
        data = loop.run_until_complete(resp.json())

        assert data == {"groupId": "group_1"}
        assert resp.status == 201


def test_node_delete_group_exception():
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.delete(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.delete_group(session, node, "group_1"))
