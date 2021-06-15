import asyncio
import pytest

from aioresponses import aioresponses
from tenacity import stop_after_attempt, RetryError

from node import NodeClient, NodeError, NodeGroupNotFound, CreateGroup, DeleteGroup, NodeActionState
from main import Coroutine, CoroutineState


def test_node_create_group():

    loop = asyncio.get_event_loop()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.post(node+"/v1/group", status=201, payload={"groupId": "group_1"})

        resp = loop.run_until_complete(NodeClient.create_group(node, "group_1"))
        data = loop.run_until_complete(resp.json())

        assert data == {"groupId": "group_1"}
        assert resp.status == 201


def test_node_create_group_exception():

    loop = asyncio.get_event_loop()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.post(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.create_group(node, "group_1"))
        mocker.post(node+"/v1/group", status=400, payload={"groupId": "group_1"})
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.create_group(node, "group_1"))


def test_node_get_group():

    loop = asyncio.get_event_loop()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.get(node+"/v1/group/group_1", status=200, payload={"groupId": "group_1"})

        resp = loop.run_until_complete(NodeClient.get_group(node, "group_1"))
        data = loop.run_until_complete(resp.json())

        assert data == {"groupId": "group_1"}
        assert resp.status == 200


def test_node_get_group_exception():

    loop = asyncio.get_event_loop()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.get(node+"/v1/group/group_1", status=404)
        with pytest.raises(NodeGroupNotFound):
            loop.run_until_complete(NodeClient.get_group(node, "group_1"))
        mocker.get(node+"/v1/group/group_1", status=500)
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.get_group(node, "group_1"))


def test_node_delete_group():

    loop = asyncio.get_event_loop()

    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.delete(node+"/v1/group", status=200)

        resp = loop.run_until_complete(NodeClient.delete_group(node, "group_1"))

        assert resp.status == 200


def test_node_delete_group_exception():

    loop = asyncio.get_event_loop()
    with aioresponses() as mocker:
        node = "node1.cluster.com"
        mocker.delete(node+"/v1/group", status=500)
        with pytest.raises(NodeError):
            loop.run_until_complete(NodeClient.delete_group(node, "group_1"))


def test_action_create_group_getstatus_not_needed():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=200, payload={"groupId": "group_1"})
        resp = loop.run_until_complete(action.get_current_status("group_1"))

        assert action.status == NodeActionState.NOT_NEEDED
        assert resp.status == 200


def test_action_create_group_getstatus_ready():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=404)
        loop.run_until_complete(action.get_current_status("group_1"))

        assert action.status == NodeActionState.READY


def test_action_create_group_getstatus_error():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)
    action.get_current_status.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=500)
        with pytest.raises(RetryError) as error:
            loop.run_until_complete(action.get_current_status("group_1"))
            assert isinstance(error.last_attempt.exception(), NodeError)

        assert action.status == NodeActionState.ERROR


def test_action_create_group_forward_done():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)

    with aioresponses() as mocker:
        mocker.post(node+"/v1/group", status=201, payload={"groupId": "group_1"})
        resp = loop.run_until_complete(action.forward("group_1"))

        assert action.status == NodeActionState.DONE
        assert resp.status == 201


def test_action_create_group_forward_error():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)
    action.forward.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.post(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(RetryError) as error:
            loop.run_until_complete(action.forward("group_1"))
            assert isinstance(error.last_attempt.exception(), NodeError)

        assert action.status == NodeActionState.ERROR


def test_action_create_group_backward_rolled_back():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)

    with aioresponses() as mocker:
        mocker.delete(node+"/v1/group", status=200, payload={"groupId": "group_1"})
        resp = loop.run_until_complete(action.backward("group_1"))

        assert action.status == NodeActionState.ROLLED_BACK
        assert resp.status == 200


def test_action_create_group_backward_error():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = CreateGroup(node)
    action.backward.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.delete(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(RetryError) as error:
            loop.run_until_complete(action.backward("group_1"))
            assert isinstance(error.last_attempt.exception(), NodeError)

        assert action.status == NodeActionState.ERROR


def test_action_delete_group_getstatus_not_needed():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=200, payload={"groupId": "group_1"})
        resp = loop.run_until_complete(action.get_current_status("group_1"))

        assert action.status == NodeActionState.READY
        assert resp.status == 200


def test_action_delete_group_getstatus_ready():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=404)
        loop.run_until_complete(action.get_current_status("group_1"))

        assert action.status == NodeActionState.NOT_NEEDED


def test_action_delete_group_getstatus_error():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)
    action.get_current_status.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=500)
        with pytest.raises(RetryError) as error:
            loop.run_until_complete(action.get_current_status("group_1"))
            assert isinstance(error.last_attempt.exception(), NodeError)

        assert action.status == NodeActionState.ERROR


def test_action_delete_group_forward_done():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)

    with aioresponses() as mocker:
        mocker.delete(node+"/v1/group", status=200, payload={"groupId": "group_1"})
        resp = loop.run_until_complete(action.forward("group_1"))

        assert action.status == NodeActionState.DONE
        assert resp.status == 200


def test_action_delete_group_forward_error():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)
    action.forward.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.delete(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(RetryError) as error:
            loop.run_until_complete(action.forward("group_1"))
            assert isinstance(error.last_attempt.exception(), NodeError)

        assert action.status == NodeActionState.ERROR


def test_action_delete_group_backward_rolled_back():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)

    with aioresponses() as mocker:
        mocker.post(node+"/v1/group", status=201, payload={"groupId": "group_1"})
        resp = loop.run_until_complete(action.backward("group_1"))

        assert action.status == NodeActionState.ROLLED_BACK
        assert resp.status == 201


def test_action_delete_group_backward_error():

    loop = asyncio.get_event_loop()
    node = "node1.cluster.com"
    action = DeleteGroup(node)
    action.backward.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.post(node+"/v1/group", status=500, payload={"groupId": "group_1"})
        with pytest.raises(RetryError) as error:
            loop.run_until_complete(action.backward("group_1"))
            assert isinstance(error.last_attempt.exception(), NodeError)

        assert action.status == NodeActionState.ERROR


def test_coroutine_create_group_error_1():

    node = "node1.cluster.com"
    coroutine = Coroutine("create_group", [node], "group_1")

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=500)
        coroutine.run()
        assert coroutine.status == CoroutineState.ERROR


def test_coroutine_create_group_done():

    node = "node1.cluster.com"
    coroutine = Coroutine("create_group", [node], "group_1")
    coroutine.tasks[0].forward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].backward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].get_current_status.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=404)
        mocker.post(node+"/v1/group", status=201)
        coroutine.run()
        assert coroutine.status == CoroutineState.DONE


def test_coroutine_create_group_rolledback():

    node = "node1.cluster.com"
    coroutine = Coroutine("create_group", [node], "group_1")
    coroutine.tasks[0].forward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].backward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].get_current_status.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=404)
        mocker.post(node+"/v1/group", status=500)
        coroutine.run()
        assert coroutine.status == CoroutineState.ROLLED_BACK


def test_coroutine_delete_group_error_1():

    node = "node1.cluster.com"
    coroutine = Coroutine("delete_group", [node], "group_1")

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=500)
        coroutine.run()
        assert coroutine.status == CoroutineState.ERROR


def test_coroutine_delete_group_done():

    node = "node1.cluster.com"
    coroutine = Coroutine("delete_group", [node], "group_1")
    coroutine.tasks[0].forward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].backward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].get_current_status.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=200)
        mocker.delete(node+"/v1/group", status=200)
        coroutine.run()
        assert coroutine.status == CoroutineState.DONE


def test_coroutine_delete_group_rolledback():

    node = "node1.cluster.com"
    coroutine = Coroutine("delete_group", [node], "group_1")
    coroutine.tasks[0].forward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].backward.retry.stop = stop_after_attempt(1)
    coroutine.tasks[0].get_current_status.retry.stop = stop_after_attempt(1)

    with aioresponses() as mocker:
        mocker.get(node+"/v1/group/group_1", status=200)
        mocker.delete(node+"/v1/group", status=500)
        coroutine.run()
        assert coroutine.status == CoroutineState.ROLLED_BACK
