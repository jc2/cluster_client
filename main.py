import asyncio
import logging
from enum import Enum

from node import CreateGroup, DeleteGroup, NodeActionState


logging.basicConfig(filename='/tmp/run.log', format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class CoroutineState(Enum):
    STATE_FETCHED = 1
    DONE = 2
    ROLLED_BACK = 3
    ERROR = 4


class Coroutine():

    actions = {
        "create_group": CreateGroup,
        "delete_group": DeleteGroup
    }

    def __init__(self, action, nodes, group):
        self.group = group
        class_ = self.actions.get(action)
        if not class_:
            # TODO: Handle this exception in a better way
            raise Exception()

        # TODO: Implement a creational pattern
        self.tasks = [class_(node) for node in nodes]

        self.status = None

    def run(self):
        loop = asyncio.get_event_loop()

        # 1. Get status of all nodes
        result = loop.run_until_complete(self._get_status(self.tasks))
        errors = [i for i in result if isinstance(i, Exception)]
        if errors:
            logger.error("Unable to get current status")
            self.status = CoroutineState.ERROR
            return

        task_to_run = [task for task in self.tasks if task.status == NodeActionState.READY]

        # 2. Run the desired action
        if task_to_run:
            result = loop.run_until_complete(self._forward(task_to_run))
            errors = [str(i.last_attempt.exception()) for i in result if isinstance(i, Exception)]
            if errors:
                logger.error(errors)
                logger.warning("Unable to perform updates. Rolling back")

                task_to_rollback = [task for task in self.tasks if task.status == NodeActionState.DONE]

                # 2.1 Rollback in case of errors
                result = loop.run_until_complete(self._backward(task_to_rollback))
                errors = [str(i.last_attempt.exception()) for i in result if isinstance(i, Exception)]
                if errors:
                    self.status = CoroutineState.ERROR
                    logger.error(errors)
                    logger.critical("Error, Rollback failed, a manual check is needed")
                else:
                    self.status = CoroutineState.ROLLED_BACK
                    logger.info("Rollback was successful")
            else:
                self.status = CoroutineState.DONE
                logger.info("Done")

    async def _get_status(self, tasks):
        result = await asyncio.gather(
                      *[asyncio.create_task(task.get_current_status(self.group)) for task in tasks],
                      return_exceptions=True
                  )
        return result

    async def _forward(self, tasks):
        result = await asyncio.gather(
                      *[asyncio.create_task(task.forward(self.group)) for task in tasks],
                      return_exceptions=True
                  )
        return result

    async def _backward(self, tasks):
        result = await asyncio.gather(
                      *[asyncio.create_task(task.backward(self.group)) for task in tasks],
                      return_exceptions=True
                  )
        return result


if __name__ == '__main__':
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(description='Cluster API: For creating groups and beyond :D')
    parser.add_argument('action', type=str, nargs='?',  choices=['create_group', 'delete_group'],
                        help='Action to perform: create_group or delete_group')
    parser.add_argument('group_name', type=str, nargs='?',
                        help='Group name')
    parser.add_argument('node_file', nargs='?', type=argparse.FileType('r'),
                        help="Json file with a list of string (nodes urls)")
    args = parser.parse_args()

    try:
        nodes = json.loads(args.node_file.read())
    except json.decoder.JSONDecodeError:
        sys.exit("File can not be readed")

    if not isinstance(nodes, list):
        sys.exit("Root element must be a list")

    if any([not isinstance(node, str) for node in nodes]):
        sys.exit("Nodes must be a string")

    c = Coroutine(args.action, set(nodes), args.group_name)
    c.run()
