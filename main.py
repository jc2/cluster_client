import asyncio
import logging

from node import CreateGroup, DeleteGroup, NodeActionState


logging.basicConfig(filename='script.log', format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


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

    def run(self):
        loop = asyncio.get_event_loop()

        # 1. Get status of all nodes
        status = loop.run_until_complete(self._get_status(self.tasks))
        errors = [i for i in status if isinstance(i, Exception)]
        if errors:
            logger.error("Unable to get current status")
            return

        task_to_run = [task for task in self.tasks if task.status == NodeActionState.READY]

        # 2. Run the desired action
        if task_to_run:
            status = loop.run_until_complete(self._fordward(task_to_run))
            errors = [str(i.last_attempt.exception()) for i in status if isinstance(i, Exception)]
            if errors:
                logger.error(errors)
                logger.warning("Unable to perform updates. Rolling back")

                task_to_rollback = [task for task in self.tasks if task.status == NodeActionState.DONE]

                # 2.1 Rollback in case of errors
                status = loop.run_until_complete(self._backward(task_to_rollback))
                errors = [str(i.last_attempt.exception()) for i in status if isinstance(i, Exception)]

                if errors:
                    logger.error(errors)
                    logger.critical("Error, Rollback failed, a manual check is needed")

                logger.info("Rollback was successful")

            logger.info("Done")

    async def _get_status(self, tasks):
        result = await asyncio.gather(
                      *[asyncio.create_task(task.get_current_status(self.group)) for task in tasks],
                      return_exceptions=True
                  )
        return result

    async def _fordward(self, tasks):
        result = await asyncio.gather(
                      *[asyncio.create_task(task.fordward(self.group)) for task in tasks],
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
    nodes = [f"http://127.0.0.1:5000/{node}" for node in range(10)]
    c = Coroutine("create_group", nodes, "main")
    c.run()
