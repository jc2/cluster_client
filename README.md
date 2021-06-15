# Cluster update tool
This project is a Cluster tool to perform different actions to each node within the cluster.
Nodes have an HTTP API to create, delete, and get groups. The idea is to make it as robust as it can. 
Since HTTP requests are I/O blocking, this project use async communication (coroutines) with `asyncio` and `aiohttp`
 
This is a demo project.
 
## Assumptions
- Node API is unstable, the system should be able to handle connection errors.
- If an action can not be performed in one node or more for connection issues, all nodes will be rolled back.
- Since rollback also consumes HTTP, there is a possibility where rollback is not possible.
- More actions can be included in the future.
- In the case of creating groups, if a node already has the group, it won't try to create it again. Same thing with delete.
- The system will do 3 attempts to perform the action.
 
 
## How to run
- Create docker image: `docker build -t <image_name> .`
- Run container: `docker run -v $(pwd):/tmp -it --rm <image_name>:latest bash`
- Inside container: `python main.py create_group group_1 /tmp/nodes.json`
- Test: `python pytest`
 
- **NOTE:** Logs will go to `/tmp`, pay attention to the volume used in your host machine.
 
- [Optional] Run fake Node API: `flask run --host=0.0.0.0`
 
## Project structure
- `tests/`: For now, only unittest are in this folder
- `main.py`: Main script with a handy CLI
- `node.py`: Classes to interact with the Node API
- `nodes.json`: A JSON example file with the nodes list
- `app.py`: Flask service, it mimics the Node API for manual integration testing
- `requirements.txt`: Python dependencies
- `Dockerfile`: Dockerfile to create a simple image to be able to run the project easily
 
## TODO:
- [ ] Move Coroutine class to other module
- [ ] Split unit test and add fixtures
- [ ] Create automated integration tests using `app.py`
- [ ] Add coverage reports
- [ ] Process nodes in chunks
- [ ] Save status of the nodes, similar to what Terraforms does.