from random import randint
import asyncio

import aiohttp 
import tenacity

@tenacity.retry(stop=tenacity.stop_after_attempt(2))
async def a(http_session, name):
    print(f"Starting {name}")
    response = await http_session.post(f"http://127.0.0.1:5000/{name}/v1/group", json={"groupId": "main"})
    if not response.status == 201:
        raise Exception(name)
    print(f"Finishing {name}")

async def core():
    async with aiohttp.ClientSession() as http_session:
        try:
            res = await asyncio.gather(*[asyncio.create_task(a(http_session, name)) for name in range(20)], return_exceptions=True)
        except Exception as e:
            print(e)
        else:
            return res

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    x = loop.run_until_complete(core())
    errors = [i for i in x if i is not None]
    print(len(errors))
    print(errors) 