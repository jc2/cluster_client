import asyncio

import aiohttp 
      

# async def core():
#     async with aiohttp.ClientSession() as http_session:
#         try:
#             res = await asyncio.gather(*[asyncio.create_task(CreateGroup().run(http_session, f"http://127.0.0.1:5000/{node}", "main")) for node in range(10)], return_exceptions=True)
#         except Exception as e:
#             print(e)
#         else:
#             return res

# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     x = loop.run_until_complete(core())
#     errors = [i for i in x if i is not None]
#     print(len(errors))
#     print(errors) 