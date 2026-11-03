import asyncio

async def slow(time: float) -> int:
    await asyncio.sleep(time)
    return 1

async def async_collect():
    res = await asyncio.gather(*(slow(0.3) for _ in range(10)))
    print(res)

async def serial_collect():
    res = [await slow(0.3) for _ in range(10)]
    print(res)


asyncio.run(async_collect())
asyncio.run(serial_collect())