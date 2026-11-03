"""Exercise: fetch prices concurrently.

get_price() simulates a slow API call that takes 1 second.

Your task: finish get_total() so it gets the price of every item and returns
their sum. Getting 3 prices one at a time takes 3 seconds, but your version
must take about 1 second.

Run it with: python3 t2.py
"""

import asyncio
import time

PRICES: dict[str, int] = {"apple": 3, "bread": 5, "milk": 4}


async def get_price(item: str) -> int:
    await asyncio.sleep(1)
    return PRICES[item]


async def get_total(items: list[str]) -> int:
    return sum(await asyncio.gather(*(get_price(item) for item in items)))


async def main() -> None:
    start: float = time.perf_counter()
    total: int = await get_total(["apple", "bread", "milk"])
    elapsed: float = time.perf_counter() - start

    print(f"Total: {total}, took {elapsed:.2f}s")
    if total != 12:
        print("Not yet: the total should be 12")
    elif elapsed > 1.5:
        print("Not yet: the total is right, but the calls ran one at a time")
    else:
        print("Passed!")


if __name__ == "__main__":
    asyncio.run(main())
