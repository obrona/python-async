"""Runnable asyncio demos. Requires Python 3.11+ and no third-party packages.

Run all demos:    python async_demo.py
Run one demo:     python async_demo.py timeout
"""

import asyncio
import random
import sys
import time
from collections.abc import Awaitable, Callable


async def fake_request(name: str, delay: float = 1.0) -> str:
    await asyncio.sleep(delay)
    return f"{name} done"


async def sequential_vs_concurrent() -> None:
    start: float = time.perf_counter()
    await fake_request("a")
    await fake_request("b")
    await fake_request("c")
    print(f"Sequential: {time.perf_counter() - start:.2f}s")

    start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        tasks: list[asyncio.Task[str]] = [tg.create_task(fake_request(n)) for n in "abc"]
    print(f"Concurrent: {time.perf_counter() - start:.2f}s")
    print("Results:", [t.result() for t in tasks])


async def error_handling() -> None:
    async def fails() -> None:
        await asyncio.sleep(0.1)
        raise ValueError("something broke")

    async def slow() -> None:
        try:
            await asyncio.sleep(5)
            print("This never prints")
        except asyncio.CancelledError:
            print("slow() was cancelled by the group")
            raise

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(fails())
            tg.create_task(slow())
    except* ValueError as group:
        for err in group.exceptions:
            print("Caught:", err)

    results: tuple[str | BaseException, None | BaseException] = await asyncio.gather(
        fake_request("ok", 0.1), fails(), return_exceptions=True
    )
    print("gather kept going:", results)


async def timeout() -> None:
    try:
        async with asyncio.timeout(0.5):
            await fake_request("slow", 3)
    except TimeoutError:
        print("Timed out after 0.5s")


async def semaphore() -> None:
    limit: asyncio.Semaphore = asyncio.Semaphore(3)
    running: int = 0
    peak: int = 0

    async def job() -> None:
        nonlocal running, peak
        async with limit:
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.2)
            running -= 1

    start: float = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        for _ in range(12):
            tg.create_task(job())
    print(f"12 jobs, limit 3: {time.perf_counter() - start:.2f}s, peak {peak}")


async def worker_queue() -> None:
    async def worker(name: str, queue: asyncio.Queue[float]) -> None:
        while True:
            delay: float = await queue.get()
            try:
                await asyncio.sleep(delay)
                print(f"{name} finished a {delay:.2f}s job")
            finally:
                queue.task_done()

    queue: asyncio.Queue[float] = asyncio.Queue()
    for _ in range(6):
        queue.put_nowait(round(random.uniform(0.1, 0.4), 2))

    workers: list[asyncio.Task[None]] = [asyncio.create_task(worker(f"w{i}", queue)) for i in range(2)]
    await queue.join()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def blocking() -> None:
    async def heartbeat(stop: asyncio.Event) -> int:
        beats: int = 0
        while not stop.is_set():
            beats += 1
            await asyncio.sleep(0.1)
        return beats

    async def measure(label: str, blocking_call: Callable[[], Awaitable[None]]) -> None:
        stop: asyncio.Event = asyncio.Event()
        beat_task: asyncio.Task[int] = asyncio.create_task(heartbeat(stop))
        await asyncio.sleep(0)  # let the heartbeat start
        await blocking_call()
        stop.set()
        print(f"{label}: {await beat_task} heartbeats in about 1s")

    async def blocks_the_loop() -> None:
        time.sleep(1)

    async def uses_a_thread() -> None:
        await asyncio.to_thread(time.sleep, 1)

    await measure("time.sleep  ", blocks_the_loop)
    await measure("to_thread   ", uses_a_thread)


DEMOS: dict[str, Callable[[], Awaitable[None]]] = {
    "concurrent": sequential_vs_concurrent,
    "errors": error_handling,
    "timeout": timeout,
    "semaphore": semaphore,
    "queue": worker_queue,
    "blocking": blocking,
}


def main() -> None:
    names: list[str] = sys.argv[1:] or list(DEMOS)
    unknown: list[str] = [n for n in names if n not in DEMOS]
    if unknown:
        sys.exit(f"Unknown demo: {', '.join(unknown)}. Choose from: {', '.join(DEMOS)}")
    for name in names:
        print(f"\n--- {name} ---")
        asyncio.run(DEMOS[name]())


if __name__ == "__main__":
    main()
