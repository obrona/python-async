"""Exercise: a cache that shares fetches between callers.

load() simulates a slow database lookup. Keys starting with "missing" always
raise LookupError, and keys starting with "flaky" raise LookupError the first
time they are loaded and work after that.

Your task: finish the Cache class so that `await cache.get(key)`:

1. Returns a value straight away, without calling load(), if that key has
   already been loaded successfully.
2. Never runs two load() calls for the same key at the same time. If a load
   for the key is already running, wait for it and share its result.
3. Lets loads for different keys run concurrently.
4. Doesn't cache failures. When a load raises, every caller waiting on it
   gets the exception, and the next get() for that key tries again.
5. Keeps the load going when one waiting caller is cancelled, because the
   other callers still want the result.
6. Cancels the load when every caller waiting on it has been cancelled,
   because nobody wants the result any more. A later get() starts a new load.

Run it with: python3 t6.py
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

LOAD_SECONDS: float = 0.2

loads: list[str] = []
cancelled_loads: list[str] = []
open_loads: int = 0


async def load(key: str) -> str:
    """Return the value for `key`, or raise LookupError."""
    global open_loads
    loads.append(key)
    open_loads += 1
    try:
        await asyncio.sleep(LOAD_SECONDS)
    except asyncio.CancelledError:
        cancelled_loads.append(key)
        raise
    finally:
        open_loads -= 1
    if key.startswith("missing"):
        raise LookupError(f"{key} not found")
    if key.startswith("flaky") and loads.count(key) == 1:
        raise LookupError(f"{key} failed, try again")
    return f"value of {key}"

@dataclass
class State:
    num_callers: int
    task: asyncio.Task[str]

class Cache:
    def __init__(self, loader: Callable[[str], Awaitable[str]]) -> None:
        self._loader: Callable[[str], Awaitable[str]] = loader
        self.store: dict[str, State] = {}

    def _on_done(self, key: str, state: State) -> str:
        if not state.task.cancelled() and state.task.exception() and self.store.get(key) is state:
            del self.store[key]

    async def get(self, key: str) -> str:
        try:
            state: State = None
            if key not in self.store:
                t = asyncio.create_task(self._loader(key))
                state = State(1, t)
                self.store[key] = state

                # callback run first before any awaiters.
                # this prevent new get() calls from joining a state with a task that has finished with an exception.
                # the order of operations depends on the order of await.
                # so if a code awaits, then later on some other code calls add_done_callback()
                # then the awaiter runs first.
                t.add_done_callback(lambda _t: self._on_done(key, state))
                
            else:
                state = self.store[key]
                state.num_callers += 1

            return await asyncio.shield(state.task)
            
        except asyncio.CancelledError as err:
            state.num_callers -= 1
            if state.num_callers == 0:
                state.task.cancel()
                del self.store[key]
            raise err









# ---------------------------------------------------------------------------
# Tests. Each case gets a fresh Cache and returns a list of problems.
# ---------------------------------------------------------------------------


def check(problems: list[str], ok: bool, message: str) -> None:
    if not ok:
        problems.append(message)


def check_time(problems: list[str], elapsed: float, expected: float) -> None:
    check(
        problems,
        abs(elapsed - expected) <= 0.08,
        f"expected to take about {expected:.2f}s, took {elapsed:.2f}s",
    )


async def case_cached(cache: Cache, problems: list[str]) -> None:
    first: str = await cache.get("a")
    start: float = time.perf_counter()
    second: str = await cache.get("a")
    elapsed: float = time.perf_counter() - start
    check(problems, first == second == "value of a", f"got {first!r} and {second!r}")
    check(problems, loads == ["a"], f"expected one load of 'a', got {loads}")
    check_time(problems, elapsed, 0.0)


async def case_same_key(cache: Cache, problems: list[str]) -> None:
    start: float = time.perf_counter()
    results: list[str] = await asyncio.gather(*(cache.get("a") for _ in range(5)))
    elapsed: float = time.perf_counter() - start
    check(problems, results == ["value of a"] * 5, f"got {results}")
    check(problems, loads == ["a"], f"expected one load of 'a', got {loads}")
    check_time(problems, elapsed, LOAD_SECONDS)


async def case_different_keys(cache: Cache, problems: list[str]) -> None:
    start: float = time.perf_counter()
    results: list[str] = await asyncio.gather(*(cache.get(k) for k in "abc"))
    elapsed: float = time.perf_counter() - start
    expected: list[str] = ["value of a", "value of b", "value of c"]
    check(problems, results == expected, f"got {results}")
    check(problems, sorted(loads) == ["a", "b", "c"], f"got loads {loads}")
    check_time(problems, elapsed, LOAD_SECONDS)


async def case_errors(cache: Cache, problems: list[str]) -> None:
    results: list[str | BaseException] = await asyncio.gather(
        *(cache.get("flaky") for _ in range(3)), return_exceptions=True
    )
    errors: list[str] = [str(r) for r in results if isinstance(r, LookupError)]
    check(
        problems,
        errors == ["flaky failed, try again"] * 3,
        f"expected all 3 callers to get the LookupError, got {results}",
    )
    check(problems, loads == ["flaky"], f"expected one load so far, got {loads}")
    try:
        retry: str = await cache.get("flaky")
        check(problems, retry == "value of flaky", f"retry got {retry!r}")
    except LookupError as e:
        problems.append(f"the retry raised {e!r}: was the failure cached?")
    check(problems, loads == ["flaky", "flaky"], f"expected two loads, got {loads}")

    missing: list[str | BaseException] = await asyncio.gather(
        cache.get("missing"), cache.get("missing"), return_exceptions=True
    )
    check(
        problems,
        all(isinstance(r, LookupError) for r in missing),
        f"expected LookupErrors for 'missing', got {missing}",
    )


async def case_one_cancelled(cache: Cache, problems: list[str]) -> None:
    quitter: asyncio.Task[str] = asyncio.create_task(cache.get("a"))
    stayer: asyncio.Task[str] = asyncio.create_task(cache.get("a"))
    await asyncio.sleep(0.05)
    quitter.cancel()
    start: float = time.perf_counter()
    result: str | BaseException
    try:
        result = await stayer
    except BaseException as e:
        result = e
    elapsed: float = time.perf_counter() - start
    check(problems, quitter.cancelled(), "the cancelled caller wasn't cancelled")
    check(problems, result == "value of a", f"the other caller got {result!r}")
    check(problems, loads == ["a"], f"expected one load of 'a', got {loads}")
    check(problems, not cancelled_loads, "the load was cancelled")
    check_time(problems, elapsed, LOAD_SECONDS - 0.05)


async def case_all_cancelled(cache: Cache, problems: list[str]) -> None:
    callers: list[asyncio.Task[str]] = [
        asyncio.create_task(cache.get("a")) for _ in range(3)
    ]
    await asyncio.sleep(0.05)
    for caller in callers:
        caller.cancel()
    await asyncio.gather(*callers, return_exceptions=True)
    await asyncio.sleep(0.01)
    check(problems, all(c.cancelled() for c in callers), "a caller wasn't cancelled")
    check(problems, cancelled_loads == ["a"], "expected the load to be cancelled")
    check(problems, open_loads == 0, "the load was still running")

    start: float = time.perf_counter()
    try:
        again: str = await cache.get("a")
        check(problems, again == "value of a", f"the next get() returned {again!r}")
    except BaseException as e:
        problems.append(f"the next get() raised {e!r}")
    elapsed: float = time.perf_counter() - start
    check(problems, loads == ["a", "a"], f"expected a fresh load, got {loads}")
    check_time(problems, elapsed, LOAD_SECONDS)


CASES: list[tuple[str, Callable[[Cache, list[str]], Awaitable[None]]]] = [
    ("cached values come back instantly", case_cached),
    ("callers share one load", case_same_key),
    ("different keys load concurrently", case_different_keys),
    ("failures are shared but not cached", case_errors),
    ("one caller gives up", case_one_cancelled),
    ("every caller gives up", case_all_cancelled),
]


async def run_case(test: Callable[[Cache, list[str]], Awaitable[None]]) -> list[str]:
    loads.clear()
    cancelled_loads.clear()
    problems: list[str] = []
    try:
        await asyncio.wait_for(test(Cache(load), problems), 5)
    except Exception as e:
        problems.append(f"raised {e!r}")

    if open_loads:
        problems.append(f"{open_loads} load(s) were still running")
    leftover: list[asyncio.Task[object]] = [
        task for task in asyncio.all_tasks() if task is not asyncio.current_task()
    ]
    if leftover:
        problems.append(f"{len(leftover)} task(s) were still running")
        for task in leftover:
            task.cancel()
        await asyncio.gather(*leftover, return_exceptions=True)
    return problems


async def main() -> None:
    passed: bool = True
    for title, test in CASES:
        print(f"{title}:")
        problems: list[str] = await run_case(test)
        for problem in problems:
            print(f"  Not yet: {problem}")
        passed = passed and not problems
    print("Passed!" if passed else "Some cases failed.")


if __name__ == "__main__":
    asyncio.run(main())
