"""Exercise: race several mirrors and keep the first answer.

download_from() simulates downloading the same file from different mirror
servers. Some mirrors are slow, some fail with ConnectionError.

Your task: finish race_mirrors() so that it:

1. Starts with the first mirror only.
2. Starts the next mirror when the most recent one has run for
   `stagger_seconds` without succeeding, OR straight away when a running
   mirror fails. (Don't start all of them at once: that wastes bandwidth.)
3. Keeps the earlier mirrors running when it starts a new one. Any of them
   may still win.
4. Returns the data from the first mirror that succeeds.
5. Cancels every other mirror that is still running and waits until they
   have actually stopped before returning.
6. If every mirror fails, raises an ExceptionGroup("all mirrors failed", ...)
   holding each mirror's ConnectionError, in mirror order.

Don't use asyncio.staggered: it's an internal module.
Useful tools: asyncio.create_task(), asyncio.wait() with `timeout` and
`return_when=asyncio.FIRST_COMPLETED`, task.cancel().

Run it with: python3 t5.py
"""

import asyncio
import time
from dataclasses import dataclass


@dataclass
class Mirror:
    name: str
    delay: float
    works: bool


started: list[str] = []
open_connections: int = 0


async def download_from(mirror: Mirror) -> str:
    """Return the file's data, or raise ConnectionError if the mirror fails."""
    global open_connections
    started.append(mirror.name)
    open_connections += 1
    try:
        await asyncio.sleep(mirror.delay)
        if not mirror.works:
            raise ConnectionError(f"{mirror.name} is down")
        return f"data from {mirror.name}"
    finally:
        open_connections -= 1


async def race_mirrors(mirrors: list[Mirror], stagger_seconds: float) -> str:
    ptr = 0
    tasks = list[asyncio.Task[str]]()
    pending = set[asyncio.Task[str]]()

    def add_task():
        nonlocal ptr
        if ptr == len(mirrors):
            return
        t = asyncio.create_task(download_from(mirrors[ptr]))
        ptr += 1
        tasks.append(t)
        pending.add(t)

    add_task()

    while pending:
        done, pending = await asyncio.wait(pending, timeout=stagger_seconds, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if t.exception() == None:
                for p in pending:
                    p.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                return t.result()

        if ptr < len(mirrors):
            add_task()

    raise ExceptionGroup('all mirrors failed', [t.exception() for t in tasks])








@dataclass
class Case:
    title: str
    mirrors: list[Mirror]
    expected: str | None  # None means every mirror should fail
    expected_started: list[str]
    expected_seconds: float


STAGGER: float = 0.25

CASES: list[Case] = [
    Case(
        title="first mirror is fast",
        mirrors=[Mirror("A", 0.1, True), Mirror("B", 0.1, True)],
        expected="data from A",
        expected_started=["A"],
        expected_seconds=0.1,
    ),
    # A is slow, so B starts at 0.25. B fails at 0.35, so C starts right away.
    # C finishes at 0.55, before D's turn at 0.60, so D never starts.
    Case(
        title="slow, broken, then fast",
        mirrors=[
            Mirror("A", 1.0, True),
            Mirror("B", 0.1, False),
            Mirror("C", 0.2, True),
            Mirror("D", 0.1, True),
        ],
        expected="data from C",
        expected_started=["A", "B", "C"],
        expected_seconds=0.55,
    ),
    # B starts at 0.25 but A finishes first at 0.4.
    Case(
        title="an earlier mirror still wins",
        mirrors=[Mirror("A", 0.4, True), Mirror("B", 0.5, True)],
        expected="data from A",
        expected_started=["A", "B"],
        expected_seconds=0.4,
    ),
    # A fails at 0.1, so B starts. B is still going at 0.35, so C starts.
    # B fails at 0.4 and C fails at 0.45.
    Case(
        title="everything is down",
        mirrors=[
            Mirror("A", 0.1, False),
            Mirror("B", 0.3, False),
            Mirror("C", 0.1, False),
        ],
        expected=None,
        expected_started=["A", "B", "C"],
        expected_seconds=0.45,
    ),
]


async def run_case(case: Case) -> list[str]:
    started.clear()
    problems: list[str] = []
    start: float = time.perf_counter()
    result: str | None = None
    errors: list[str] | None = None

    try:
        result = await asyncio.wait_for(race_mirrors(case.mirrors, STAGGER), 5)
    except* ConnectionError as group:
        errors = [str(e) for e in group.exceptions]
        if group.message != "all mirrors failed":
            problems.append(f"the ExceptionGroup message was {group.message!r}")
    elapsed: float = time.perf_counter() - start

    print(f"  started {started}, took {elapsed:.2f}s")
    if case.expected is None:
        expected_errors: list[str] = [f"{m.name} is down" for m in case.mirrors]
        if errors is None:
            problems.append(f"expected an ExceptionGroup, got {result!r}")
        elif errors != expected_errors:
            problems.append(f"expected errors {expected_errors}, got {errors}")
    elif result != case.expected:
        problems.append(f"expected {case.expected!r}, got {result!r}")

    if started != case.expected_started:
        problems.append(f"expected to start {case.expected_started}")
    if abs(elapsed - case.expected_seconds) > 0.08:
        problems.append(f"expected to take about {case.expected_seconds:.2f}s")
    if open_connections:
        problems.append(f"{open_connections} download(s) were still running")

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
    for case in CASES:
        print(f"{case.title}:")
        problems: list[str] = await run_case(case)
        for problem in problems:
            print(f"  Not yet: {problem}")
        passed = passed and not problems
    print("Passed!" if passed else "Some cases failed.")


if __name__ == "__main__":
    asyncio.run(main())
