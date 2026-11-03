"""Exercise: a download manager that survives failures.

download() simulates fetching a file. Some files download quickly, some hang
for 5 seconds, and one always raises ConnectionError.

Your task: finish fetch_all() so that it:

1. Downloads every file concurrently.
2. Runs at most `max_concurrent` downloads at the same time.
3. Gives each download `timeout_seconds` to finish. Time spent waiting for a
   free slot must not count, only time spent in download().
4. Keeps going when a download fails.
5. Returns a Report:
   - `downloaded` maps each successful file name to its size.
   - `failed` maps each failed file name to "timeout" or "connection error".

You will probably want a helper coroutine that handles a single file.

Run it with: python3 t3.py
"""

import asyncio
import time
from dataclasses import dataclass, field

MAX_CONCURRENT: int = 3
TIMEOUT_SECONDS: float = 0.5

# File name -> (download time in seconds, size in bytes)
FILES: dict[str, tuple[float, int]] = {
    "report.pdf": (0.2, 100),
    "photo.jpg": (0.2, 200),
    "video.mp4": (5.0, 900),
    "notes.txt": (0.2, 50),
    "data.csv": (0.2, 300),
    "music.mp3": (5.0, 700),
    "backup.zip": (0.2, 400),
    "logs.txt": (0.2, 150),
    "slides.pptx": (0.2, 250),
}
BROKEN_FILES: set[str] = {"notes.txt"}

active_downloads: int = 0
peak_downloads: int = 0


@dataclass
class Report:
    downloaded: dict[str, int] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)


async def download(name: str) -> int:
    """Return the file's size, or raise ConnectionError if it is broken."""
    global active_downloads, peak_downloads
    active_downloads += 1
    peak_downloads = max(peak_downloads, active_downloads)
    try:
        delay: float
        size: int
        delay, size = FILES[name]
        await asyncio.sleep(delay)
        if name in BROKEN_FILES:
            raise ConnectionError(f"could not reach {name}")
        return size
    finally:
        active_downloads -= 1


async def fetch_all(
    names: list[str], max_concurrent: int, timeout_seconds: float
) -> Report:
    # TODO: replace this line with your solution
    limit = asyncio.Semaphore(max_concurrent)
    out = Report()

    async def helper(name: str):
        async with limit:
            try:
                res = await asyncio.wait_for(download(name), timeout=timeout_seconds)
                out.downloaded[name] = res
            except TimeoutError:
                out.failed[name] = 'timeout'
            except ConnectionError:
                out.failed[name] = 'connection error'

    await asyncio.gather(*(helper(name) for name in names))
    return out
    
    


async def main() -> None:
    start: float = time.perf_counter()
    report: Report = await fetch_all(list(FILES), MAX_CONCURRENT, TIMEOUT_SECONDS)
    elapsed: float = time.perf_counter() - start

    expected_failed: dict[str, str] = {
        "video.mp4": "timeout",
        "music.mp3": "timeout",
        "notes.txt": "connection error",
    }
    expected_downloaded: dict[str, int] = {
        name: size
        for name, (_, size) in FILES.items()
        if name not in expected_failed
    }

    print(f"Downloaded: {report.downloaded}")
    print(f"Failed: {report.failed}")
    print(f"Took {elapsed:.2f}s, peak of {peak_downloads} downloads at once")

    problems: list[str] = []
    if report.downloaded != expected_downloaded:
        problems.append("`downloaded` is wrong")
    if report.failed != expected_failed:
        problems.append(
            "`failed` is wrong (does waiting for a slot count towards the timeout?)"
        )
    if peak_downloads > MAX_CONCURRENT:
        problems.append(f"more than {MAX_CONCURRENT} downloads ran at once")
    if peak_downloads < MAX_CONCURRENT or elapsed > 1.5:
        problems.append(f"downloads should use all {MAX_CONCURRENT} slots at once")

    if problems:
        for problem in problems:
            print(f"Not yet: {problem}")
    else:
        print("Passed!")


if __name__ == "__main__":
    asyncio.run(main())
