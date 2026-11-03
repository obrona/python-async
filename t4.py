"""Exercise: a website crawler with a pool of workers.

fetch() simulates downloading a page and returns the links found on it. Pages
link to each other in loops, and a few links point to pages that don't exist:
fetching one of those raises LookupError (think "404").

Your task: finish crawl() so that it:

1. Starts at `start` and follows every link until no pages are left to visit.
2. Uses an asyncio.Queue of URLs and exactly `num_workers` worker tasks that
   take URLs from the queue, fetch them, and put new links back on it.
3. Fetches each URL at most once, even though many pages link to the same
   place and several workers run at the same time.
4. Keeps going when a fetch raises LookupError.
5. Returns a CrawlResult:
   - `pages` holds every URL that was fetched successfully.
   - `broken` holds every URL that raised LookupError.
6. Leaves no tasks running when it returns. Cancelling a task only asks it to
   stop, so make sure your workers have actually finished.

The tricky part is knowing when you are done: the queue can be empty for a
moment while a worker is still fetching a page that will add more links.

Run it with: python3 t4.py
"""

import asyncio
import time
from collections import Counter
from dataclasses import dataclass, field

NUM_WORKERS: int = 3
FETCH_SECONDS: float = 0.2

# URL -> links on that page. Any URL not listed here is broken.
SITE: dict[str, list[str]] = {
    "/": ["/about", "/blog", "/shop", "/contact"],
    "/about": ["/", "/team"],
    "/team": ["/about", "/careers"],
    "/careers": ["/jobs/old", "/"],
    "/blog": ["/blog/1", "/blog/2", "/blog/3"],
    "/blog/1": ["/blog", "/blog/2"],
    "/blog/2": ["/blog/3", "/shop"],
    "/blog/3": ["/blog/1", "/drafts"],
    "/shop": ["/shop/cart", "/shop/items"],
    "/shop/cart": ["/shop"],
    "/shop/items": ["/shop/cart", "/"],
    "/contact": [],
}

fetch_counts: Counter[str] = Counter()
active_fetches: int = 0
peak_fetches: int = 0


@dataclass
class CrawlResult:
    pages: set[str] = field(default_factory=set)
    broken: set[str] = field(default_factory=set)


async def fetch(url: str) -> list[str]:
    """Return the links on a page, or raise LookupError if it doesn't exist."""
    global active_fetches, peak_fetches
    fetch_counts[url] += 1
    active_fetches += 1
    peak_fetches = max(peak_fetches, active_fetches)
    try:
        await asyncio.sleep(FETCH_SECONDS)
        if url not in SITE:
            raise LookupError(f"404: {url}")
        return list(SITE[url])
    finally:
        active_fetches -= 1


async def crawl(start: str, num_workers: int) -> CrawlResult:
    seen = set[str]()
    crawl_result = CrawlResult()
    queue = asyncio.Queue[str]()

    async def worker():
        while True:
            try:
                url = await queue.get()
                
                if url in seen:
                    queue.task_done()
                    continue
                seen.add(url)

                try:
                    next = await fetch(url)
                    crawl_result.pages.add(url)
                    for n in next:
                        await queue.put(n)
                except LookupError:
                    crawl_result.broken.add(url)
                finally:
                    queue.task_done()
            
            except asyncio.QueueShutDown:
                return

    await queue.put(start)
    
    workers = asyncio.gather(*(worker() for _ in range(num_workers)))
    await queue.join()
    queue.shutdown()
    await workers
    
    return crawl_result


async def main() -> None:
    start: float = time.perf_counter()
    result: CrawlResult = await asyncio.wait_for(crawl("/", NUM_WORKERS), timeout=5)
    elapsed: float = time.perf_counter() - start

    leftover: list[asyncio.Task[object]] = [
        task for task in asyncio.all_tasks() if task is not asyncio.current_task()
    ]
    expected_pages: set[str] = set(SITE)
    expected_broken: set[str] = {"/jobs/old", "/drafts"}
    total_fetches: int = len(expected_pages) + len(expected_broken)
    repeated: list[str] = sorted(url for url, n in fetch_counts.items() if n > 1)

    print(f"Pages: {sorted(result.pages)}")
    print(f"Broken: {sorted(result.broken)}")
    print(f"Took {elapsed:.2f}s, peak of {peak_fetches} fetches at once")

    problems: list[str] = []
    if result.pages != expected_pages:
        problems.append("`pages` is wrong")
    if result.broken != expected_broken:
        problems.append("`broken` is wrong")
    if repeated:
        problems.append(f"these URLs were fetched more than once: {repeated}")
    if peak_fetches > NUM_WORKERS:
        problems.append(f"more than {NUM_WORKERS} fetches ran at once")
    if peak_fetches < NUM_WORKERS or elapsed > total_fetches * FETCH_SECONDS / 2:
        problems.append(f"the crawl should keep all {NUM_WORKERS} workers busy")
    if leftover:
        problems.append(f"{len(leftover)} task(s) were still running after crawl()")

    if problems:
        for problem in problems:
            print(f"Not yet: {problem}")
    else:
        print("Passed!")


if __name__ == "__main__":
    asyncio.run(main())
