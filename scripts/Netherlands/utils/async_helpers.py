"""
Async utility functions and decorators.
"""

import asyncio
import functools
from typing import Callable, Any, List, TypeVar, Generic
from contextlib import asynccontextmanager

T = TypeVar('T')


def retry_async(max_attempts: int = 3, backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Decorator for async functions with exponential backoff retry.

    Args:
        max_attempts: Maximum retry attempts (default: 3)
        backoff: Exponential backoff multiplier (default: 2.0)
        exceptions: Tuple of exceptions to catch (default: all Exception)

    Usage:
        @retry_async(max_attempts=2, backoff=5.0)
        async def fetch_page(url):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    # Don't wait on last attempt
                    if attempt < max_attempts - 1:
                        wait_time = backoff ** attempt
                        await asyncio.sleep(wait_time)

            # All attempts failed, raise last exception
            raise last_exception

        return wrapper
    return decorator


class BatchBuffer(Generic[T]):
    """
    Generic batch buffer for accumulating items and processing in batches.

    Usage:
        buffer = BatchBuffer(process_func=db.insert_batch, batch_size=100)
        await buffer.add(item)
        await buffer.flush()  # Process remaining
    """

    def __init__(self, process_func: Callable[[List[T]], Any], batch_size: int = 100):
        """
        Initialize batch buffer.

        Args:
            process_func: Async function to process batch (receives List[T])
            batch_size: Number of items before auto-flush (default: 100)
        """
        self.process_func = process_func
        self.batch_size = batch_size
        self.buffer: List[T] = []
        self.total_processed = 0

    async def add(self, item: T) -> None:
        """Add item to buffer, auto-flush if batch size reached."""
        self.buffer.append(item)

        if len(self.buffer) >= self.batch_size:
            await self.flush()

    async def add_many(self, items: List[T]) -> None:
        """Add multiple items at once."""
        self.buffer.extend(items)

        if len(self.buffer) >= self.batch_size:
            await self.flush()

    async def flush(self) -> int:
        """
        Process buffered items.

        Returns:
            Number of items processed
        """
        if not self.buffer:
            return 0

        count = len(self.buffer)

        # Call process function (sync or async)
        if asyncio.iscoroutinefunction(self.process_func):
            await self.process_func(self.buffer)
        else:
            self.process_func(self.buffer)

        self.buffer.clear()
        self.total_processed += count
        return count

    def get_stats(self) -> dict:
        """Get buffer statistics."""
        return {
            "buffered": len(self.buffer),
            "total_processed": self.total_processed,
            "batch_size": self.batch_size,
        }

    async def __aenter__(self):
        """Context manager support."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Auto-flush on exit."""
        await self.flush()


@asynccontextmanager
async def timed_operation(operation_name: str, print_result: bool = True):
    """
    Async context manager for timing operations.

    Usage:
        async with timed_operation("Scraping"):
            await scrape_all()
    """
    import time
    start = time.time()

    try:
        yield
    finally:
        elapsed = time.time() - start
        if print_result:
            print(f"[TIMING] {operation_name} completed in {elapsed:.2f}s")


async def run_with_semaphore(
    tasks: List[Callable],
    max_concurrent: int = 10,
    return_exceptions: bool = True
) -> List[Any]:
    """
    Run async tasks with concurrency limit.

    Args:
        tasks: List of async callables
        max_concurrent: Max concurrent tasks (default: 10)
        return_exceptions: Return exceptions instead of raising (default: True)

    Returns:
        List of results (or exceptions if return_exceptions=True)
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def limited_task(task):
        async with semaphore:
            return await task()

    return await asyncio.gather(
        *[limited_task(task) for task in tasks],
        return_exceptions=return_exceptions
    )


async def async_enumerate(async_iterable, start=0):
    """
    Async version of enumerate().

    Usage:
        async for index, item in async_enumerate(async_gen()):
            print(index, item)
    """
    index = start
    async for item in async_iterable:
        yield index, item
        index += 1
