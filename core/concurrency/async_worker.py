import asyncio
import logging
import random
import time
from typing import Awaitable, Callable, List, TypeVar, Any

log = logging.getLogger(__name__)

T = TypeVar('T')

async def process_concurrently(
    items: List[T],
    worker_func: Callable[[T], Awaitable[Any]],
    concurrency: int = 5,
    delay: float = 0.5,
    stop_event: asyncio.Event = None
):
    """
    Process items concurrently using a worker pool.
    
    Args:
        items: List of items to process.
        worker_func: Async function to process each item.
        concurrency: Number of concurrent workers.
        delay: Delay between processing items.
        stop_event: Event to signal stop.
    """
    queue = asyncio.Queue()
    for item in items:
        queue.put_nowait(item)
    
    tasks = []
    
    async def worker(worker_id):
        log.debug(f"[Worker {worker_id}] Started")
        while not queue.empty():
            if stop_event and stop_event.is_set():
                break
                
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            try:
                await worker_func(item)
            except Exception as e:
                log.error(f"[Worker {worker_id}] Error processing item: {e}")
            finally:
                queue.task_done()
                await asyncio.sleep(delay + random.uniform(0.0, 0.5))

    for i in range(concurrency):
        task = asyncio.create_task(worker(i))
        tasks.append(task)
        
    await asyncio.gather(*tasks, return_exceptions=True)
