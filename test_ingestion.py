import asyncio
from elasticsearch import AsyncElasticsearch, ApiError
from elastic_transport import ConnectionTimeout
from faker import Faker
import functools
from datetime import datetime, timedelta
import random
import logging

INDEX="books-index"
BATCH_SIZE=250
MAX_PARALLEL_PROCESSES=25

PROC = random.randint(0, 100000000)

logging.basicConfig(level=logging.DEBUG)

class IngestionStats:
    def __init__(self):
        self.window = [0] * 100
        self.window_idx = 0
        self.threshold = 1
        self.error_time_threshold = 30
        self.resize_threshold = 10
        self.last_resize = None
        self.last_error = None

        self.stats = []


    def success(self):
        self.window[self.window_idx] = 0
        self.window_idx = (self.window_idx + 1) % 100
        self.stats.append({ "time": datetime.now(), "success": True })


    def failure(self):
        self.window[self.window_idx] = 1
        self.window_idx = (self.window_idx + 1) % 100
        self.last_error = datetime.now()
        self.stats.append({ "time": datetime.now(), "success": True })


    def should_downsize(self):
        if self.last_resize is not None and (datetime.now() - self.last_resize).seconds < self.resize_threshold:
            return False

        return sum(self.window) >= self.threshold


    def should_upsize(self):
        if self.last_resize and (datetime.now() - self.last_resize).seconds < self.resize_threshold:
            return False

        if sum(self.window) > self.threshold:
            return False

        # Meh
        if not self.last_error:
            return False

        if (datetime.now() - self.last_error).seconds > self.error_time_threshold:
            return True

        return False


    def track_resize(self):
        self.window = [0] * 100
        self.last_resize = datetime.now()


    def export_stats(self, nr_coros):
        file = open(f"/tmp/stats-{PROC}.csv", "a")
        file.writelines(f"{record['time']},{record['success']},{nr_coros}\n" for record in self.stats)
        self.stats = []


class Queue:
    def __init__(self):
        self.queue = asyncio.Queue()
    
    async def put(self, item):
        await self.queue.put(item)

    async def pop(self):
        return await self.queue.get()

class IngesterPool:
    def __init__(self, index, client):
        self.index = index
        self.queue = Queue()
        self.coordinator = Coordinator(self)
        self.client = client
        self.process_count = 5 # int((1 + MAX_PARALLEL_PROCESSES) / 2) # TODO: lalala why?
        self.ingesters = []
        self.ingester_coros = []

    async def run(self):
        await self.watch() 

    async def watch(self):
        print(f"Watching with {self.process_count} process")
        
        for _ in range(self.process_count):
            ingester = Ingester(self.index, self.coordinator, self.client)
            self.ingesters.append(ingester)
            self.ingester_coros.append(asyncio.create_task(ingester.watch(self.queue)))

        while len(self.ingester_coros) > 0:
            await asyncio.gather(*self.ingester_coros)

    async def allocate(self):
        print(f"Allocating one more ingester")
        ingester = Ingester(self.index, self.coordinator, self.client)
        self.ingesters.append(ingester)

        self.ingester_coros.append(asyncio.create_task(ingester.watch(self.queue)))


    async def close(self):
        for ingester in self.ingesters:
            await ingester.close()

        for ingester_coro in self.ingester_coros:
            ingester_coro.cancel()

        self.ingesters = []


class Ingester:
    def __init__(self, index, coordinator, client):
        self.running = False
        self.flush_threshold = BATCH_SIZE
        self.index = index
        self.coordinator = coordinator
        self.client = client
        self.watch_task = None

    async def watch(self, queue):
        print(f"[INGESTER {self}] Watching over {queue}")
        self.running = True

        last_flush = datetime.now()
        items = []

        while self.running:
            doc = await queue.pop()

            items.append(doc)

            if len(items) > self.flush_threshold:
                await self.flush(items)
                items = []
                last_flush = datetime.now()
                print(f"FLUSHED at {last_flush}")

    async def close(self):
        self.running = False
        if self.watch_task is not None:
            self.watch_task.cancel()

    async def flush(self, items):
        ops = []

        try: 
            for doc in items:
                ops.append({"index": {"_index": self.index, "_id": doc.pop("_id")}})
                ops.append(doc)

            print(f"[INGESTER {self}] SENDING A BATCH")
            resp = await self.client.bulk(operations=ops, pipeline="elser-v2-books")
            if resp['errors']:
                print(f"[INGESTER {self}] HAS ERRORS: {resp}")
                print(resp['errors'])
                await self.coordinator.ingestion_failed(self)
                await asyncio.sleep(10)
            else:
                await self.coordinator.ingestion_succeeded(self)
                print(f"[INGESTER {self}] DONE, Errors: {resp['errors']}")
        except ConnectionTimeout:
                await self.coordinator.ingestion_failed(self)
                print(f"[INGESTER {self}] Timed out!")
                await asyncio.sleep(10)
        except ApiError as e:
            await self.coordinator.ingestion_failed(self)
            if e.status_code == 429:
                # Slow down, cowboy!
                print(f"[INGESTER {self}] Throttled!")
                await asyncio.sleep(10)
            else:
                print(e)
        except Exception as e:
            await self.coordinator.ingestion_failed(self)
            print(f"[INGESTER {self}] Disconnected!")


class Coordinator:
    def __init__(self, ingester_pool):
        self.min_process_count = 1
        self.max_process_count = MAX_PARALLEL_PROCESSES
        self.ingester_pool = ingester_pool
        self.successful_batches = 0
        self.failed_batches = 0


    async def ingestion_failed(self, ingester):
        self.failed_batches += 1
        if self.failed_batches % 10 == 0:
            print(f"Failed batches: {self.failed_batches}")


    async def ingestion_succeeded(self, ingester):
        self.successful_batches += 1
        if self.successful_batches % 10 == 0:
            print(f"Successful batches: {self.successful_batches}")
            if self.failed_batches == 0:
                if self.ingester_pool.process_count < self.max_process_count:
                    await self.ingester_pool.allocate()
                else:
                    print(f"Already allocated max ingesters, skipping")


async def main():
    client = AsyncElasticsearch(
        # ["http://elastic:changeme@localhost:9200"],
        request_timeout=30,
        retry_on_timeout=True
    )

    await client.indices.create(index=INDEX, ignore=400)
    print(f"Ensured that index {INDEX} exists")

    fake = Faker()
    cached_large_text = fake.paragraph(nb_sentences=500)
    
    ingester_pool = IngesterPool(INDEX, client)

    async def generator():
        while True:
            doc = {
                "_id": fake.bothify(text='????-########'),
                "public_author_works_authors_edition_isbns_editions_works_title": cached_large_text,
                "public_author_works_authors_edition_isbns_editions_works_description": fake.paragraph(nb_sentences=1)
            }
            await asyncio.sleep(0)
            await ingester_pool.queue.put(doc)

    await asyncio.gather(ingester_pool.run(), generator())

    resp = await client.search(
        index=INDEX,
        query={"match_all": {}},
        size=20,
    )
    print("Got results:")
    print(resp)

    await ingester_pool.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

