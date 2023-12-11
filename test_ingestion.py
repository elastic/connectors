import asyncio
from elasticsearch import AsyncElasticsearch, ApiError
from elastic_transport import ConnectionTimeout
from faker import Faker
import functools
from datetime import datetime, timedelta
import random

INDEX="testing-script-ingestion"
BATCH_SIZE=350
MAX_PARALLEL_PROCESSES=25

PROC = random.randint(0, 100000000)

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
        self.process_count = MAX_PARALLEL_PROCESSES
        self.ingesters = []

    async def run(self, yielder):
       await self.watch() 

       for doc in yielder:
            await self.queue.put(doc)

    async def watch(self):
        for i in range(self.process_count):
            ingester = Ingester(self, self.index, self.coordinator, self.client)
            self.ingesters.append(ingester)
            await ingester.watch(self.queue)


class Ingester:
    def __init__(self, index, coordinator, client):
        self.running = False
        self.flush_threshold = BATCH_SIZE
        self.flush_timeout_threshold = datetime.timediff(seconds=1)
        self.index = index
        self.coordinator = coordinator
        self.client = client

    async def watch(self, queue):
        self.running = True

        last_flush = datetime.now()
        items = []

        while self.running:
            doc = await queue.pop()

            items.append(doc)
            
            if len(items) > self.flush_threshold:
                self.flush(items)

    async  def close(self):
        self.running = False

    async def flush(self, items):
        ops = []

        try: 
            for doc in items:
                ops.append({"index": {"_index": self.index, "_id": doc.pop("_id")}})
                ops.append(doc)

                resp = await self.client.bulk(operations=ops)
                if resp['errors']:
                    print(f"[INGESTER {self}] HAS ERRORS")
                    self.coordinator.ingestion_failed(self)
                    await asyncio.sleep(10)
                else:
                    self.coordinator.ingestion_succeeded(self)
                    print(f"[INGESTER {self}] DONE, Errors: {resp['errors']}")
        except ConnectionTimeout:
                print(f"[INGESTER {self}] Timed out!")
                self.stats.failure()
                await asyncio.sleep(10)
        except ApiError as e:
            if e.status_code == 429:
                # Slow down, cowboy!
                print(f"[INGESTER {self}] Throttled!")
                self.stats.failure()
                await asyncio.sleep(10)
            else:
                raise


class Coordinator:
    def __init__(self, ingester_pool):
        self.min_process_count = 1
        self.max_process_count = MAX_PARALLEL_PROCESSES
        self.ingester_pool = ingester_pool

    def ingestion_failed(self, ingester):
        pass

    def ingestion_succeeded(self, ingester):
        pass


class Ingester:
    def __init__(self):
        self.client = AsyncElasticsearch(
            # ["http://elastic:changeme@localhost:9200"],
            request_timeout=30,
            retry_on_timeout=True
        )
        self.fake = Faker()
        self.cached_random_text = self.fake.paragraph(nb_sentences=100)
        self.parallel_process_count = MAX_PARALLEL_PROCESSES
        self.stats = IngestionStats()


    async def ensure_content_idx_exists(self, idx):
        await self.client.indices.create(index=idx, ignore=400)
        print(f"Ensured that index {idx} exists")


    def get_random_record(self):
        return {
            "_id": self.fake.bothify(text='????-########'),
            "text": self.cached_random_text
        }


    async def ingest(self, index, ingester_idx):
        while True:
            try:
                if ingester_idx > self.parallel_process_count:
                    await asyncio.sleep(10)
                    await asyncio.sleep(10)
                    print(f"[INGESTER {ingester_idx}] Not working")
                    return

                ops = []
                for i in range(BATCH_SIZE):
                    record = self.get_random_record()
                    
                    ops.append({"index": {"_index": index, "_id": record.pop("_id")}})
                    ops.append(record)

                print(f"[INGESTER {ingester_idx}] Ingesting yet another batch of {BATCH_SIZE} docs")
                resp = await self.client.bulk(operations=ops)
                if resp['errors']:
                    print(f"[INGESTER {ingester_idx}] HAS ERRORS")
                    self.stats.failure()
                    await asyncio.sleep(10)
                else:
                    self.stats.success()
                    print(f"[INGESTER {ingester_idx}] DONE, Errors: {resp['errors']}")
            except ConnectionTimeout:
                    print(f"[INGESTER {ingester_idx}] Timed out!")
                    self.stats.failure()
                    await asyncio.sleep(10)
            except ApiError as e:
                if e.status_code == 429:
                    # Slow down, cowboy!
                    print(f"[INGESTER {ingester_idx}] Throttled!")
                    self.stats.failure()
                    await asyncio.sleep(10)
                else:
                    raise


    async def run(self):
        ingest_coros = []
        for i in range(self.parallel_process_count):
            task = asyncio.create_task(self.ingest(INDEX, i))
            ingest_coros.append(task)

        ingest_coros.append(asyncio.create_task(self.monitor()))

        await asyncio.gather(*ingest_coros)


    async def monitor(self):
        while True:
            print(f"[STATS]: running {self.parallel_process_count} processes. Last resize: {self.stats.last_resize}")
            await asyncio.sleep(1)
            self.stats.export_stats(self.parallel_process_count)

            if self.stats.should_downsize():
                self.stats.track_resize()
                self.parallel_process_count -= 1
                print(f"[STATS]: downsized")
            elif self.stats.should_upsize() and self.parallel_process_count < MAX_PARALLEL_PROCESSES:
                self.stats.track_resize()
                self.parallel_process_count += 1
                print(f"[STATS]: upsized")



    async def close(self):
        await self.client.close()


async def main():
    ingester = Ingester()

    await ingester.ensure_content_idx_exists(INDEX)
    await ingester.run()

    resp = await ingester.client.search(
        index=INDEX,
        query={"match_all": {}},
        size=20,
    )
    print("Got results:")
    print(resp)

    await ingester.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

