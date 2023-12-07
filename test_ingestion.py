import asyncio
from elasticsearch import AsyncElasticsearch, ApiError
from faker import Faker
import functools

INDEX="testing-script-ingestion"
BATCH_SIZE=25
MAX_PARALLEL_PROCESSES=25

class Ingester:
    def __init__(self):
        self.client = AsyncElasticsearch(
            # ["http://elastic:changeme@localhost:9200"],
        )
        self.fake = Faker()
        self.cached_random_text = self.fake.paragraph(nb_sentences=1000)

        self.parallel_process_count = MAX_PARALLEL_PROCESSES

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
                    print(f"[INGESTER {ingester_idx}] Exiting")
                    break

                ops = []
                for i in range(BATCH_SIZE):
                    record = self.get_random_record()
                    
                    ops.append({"index": {"_index": index, "_id": record.pop("_id")}})
                    ops.append(record)

                print(f"[INGESTER {ingester_idx}] Ingesting yet another batch of {BATCH_SIZE} docs")
                resp = await self.client.bulk(operations=ops)
                print(f"[INGESTER {ingester_idx}] DONE, Errors: {resp['errors']}")
            except ApiError as e:
                if e.status_code == 429:
                    # Slow down, cowboy!
                    await asyncio.sleep(10)
                else:
                    raise


    async def run(self):
        ingest_coros = []
        for i in range(self.parallel_process_count):
            task = asyncio.create_task(self.ingest(INDEX, i))
            ingest_coros.append(task)

        await asyncio.gather(*ingest_coros)

    async def monitor(self):
        while True:
            print(f"[STATS]: running {self.parallel_process_count} processes")
            await asyncio.sleep(1)

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

