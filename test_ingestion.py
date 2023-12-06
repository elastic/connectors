import asyncio
from elasticsearch import AsyncElasticsearch
from faker import Faker
import functools

INDEX="testing-script-ingestion"
BATCH_SIZE=25
NUM_PARALLEL_PROCESSES=25

fake = Faker()
random_text = fake.paragraph(nb_sentences=1000)

async def create_if_not_exists(es, idx):
    if await es.indices.exists(index=idx) == True:
        print(f"Index {idx} already exists, skipping")
        return

    await es.index(
        index=idx, document={}, id=".connectors-create-doc"
    )
    await es.delete(index=idx, id=".connectors-create-doc")
    print(f"Created index {idx} successfully")

def get_random_record():
    return {
        "_id": fake.bothify(text='????-########'),
        "text": random_text
    }
    

async def ingest(es, index, ingester_idx):
    while True:
        ops = []
        for i in range(BATCH_SIZE):
            record = get_random_record()
            
            ops.append({"index": {"_index": index, "_id": record.pop("_id")}})
            ops.append(record)

        print(f"[INGESTER {ingester_idx}] Ingesting yet another batch of {BATCH_SIZE} docs")
        resp = await es.bulk(operations=ops)
        print(f"[INGESTER {ingester_idx}] DONE, Errors: {resp['errors']}")


async def main():
    es = AsyncElasticsearch(
        # ["http://elastic:changeme@localhost:9200"],
    )

    await create_if_not_exists(es, INDEX)
    ingest_coros = []
    for i in range(NUM_PARALLEL_PROCESSES):
        task = asyncio.create_task(ingest(es, INDEX, i))
        ingest_coros.append(task)

    await asyncio.gather(*ingest_coros)

    resp = await es.search(
        index=INDEX,
        query={"match_all": {}},
        size=20,
    )
    print("Got results:")
    print(resp)

    await es.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

