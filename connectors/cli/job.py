import asyncio

from elasticsearch import ApiError

from connectors.es.client import ESManagementClient
from connectors.protocol import (
    CONCRETE_CONNECTORS_INDEX,
    CONCRETE_JOBS_INDEX,
    ConnectorIndex,
    JobStatus,
    JobTriggerMethod,
    JobType,
    Sort,
    SyncJobIndex,
)


class Job:
    def __init__(self, config):
        self.config = config
        self.es_client = ESManagementClient(self.config)
        self.sync_job_index = SyncJobIndex(self.config)
        self.connector_index = ConnectorIndex(self.config)

    def list_jobs(self, connector_id=None, index_name=None, job_id=None):
        return asyncio.run(self.__async_list_jobs(connector_id, index_name, job_id))

    def cancel(self, connector_id=None, index_name=None, job_id=None):
        return asyncio.run(self.__async_cancel_jobs(connector_id, index_name, job_id))

    def start(self, connector_id, job_type):
        return asyncio.run(self.__async_start(connector_id, job_type))

    def job(self, job_id):
        return asyncio.run(self.__async_job(job_id))

    async def __async_job(self, job_id):
        try:
            await self.es_client.ensure_exists(
                indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX]
            )
            job = await self.sync_job_index.fetch_by_id(job_id)
            return job
        finally:
            await self.sync_job_index.close()
            await self.es_client.close()

    async def __async_start(self, connector_id, job_type):
        try:
            connector = await self.connector_index.fetch_by_id(connector_id)
            job_id = await self.sync_job_index.create(
                connector=connector,
                trigger_method=JobTriggerMethod.ON_DEMAND,
                job_type=JobType(job_type),
            )

            return job_id
        finally:
            await self.sync_job_index.close()
            await self.connector_index.close()
            await self.es_client.close()

    async def __async_list_jobs(self, connector_id, index_name, job_id):
        try:
            await self.es_client.ensure_exists(
                indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX]
            )
            jobs = self.sync_job_index.get_all_docs(
                query=self.__job_list_query(connector_id, index_name, job_id),
                sort=self.__job_list_sort(),
            )

            return [job async for job in jobs]

        # TODO catch exceptions
        finally:
            await self.sync_job_index.close()
            await self.es_client.close()

    async def __async_cancel_jobs(self, connector_id, index_name, job_id):
        try:
            jobs = await self.__async_list_jobs(connector_id, index_name, job_id)

            for job in jobs:
                await job._terminate(JobStatus.CANCELING)

            return True
        except ApiError:
            return False
        finally:
            await self.sync_job_index.close()
            await self.es_client.close()

    def __job_list_query(self, connector_id, index_name, job_id):
        if job_id:
            return {"bool": {"must": [{"term": {"_id": job_id}}]}}

        if index_name:
            return {
                "bool": {"filter": [{"term": {"connector.index_name": index_name}}]}
            }

        if connector_id:
            return {"bool": {"must": [{"term": {"connector.id": connector_id}}]}}

        return None

    def __job_list_sort(self):
        return [{"created_at": Sort.ASC.value}]
