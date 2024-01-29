import asyncio
from collections import OrderedDict

from connectors.es.management_client import ESManagementClient
from connectors.es.settings import DEFAULT_LANGUAGE
from connectors.protocol import (
    CONCRETE_CONNECTORS_INDEX,
    CONCRETE_JOBS_INDEX,
    CONNECTORS_ACCESS_CONTROL_INDEX_PREFIX,
    ConnectorIndex,
)
from connectors.source import get_source_klass
from connectors.utils import iso_utc


class IndexAlreadyExists(Exception):
    pass


class Connector:
    def __init__(self, config):
        self.config = config

        # initialize ES client
        self.es_management_client = ESManagementClient(self.config)

        self.connector_index = ConnectorIndex(self.config)

    async def list_connectors(self):
        # TODO move this on top
        try:
            await self.es_management_client.ensure_exists(
                indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX]
            )

            return [
                connector async for connector in self.connector_index.all_connectors()
            ]

        # TODO catch exceptions
        finally:
            await self.connector_index.close()
            await self.es_management_client.close()

    def service_type_configuration(self, source_class):
        source_klass = get_source_klass(source_class)
        configuration = source_klass.get_simple_configuration()

        return OrderedDict(sorted(configuration.items(), key=lambda x: x[1]["order"]))

    def create(
        self,
        index_name,
        service_type,
        configuration,
        is_native,
        language=DEFAULT_LANGUAGE,
        from_index=False,
    ):
        return asyncio.run(
            self.__create(
                index_name,
                service_type,
                configuration,
                is_native,
                language,
                from_index,
            )
        )

    async def __create(
        self,
        index_name,
        service_type,
        configuration,
        is_native,
        language=DEFAULT_LANGUAGE,
        from_index=False,
    ):
        try:
            return await self.__create_connector(
                index_name, service_type, configuration, is_native, language, from_index
            )
        except Exception as e:
            raise e
        finally:
            await self.es_management_client.close()

    async def __create_connector(
        self, index_name, service_type, configuration, is_native, language, from_index
    ):
        try:
            await self.es_management_client.ensure_exists(
                indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX]
            )
            timestamp = iso_utc()

            if not from_index:
                await self.es_management_client.create_content_index(
                    index_name, language
                )

            api_key_id = None
            api_key_encoded = None
            api_key_error = None
            api_key_skipped = False

            # Skip creating an API key if the CLI is authenticated with an API key or if the connector is native
            #
            if "api_key" in self.config or not is_native:
                api_key_skipped = True
            else:
                try:
                    api_key = await self.__create_api_key(index_name)
                    api_key_id = api_key["id"]
                    api_key_encoded = api_key["encoded"]
                except Exception as e:
                    api_key_error = f"Could not create a connector-specific API key. Elasticsearch reported the following error {e}"

            # TODO features still required
            doc = {
                "api_key_id": api_key_id,
                "configuration": configuration,
                "index_name": index_name,
                "service_type": service_type,
                "status": "configured",  # TODO use a predefined constant
                "is_native": is_native,
                "language": language,
                "last_access_control_sync_error": None,
                "last_access_control_sync_scheduled_at": None,
                "last_access_control_sync_status": None,
                "last_sync_status": None,
                "last_sync_error": None,
                "last_sync_scheduled_at": None,
                "last_synced": None,
                "last_seen": None,
                "created_at": timestamp,
                "updated_at": timestamp,
                "filtering": self.default_filtering(timestamp),
                "scheduling": self.default_scheduling(),
                "custom_scheduling": {},
                "pipeline": {
                    "extract_binary_content": True,
                    "name": "ent-search-generic-ingestion",
                    "reduce_whitespace": True,
                    "run_ml_inference": True,
                },
                "last_indexed_document_count": 0,
                "last_deleted_document_count": 0,
            }

            connector = await self.connector_index.index(doc)
            return {
                "id": connector["_id"],
                "api_key": api_key_encoded,
                "api_key_skipped": api_key_skipped,
                "api_key_error": api_key_error,
            }
        finally:
            await self.connector_index.close()

    def default_scheduling(self):
        return {
            "access_control": {"enabled": False, "interval": "0 0 0 * * ?"},
            "full": {"enabled": False, "interval": "0 0 0 * * ?"},
            "incremental": {"enabled": False, "interval": "0 0 0 * * ?"},
        }

    def default_filtering(self, timestamp):
        return [
            {
                "active": {
                    "advanced_snippet": {
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "value": {},
                    },
                    "rules": [
                        {
                            "created_at": timestamp,
                            "field": "_",
                            "id": "DEFAULT",
                            "order": 0,
                            "policy": "include",
                            "rule": "regex",
                            "updated_at": timestamp,
                            "value": ".*",
                        }
                    ],
                    "validation": {"errors": [], "state": "valid"},
                },
                "domain": "DEFAULT",
                "draft": {
                    "advanced_snippet": {
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "value": {},
                    },
                    "rules": [
                        {
                            "created_at": timestamp,
                            "field": "_",
                            "id": "DEFAULT",
                            "order": 0,
                            "policy": "include",
                            "rule": "regex",
                            "updated_at": timestamp,
                            "value": ".*",
                        }
                    ],
                    "validation": {"errors": [], "state": "valid"},
                },
            }
        ]

    async def __create_api_key(self, name):
        acl_index_name = f"{CONNECTORS_ACCESS_CONTROL_INDEX_PREFIX}{name}"
        metadata = {"created_by": "Connectors CLI"}
        role_descriptors = {
            f"{name}-connector-role": {
                "cluster": ["monitor"],
                "index": [
                    {
                        "names": [
                            name,
                            acl_index_name,
                            f"{CONCRETE_CONNECTORS_INDEX}*",
                        ],
                        "privileges": ["all"],
                    },
                ],
            },
        }
        try:
            return await self.es_management_client.client.security.create_api_key(
                name=f"{name}-connector",
                role_descriptors=role_descriptors,
                metadata=metadata,
            )
        finally:
            await self.es_management_client.close()
