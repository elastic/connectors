#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Elasticsearch Mappings Connector

Connects to an Elasticsearch cluster and extracts index mappings as documents.
"""

import json

from connectors.es.client import ESClient
from connectors.source import BaseDataSource


class ElasticsearchMappingsDataSource(BaseDataSource):
    """Elasticsearch Mappings"""

    name = "Elasticsearch Mappings"
    service_type = "elasticsearch_mappings"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        # Build ES client configuration from our configuration
        es_config = {
            "host": self.configuration["host"],
            "ssl": self.configuration.get("ssl", False),
            "verify_certs": self.configuration.get("ssl_verify", True),
        }

        if self.configuration.get("username") and self.configuration.get("password"):
            es_config["username"] = self.configuration["username"]
            es_config["password"] = self.configuration["password"]

        if self.configuration.get("api_key"):
            es_config["api_key"] = self.configuration["api_key"]

        self.es_client = ESClient(es_config)
        self.include_system_indices = self.configuration.get(
            "include_system_indices", False
        )

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "label": "Elasticsearch Host",
                "order": 1,
                "type": "str",
                "value": "http://localhost:9200",
            },
            "username": {
                "label": "Username",
                "order": 2,
                "type": "str",
                "value": "",
                "required": False,
            },
            "password": {
                "label": "Password",
                "order": 3,
                "type": "str",
                "value": "",
                "required": False,
                "sensitive": True,
            },
            "api_key": {
                "label": "API Key",
                "order": 4,
                "type": "str",
                "value": "",
                "required": False,
                "sensitive": True,
            },
            "ssl": {
                "label": "Enable SSL",
                "order": 5,
                "type": "bool",
                "value": False,
            },
            "ssl_verify": {
                "label": "Verify SSL certificate",
                "order": 6,
                "type": "bool",
                "value": True,
            },
            "include_system_indices": {
                "label": "Include system indices (starting with .)",
                "order": 7,
                "type": "bool",
                "value": False,
            },
        }

    async def ping(self):
        """Test connection to Elasticsearch"""
        try:
            response = await self.es_client.ping()
            return response is not None
        except Exception as e:
            self._logger.error(f"Failed to ping Elasticsearch: {e}")
            return False

    async def close(self):
        """Close the ES client"""
        if hasattr(self, "es_client"):
            await self.es_client.close()

    async def _get_indices(self):
        """Get list of indices from Elasticsearch"""
        try:
            # Use _cat/indices API to get index list
            response = await self.es_client.client.cat.indices(format="json", h="index")
            indices = [item["index"] for item in response]

            # Filter out system indices if not requested
            if not self.include_system_indices:
                indices = [idx for idx in indices if not idx.startswith(".")]

            return indices
        except Exception as e:
            self._logger.error(f"Error getting indices: {e}")
            return []

    async def _get_index_mapping(self, index_name):
        """Get mapping for a specific index"""
        try:
            response = await self.es_client.client.indices.get_mapping(index=index_name)
            return response.get(index_name, {}).get("mappings", {})
        except Exception as e:
            self._logger.warning(f"Error getting mapping for {index_name}: {e}")
            return {}

    async def get_docs(self, filtering=None):
        """Yield documents containing index mappings"""
        indices = await self._get_indices()
        self._logger.info(f"Found {len(indices)} indices to process")

        for index_name in indices:
            self._logger.debug(f"Processing mapping for index: {index_name}")

            mapping = await self._get_index_mapping(index_name)

            # Create document with mapping information
            doc = {
                "_id": index_name,  # Use index name as document ID
                "index_name": index_name,
                "mapping": json.dumps(mapping),  # Convert mapping to JSON string
                "source_cluster": self.es_client.configured_host,
            }

            # Extract _meta properties and add them as separate fields
            if mapping and "_meta" in mapping:
                for meta_key, meta_value in mapping["_meta"].items():
                    doc[f"meta_{meta_key}"] = meta_value

            # Extract field descriptions from meta.description properties
            field_descriptions = []
            if mapping and "properties" in mapping:
                field_descriptions = self._extract_field_descriptions(
                    mapping["properties"]
                )

            if field_descriptions:
                doc["field_descriptions"] = field_descriptions

            # No lazy download function needed for this connector
            yield doc, None

    def _extract_field_descriptions(self, properties):
        """Extract field descriptions from meta.description properties"""
        descriptions = []

        for _field_name, field_config in properties.items():
            # Check if this field has a meta.description
            if isinstance(field_config, dict) and "meta" in field_config:
                meta = field_config["meta"]
                if isinstance(meta, dict) and "description" in meta:
                    descriptions.append(meta["description"])

            # Recursively check nested properties
            if isinstance(field_config, dict) and "properties" in field_config:
                nested_descriptions = self._extract_field_descriptions(
                    field_config["properties"]
                )
                descriptions.extend(nested_descriptions)

        return descriptions
