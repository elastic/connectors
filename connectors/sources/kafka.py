#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import json
from itertools import chain

from aiokafka import AIOKafkaConsumer

from connectors.source import BaseDataSource


class KafkaClient:
    def __init__(self, kafka_topic, kafka_bootstrap_servers, kafka_consumer_group):
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_consumer_group = kafka_consumer_group

    def _get_consumer(self, auto_offset_reset="earliest"):
        kafka_consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_consumer_group,
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
            key_deserializer=lambda k: json.loads(k.decode("utf-8")),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        return kafka_consumer

    def set_logger(self, logger_):
        self._logger = logger_

    async def ping(self):
        async with self._get_consumer() as consumer:
            await consumer.topics()


    def _get_pkeys_from_schema(self, schema):
        # Extract the fields from the schema
        fields = schema.get('fields', [])

        # Retrieve all primary key fields
        primary_keys = [field['field'] for field in fields if not field.get('optional')]

        return primary_keys


    def _build_unique_primary_key(self, primary_keys, payload):

        pkey_values = []

        for pkey in primary_keys:
            pkey_values.append(str(payload.get(pkey)))

        return '_'.join(pkey_values)

    async def consume_all_messages(self):
        async with self._get_consumer() as consumer:
            msgs = True
            while msgs:
                msgs = await consumer.getmany(timeout_ms=2500, max_records=10)
                msg_list = msgs.values()
                # same pkey in same parition, so no need to worry about global ordering here
                for msg in list(chain.from_iterable(msg_list)):
                    primary_keys = self._get_pkeys_from_schema(schema=msg.key.get("schema", {}))
                    message_id = self._build_unique_primary_key(
                        primary_keys=primary_keys,
                        payload=msg.key.get("payload", {})
                    )
                    yield message_id, msg.value, msg.timestamp


class KafkaDataSource(BaseDataSource):
    """Apache Kafka Data Source"""

    name = "Apache Kafka"
    service_type = "kafka"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        self.client = KafkaClient(
            kafka_topic=self.configuration["kafka_topic"],
            kafka_bootstrap_servers=f'{self.configuration["kafka_host"]}:{self.configuration["kafka_port"]}',
            kafka_consumer_group=self.configuration["kafka_consumer_group"],
        )

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for the GMail connector.
        Returns:
            dict: Default configuration.
        """

        return {
            "kafka_host": {
                "display": "text",
                "label": "Kafka cluster host",
                "order": 1,
                "required": True,
                "type": "str",
            },
            "kafka_port": {
                "display": "text",
                "label": "Kafka cluster port",
                "order": 2,
                "required": True,
                "type": "int",
            },
            "kafka_topic": {
                "display": "text",
                "label": "Kafka topic",
                "order": 3,
                "required": True,
                "type": "str",
            },
            "kafka_consumer_group": {
                "display": "next",
                "label": "Kafka consumer group",
                "order": 4,
                "required": True,
                "tooltip": "Kafka consumer group defined by user, make sure it's unique",
                "type": "str",
            },
        }

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    async def ping(self):
        """Verify the connection with Kafka"""
        try:
            await self.client.ping()
            self._logger.info("Successfully connected to Kafka.")
        except Exception:
            self._logger.exception("Error while connecting to Kafka.")
            raise

    def _process_message(self, message_id, message_value, message_timestamp):
        return {
            "_id": message_id,
            "_timestamp": message_timestamp,
            **message_value.get("payload").get("after", {}),
        }

    async def get_docs(self, filtering=None):
        async for message_key, message_value, message_timestamp in self.client.consume_all_messages():
            processed_message = self._process_message(
                message_key, message_value, message_timestamp
            )
            yield processed_message, None
