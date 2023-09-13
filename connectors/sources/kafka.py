#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.logger import logger
from connectors.source import BaseDataSource

import json

from aiokafka import AIOKafkaConsumer



class KafkaClient:

    def __init__(self, kafka_topic, kafka_bootstrap_servers, kafka_consumer_group, auto_offset_reset='latest'):
        self.kafka_consumer = AIOKafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=kafka_consumer_group,
            enable_auto_commit=True,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    async def connect_to_kafka(self):
        self.kafka_consumer.start()


class KafkaDataSource(BaseDataSource):
    """Apache Kafka Data Source"""

    name = "Apache Kafka"
    service_type = "kafka"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for the GMail connector.
        Returns:
            dict: Default configuration.
        """

        return {
            "kafka_host": {
                "display": "textarea",
                "label": "Kafka cluster host",
                "order": 1,
                "required": True,
                "type": "str",
            },
            "kafka_port": {
                "display": "textarea",
                "label": "Kafka cluster port",
                "order": 2,
                "required": True,
                "type": "int",
            },
            "kafka_topic": {
                "display": "textarea",
                "label": "Kafka topic",
                "order": 3,
                "required": True,
                "type": "str",
            },
            "kafka_consumer_group": {
                "display": "textarea",
                "label": "Kafka consumer group",
                "order": 4,
                "required": True,
                "tooltip": "Kafka consumer group defined by user, make sure it's unique",
                "type": "str",
            }
        }

    def _set_internal_logger(self):
        # self.client.set_logger(self._logger)
        pass
