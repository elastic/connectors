#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os

import psutil
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from connectors.logger import logger
from connectors.utils import SingletonMeta

FIVE_SECOND_INTERVAL = 5000
UNKNOWN = "unknown"


class ConnectorResource(Resource):
    def __init__(self):
        super().__init__(
            {
                "service.name": "connectors",
                "service.version": os.environ.get("VERSION", UNKNOWN),
            }
        )


# TODO: disable by default
# TODO: README
class Telemetry(metaclass=SingletonMeta):
    def __init__(self):
        self.exporter = None
        self.provider = None
        self.meter = None
        self.running = False
        # TODO: config.yml

    def start(self):
        if self.running:
            logger.info("Telemetry already started")
            return

        logger.debug("Starting telemetry...")
        self.running = True

        # TODO: set OTel loggers

        resource = ConnectorResource()

        # TODO: log telemetry transient errors

        # TODO: make endpoint configurable
        self.exporter = OTLPMetricExporter(endpoint="localhost:4317", insecure=True)

        # TODO: make interval configurable
        reader = PeriodicExportingMetricReader(self.exporter, FIVE_SECOND_INTERVAL)
        self.provider = MeterProvider(metric_readers=[reader], resource=resource)
        metrics.set_meter_provider(self.provider)
        self.meter = metrics.get_meter("connectors.meter")

        self.observe_cpu_usage()
        self.observe_virtual_memory_in_gb()

        logger.info("Successfully started telemetry.")

    def shutdown(self, sig=None):
        self.running = False

        if sig:
            logger.info(
                f"Caught {sig}. Graceful shutdown for {Telemetry.__class__.__name__}."
            )

        self.exporter.shutdown()
        logger.info(f"Shutdown {self.exporter.__class__.__name__}")

        self.provider.shutdown()
        logger.info(f"Shutdown {self.provider.__class__.__name__}")

    def observe_virtual_memory_in_gb(self):
        self.create_observable_gauge(
            name="memory.used_gbs",
            callbacks=[Telemetry._get_virtual_memory_in_gb],
            unit="GB",
            description="Memory usage in GBs",
        )

    def observe_cpu_usage(self):
        self.create_observable_gauge(
            name="cpu.usage",
            unit="%",
            description="CPU usage",
            callbacks=[Telemetry._get_cpu_usage_callback],
        )

    def create_observable_gauge(self, **kwargs):
        name = kwargs.get("name")

        if not self.running:
            logger.error(
                f"Could not add observable gauge '{name}'. Telemetry is not running"
            )
            return

        self.meter.create_observable_gauge(**kwargs)
        # TODO: add to telemetry logger?
        logger.info(f"Created observable gauge '{name}'")

    @staticmethod
    def _get_cpu_usage_callback(_):
        # TODO: use interval? could be blocking?
        yield Observation(psutil.cpu_percent())

    @staticmethod
    def _get_virtual_memory_in_gb():
        # TODO: is this correctly calculated?
        yield Observation(psutil.virtual_memory()[3] / 1000000000)
