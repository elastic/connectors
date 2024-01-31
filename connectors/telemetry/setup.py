#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import gc
import os
import subprocess
from abc import abstractmethod

import psutil
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from connectors.logger import logger
from connectors.utils import running_ftest, running_on_buildkite, SingletonMeta

FIVE_SECOND_INTERVAL = 5000
UNKNOWN = "unknown"


def get_git_commit():
    # TODO: only works when running from source
    try:
        commit_hash_bytes = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
        commit_hash = commit_hash_bytes.decode('utf-8')

        return f"https://github.com/elastic/connectors/commit/{commit_hash}"
    except subprocess.CalledProcessError:
        logger.error("Failed to retrieve Git commit hash")
        return ""


# TODO: do not repeat calls to disk_io_counters
class BasicResource(Resource):

    def __init__(self):
        super().__init__({
            "service.name": "connectors",
            "service.version": os.environ.get("VERSION", UNKNOWN),
            "commit": get_git_commit()
        })


class FTestResource(Resource):
    def __init__(self):
        super().__init__({
            "ftest.tested_connectors": [os.environ.get("CONNECTOR_UNDER_FTEST", UNKNOWN)]
        })


class BuildkiteResource(Resource):
    def __init__(self):
        super().__init__({
            "buildkite.build_number": os.environ.get("BUILDKITE_BUILD_NUMBER", -1),
            "buildkite.build_url": os.environ.get("BUILDKITE_BUILD_URL", "")
        })


class MeterDecorator:

    def __init__(self, meter):
        self.meter = meter
        self.collected_metrics = []

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError()

    @abstractmethod
    def decorate(self):
        raise NotImplementedError()

    def decorate_with_observable_gauge(self, **kwargs):
        if "name" not in kwargs:
            raise ValueError("'name' must be specified")

        self.collected_metrics.append(**kwargs["name"])
        self.meter.create_observable_gauge(**kwargs)


class ResourceUtilizationMeterDecorator(MeterDecorator):

    def __init__(self, meter):
        super().__init__(meter)
        self.process_info = psutil.Process(os.getpid())
        disk_io_counters = self.process_info.io_counters()

        self.initial_read_count = disk_io_counters.read_count
        self.initial_write_count = disk_io_counters.write_count

    @property
    def name(self):
        return "Resource utilization"

    # TODO: network I/O
    # TODO: RSS
    # TODO: other system metrics
    # TODO: disk I/Ox
    def decorate(self):
        self.decorate_with_observable_gauge(name="cpu.usage",
                                            unit="%",
                                            description="CPU usage",
                                            callbacks=[ResourceUtilizationMeterDecorator._get_cpu_usage_callback])

        self.decorate_with_observable_gauge(name="memory.used_gbs",
                                            callbacks=[ResourceUtilizationMeterDecorator._get_virtual_memory_in_gb],
                                            unit="GB",
                                            description="Memory usage in GBs")

        self.decorate_with_observable_gauge(name="process.disk_io.read_count",
                                            callbacks=[self._get_disk_io_read_count],
                                            unit="1",
                                            description="Number of disk read counts (process)")

        self.decorate_with_observable_gauge(name="process.disk_io.write_count",
                                            callbacks=[self._get_disk_io_write_count],
                                            unit="1",
                                            description="Number of disk write counts (process)")

        self.decorate_with_observable_gauge(name="process.num_file_descriptors",
                                            callbacks=[self._get_num_file_descriptors],
                                            unit="1",
                                            description="Number of open file descriptors (process)")

        self.decorate_with_observable_gauge(name="process.num_threads",
                                            callbacks=[self._get_num_threads],
                                            unit="1",
                                            description="Number of running threads (process)")

    def _get_num_file_descriptors(self, _):
        yield Observation(self.process_info.num_fds())

    def _get_num_threads(self, _):
        yield Observation(self.process_info.num_threads())

    def _get_disk_io_write_count(self, _):
        yield Observation(self.process_info.io_counters().write_count - self.initial_write_count)

    def _get_disk_io_read_count(self, _):
        yield Observation(self.process_info.io_counters().read_count - self.initial_read_count)

    @staticmethod
    def _get_virtual_memory_in_gb():
        # TODO: is this correctly calculated?
        yield Observation(psutil.virtual_memory()[3] / 1000000000)

    @staticmethod
    def _get_cpu_usage_callback(_):
        # TODO: use interval? could be blocking?
        yield Observation(psutil.cpu_percent())


class AsyncStatsMeterDecorator(MeterDecorator):

    def __init__(self, meter, event_loop):
        super().__init__(meter)
        self.event_loop = event_loop

    @property
    def name(self):
        return "Async stats"

    def decorate(self):
        self.decorate_with_observable_gauge(name="async_io.running_tasks",
                                            unit="1",
                                            callbacks=[self._get_running_async_tasks],
                                            description="Number of running async tasks")

    def _get_running_async_tasks(self, _):
        yield Observation(len(asyncio.all_tasks(self.event_loop)))


class GarbageCollectionMeterDecorator(MeterDecorator):

    @property
    def name(self):
        return "Garbage collection"

    def decorate(self):
        self.decorate_with_observable_gauge(name="gc.num_of_tracked_objects",
                                            unit="1",
                                            callbacks=[GarbageCollectionMeterDecorator._get_num_of_tracked_objects],
                                            description="Number of all tracked objects")

        self.decorate_with_observable_gauge(name="gc.num_of_tracked_objects_first_gen",
                                            unit="1",
                                            callbacks=[GarbageCollectionMeterDecorator._get_num_of_tracked_objects_first_gen],
                                            description="Number of tracked objects in first generation")

        self.decorate_with_observable_gauge(name="gc.num_of_tracked_objects_second_gen",
                                            unit="1",
                                            callbacks=[
                                                GarbageCollectionMeterDecorator._get_num_of_tracked_objects_second_gen],
                                            description="Number of tracked objects in second generation"
                                            )

        self.decorate_with_observable_gauge(name="gc.num_of_tracked_objects_third_gen",
                                            unit="1",
                                            callbacks=[
                                                GarbageCollectionMeterDecorator._get_num_of_tracked_objects_third_gen],
                                            description="Number of tracked objects in third generation")

    @staticmethod
    def _get_num_of_tracked_objects():
        yield Observation(len(gc.get_objects()))

    @staticmethod
    def _get_num_of_tracked_objects_first_gen():
        yield Observation(len(gc.get_objects(0)))

    @staticmethod
    def _get_num_of_tracked_objects_second_gen():
        yield Observation(len(gc.get_objects(1)))

    @staticmethod
    def _get_num_of_tracked_objects_third_gen():
        yield Observation(len(gc.get_objects(2)))


# TODO: throughput, bulk API, get_docs, get_docs_incrementally, get_access_control

class Telemetry(metaclass=SingletonMeta):
    def __init__(self, event_loop):
        self.running = False
        self.event_loop = event_loop
        # TODO: which test ran?
        # TODO: config.yml
        # TODO: signal handling? doesn't stop

        logger.debug("Successfully setup telemetry.")

    def start(self):
        if self.running:
            logger.info("Telemetry already started")
            return

        logger.info("Starting telemetry...")
        self.running = True

        resource = BasicResource()

        if running_ftest():
            logger.info("Adding FTEST metadata to telemetry")
            resource = resource.merge(FTestResource())

        if running_on_buildkite():
            logger.info("Adding buildkite metadata to telemetry")
            resource = resource.merge(BuildkiteResource())

        # TODO: different collectors for different environments?
        # TODO: log telemetry transient errors

        # TODO: insecure?
        # TODO: make endpoint configurable
        exporter = OTLPMetricExporter(endpoint="localhost:4317", insecure=True)

        # TODO: make interval configurable
        reader = PeriodicExportingMetricReader(exporter, FIVE_SECOND_INTERVAL)
        provider = MeterProvider(metric_readers=[reader], resource=resource)
        metrics.set_meter_provider(provider)
        meter = metrics.get_meter("connectors.meter")

        meter_decorators = [
            AsyncStatsMeterDecorator(meter, self.event_loop),
            GarbageCollectionMeterDecorator(meter),
            ResourceUtilizationMeterDecorator(meter)
        ]

        for meter_decorator in meter_decorators:
            meter_decorator.decorate()
            logger.info(f"[{meter_decorator.name} metrics]: Collecting {','.join(meter_decorator.collected_metrics)}.")

        logger.debug("Successfully started telemetry.")

    # TODO: stopping logic?

    # TODO: event loop lag
