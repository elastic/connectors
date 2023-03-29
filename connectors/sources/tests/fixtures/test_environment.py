#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from abc import ABC, abstractmethod

from connectors.logger import logger


class TestEnvironment(ABC):

    name = "None"

    @abstractmethod
    async def setup(self):
        raise NotImplementedError

    @abstractmethod
    async def teardown(self):
        raise NotImplementedError


class DockerTestEnvironment(TestEnvironment):
    DOCKER_COMPOSE_UP_DETACHED = "docker compose up -d"
    DOCKER_COMPOSE_DOWN_REMOVE_VOLUMES = "docker compose down --volumes"

    name = "Docker"

    def __init__(self, data_source_dir):
        # navigate to the directory containing the docker-compose.yml file
        os.chdir(os.path.join(os.path.dirname(__file__), data_source_dir))

    async def setup(self):
        # TODO: check for docker compose existence
        logger.info("Starting containers from docker compose...")
        os.system(DockerTestEnvironment.DOCKER_COMPOSE_UP_DETACHED)
        logger.info("Started containers from docker compose.")

    async def teardown(self):
        # TODO: check again for docker compose existence
        logger.info("Stopping containers from docker compose...")
        os.system(DockerTestEnvironment.DOCKER_COMPOSE_DOWN_REMOVE_VOLUMES)
        logger.info("Stopped container from docker compose.")
