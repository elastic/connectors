from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import get_pem_format
from contextlib import contextmanager
from copy import deepcopy
from bson import OLD_UUID_SUBTYPE, Binary, DBRef, Decimal128, ObjectId
from tempfile import NamedTemporaryFile
from fastjsonschema import JsonSchemaValueException
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure
from bson.binary import UUID_SUBTYPE
from datetime import datetime

from connectors.sources.mongo.mongoconnectorutils import MongoAdvancedRulesValidator

import os
import urllib.parse

class MongoDataSource(BaseDataSource):
    """MongoDB"""

    name = "MongoDB"
    service_type = "mongodb"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        self.client = None
        self.host = self.configuration["host"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.ssl_ca = self.configuration["ssl_ca"]
        self.user = self.configuration["user"]
        self.password = self.configuration["password"]
        self.tls_insecure = self.configuration["tls_insecure"]
        self.collection = None

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "label": "Server hostname",
                "order": 1,
                "type": "str",
            },
            "user": {
                "label": "Username",
                "order": 2,
                "required": False,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 3,
                "required": False,
                "sensitive": True,
                "type": "str",
            },
            "database": {"label": "Database", "order": 4, "type": "str"},
            "collection": {
                "label": "Collection",
                "order": 5,
                "type": "str",
            },
            "direct_connection": {
                "display": "toggle",
                "label": "Direct connection",
                "order": 6,
                "type": "bool",
                "value": False,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "SSL/TLS Connection",
                "order": 7,
                "tooltip": "This option establishes a secure connection to the MongoDB server using SSL/TLS encryption. Ensure that your MongoDB deployment supports SSL/TLS connections. Enable if MongoDB cluster uses DNS SRV records.",
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "Certificate Authority (.pem)",
                "order": 8,
                "required": False,
                "tooltip": "Specifies the root certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the MongoDB instance.",
                "type": "str",
            },
            "tls_insecure": {
                "display": "toggle",
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "Skip certificate verification",
                "order": 9,
                "tooltip": "This option skips certificate validation for TLS/SSL connections to your MongoDB server. We strongly recommend setting this option to 'disable'.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    @contextmanager
    def get_client(self):
        certfile = ""
        try:
            client_params = {}

            if self.configuration["direct_connection"]:
                client_params["directConnection"] = True

            if len(self.user) > 0 or len(self.password) > 0:
                client_params["username"] = self.user
                client_params["password"] = self.password

            if self.configuration["ssl_enabled"]:
                client_params["tls"] = True
                if self.ssl_ca:
                    pem_certificates = get_pem_format(key=self.ssl_ca)
                    with NamedTemporaryFile(
                        mode="w", suffix=".pem", delete=False
                    ) as cert:
                        cert.write(pem_certificates)
                        certfile = cert.name
                    client_params["tlsCAFile"] = certfile
                client_params["tlsInsecure"] = self.tls_insecure
            else:
                client_params["tls"] = False

            yield AsyncIOMotorClient(self.host, **client_params)
        finally:
            if os.path.exists(certfile):
                try:
                    os.remove(certfile)
                except Exception as exception:
                    self._logger.warning(
                        f"Something went wrong while removing temporary certificate file. Exception: {exception}",
                        exc_info=True,
                    )

    def advanced_rules_validators(self):
        return [MongoAdvancedRulesValidator()]

    async def ping(self):
        with self.get_client() as client:
            await client.admin.command("ping")

    def remove_temp_file(self, temp_file): # type: ignore
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while removing temporary certificate file. Exception: {exception}",
                    exc_info=True,
                )

    # TODO: That's a lot of work. Find a better way
    def serialize(self, doc):
        def _serialize(value):
            if isinstance(value, ObjectId):
                value = str(value)
            elif isinstance(value, (list, tuple)):
                value = [_serialize(item) for item in value]
            elif isinstance(value, dict):
                for key, svalue in value.items():
                    value[key] = _serialize(svalue)
            elif isinstance(value, datetime):
                value = value.isoformat()
            elif isinstance(value, Decimal128):
                value = value.to_decimal()
            elif isinstance(value, DBRef):
                value = _serialize(value.as_doc().to_dict())
            elif isinstance(value, Binary):
                # UUID_SUBTYPE is guaranteed to properly be serialized cross-platform and cross-driver
                if value.subtype == UUID_SUBTYPE:
                    value = value.as_uuid()
                # OLD_UUID_SUBTYPE is platform-specific. If Java writes old UUID and Python reads it they won't match
                elif value.subtype == OLD_UUID_SUBTYPE:
                    self._logger.warning(
                        f"Unexpected uuid subtype, skipping serialization of a field {value}. Please provide uuidRepresentation=VALUE with correct value in connection string"
                    )
                    return None
            return value

        for key, value in doc.items():
            doc[key] = _serialize(value)

        return doc

    async def get_docs(self, filtering=None): # type: ignore
        with self.get_client() as client:
            db = client[self.configuration["database"]]
            collection = db[self.configuration["collection"]]

            if filtering and filtering.has_advanced_rules():
                advanced_rules = filtering.get_advanced_rules()

                if "find" in advanced_rules:
                    find_kwargs = advanced_rules.get("find", {})

                    async for doc in collection.find(**find_kwargs):
                        yield self.serialize(doc), None

                elif "aggregate" in advanced_rules:
                    aggregate_kwargs = deepcopy(advanced_rules.get("aggregate", {}))
                    pipeline = aggregate_kwargs.pop("pipeline", [])

                    async for doc in collection.aggregate(
                        pipeline=pipeline, **aggregate_kwargs
                    ):
                        yield self.serialize(doc), None
            else:
                async for doc in collection.find():
                    yield self.serialize(doc), None

    def check_conflicting_values(self, value):
        if value == "true":
            value = True
        elif value == "false":
            value = False
        else:
            value = None

        if value != self.ssl_enabled:
            msg = "The value of SSL/TLS must be the same in the hostname and configuration field."
            raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        await super().validate_config()
        parsed_url = urllib.parse.urlparse(self.host)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        if "ssl" in query_params:
            ssl_value = query_params.get("ssl", ["None"])[0]
            self.check_conflicting_values(ssl_value)

        if "tls" in query_params:
            ssl_value = query_params.get("tls", ["None"])[0]
            self.check_conflicting_values(ssl_value)

        user = self.configuration["user"]
        configured_database_name = self.configuration["database"]
        configured_collection_name = self.configuration["collection"]

        with self.get_client() as client:
            # First check if collection is accessible
            try:
                # This works on both standalone and Managed mongo in the same way
                await client[configured_database_name].validate_collection(
                    configured_collection_name
                )
                return
            except OperationFailure:
                self._logger.debug(
                    f"Unable to validate '{configured_database_name}.{configured_collection_name}' as user '{user}'"
                )

            # If it's not accessible, try to make a good user-friendly error message
            try:
                # We will try to access some databases/collections to give a friendly message
                # That will suggest the name of existing collection - but only if the user
                # that we use to log in into MongoDB has access to it
                existing_database_names = await client.list_database_names()

                self._logger.debug(f"Existing databases: {existing_database_names}")

                if configured_database_name not in existing_database_names:
                    msg = f"Database '{configured_database_name}' does not exist. Existing databases: {', '.join(existing_database_names)}"
                    raise ConfigurableFieldValueError(msg)

                database = client[configured_database_name]

                existing_collection_names = await database.list_collection_names()
                self._logger.debug(
                    f"Existing collections in {configured_database_name}: {existing_collection_names}"
                )

                if configured_collection_name not in existing_collection_names:
                    msg = f"Collection '{configured_collection_name}' does not exist within database '{configured_database_name}'. Existing collections: {', '.join(existing_collection_names)}"
                    raise ConfigurableFieldValueError(msg)
                else:
                    self._logger.debug(
                        f"Found {configured_database_name}.{configured_collection_name} as user {user}"
                    )
            except OperationFailure as e:
                # This happens if the user has no access to operations to list collection/database names
                # Managed MongoDB never gets here, but if we're running against a standalone mongo
                # Then this code can trigger
                msg = f"Database '{configured_database_name}' or collection '{configured_collection_name}' is not accessible by user '{user}'. Verify that these database and collection exist, and specified user has access to it"
                raise ConfigurableFieldValueError(msg) from e
