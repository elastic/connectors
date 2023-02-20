#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the MySQL source class methods"""
import asyncio
import ssl
from unittest import mock
from unittest.mock import AsyncMock

import aiomysql
import pytest

from connectors.byoc import Filter
from connectors.filtering.validation import SyncRuleValidationResult
from connectors.source import DataSourceConfiguration
from connectors.sources.mysql import MySQLAdvancedRulesValidator, MySqlDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator


def immutable_doc(**kwargs):
    return frozenset(kwargs.items())


ADVANCED_SNIPPET = "advanced_snippet"

DB_ONE = "db_1"
DB_TWO = "db_2"

TABLE_ONE = "table1"
TABLE_TWO = "table2"
TABLE_THREE = "table3"

DOC_ONE = immutable_doc(id=1, text="some text 1")
DOC_TWO = immutable_doc(id=2, text="some text 2")
DOC_THREE = immutable_doc(id=3, text="some text 3")
DOC_FOUR = immutable_doc(id=4, text="some text 4")
DOC_FIVE = immutable_doc(id=5, text="some text 5")
DOC_SIX = immutable_doc(id=6, text="some text 6")
DOC_SEVEN = immutable_doc(id=7, text="some text 7")
DOC_EIGHT = immutable_doc(id=8, text="some text 8")

DB_ONE_TABLE_ONE_QUERY_ALL = "query all db one table one"
DB_ONE_TABLE_ONE_QUERY_DOC_ONE = "query doc one"
DB_ONE_TABLE_TWO_QUERY_ALL = "query all db one table two"

DB_TWO_TABLE_ONE_QUERY_ALL = "query all db two table one"
DB_TWO_TABLE_TWO_QUERY_ALL = "query all db two table two"

ALL_DOCS = "all_docs"
ONLY_DOC_ONE = "only_doc_one"

ACCESSIBLE = "accessible"
INACCESSIBLE = "inaccessible"

MYSQL = {
    DB_ONE: {
        TABLE_ONE: {
            DB_ONE_TABLE_ONE_QUERY_ALL: [DOC_ONE, DOC_TWO],
            DB_ONE_TABLE_ONE_QUERY_DOC_ONE: [DOC_ONE],
        },
        TABLE_TWO: {DB_ONE_TABLE_TWO_QUERY_ALL: [DOC_THREE, DOC_FOUR]},
    },
    DB_TWO: {
        TABLE_ONE: {DB_TWO_TABLE_ONE_QUERY_ALL: [DOC_FIVE, DOC_SIX]},
        TABLE_TWO: {DB_TWO_TABLE_TWO_QUERY_ALL: [DOC_SEVEN, DOC_EIGHT]},
    },
}


@pytest.fixture()
def patch_configured_databases():
    with mock.patch.object(
        MySqlDataSource, "configured_databases", return_value=([])
    ) as configured_databases:
        yield configured_databases


@pytest.fixture
def patch_validate_databases():
    with mock.patch.object(
        MySqlDataSource, "validate_databases", return_value=([])
    ) as validate_databases:
        yield validate_databases


@pytest.fixture
def patch_fetch_tables():
    with mock.patch.object(
        MySqlDataSource, "fetch_tables", side_effect=([])
    ) as fetch_tables:
        yield fetch_tables


@pytest.fixture
def patch_ping():
    with mock.patch.object(MySqlDataSource, "ping", return_value=AsyncMock()) as ping:
        yield ping


@pytest.fixture
def patch_fetch_rows_for_table():
    with mock.patch.object(MySqlDataSource, "fetch_rows_for_table") as mock_to_patch:
        yield mock_to_patch


@pytest.fixture
def patch_default_wait_multiplier():
    with mock.patch("connectors.sources.mysql.RETRY_INTERVAL", 0):
        yield


def test_get_configuration():
    """Test get_configuration method of MySQL"""
    # Setup
    klass = MySqlDataSource

    # Execute
    config = DataSourceConfiguration(klass.get_default_configuration())

    # Assert
    assert config["host"] == "127.0.0.1"
    assert config["port"] == 3306


class Result:
    """This class contains method which returns dummy response"""

    def result(self):
        """Result method which returns dummy result"""
        return [["table1"], ["table2"]]


class Cursor:
    """This class contains methods which returns dummy response"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, *args, **kw):
        self.first_call = True
        self.description = [["Database"]]

    def fetchall(self):
        """This method returns object of Return class"""
        futures_object = asyncio.Future()
        futures_object.set_result([["table1"], ["table2"]])
        return futures_object

    async def fetchmany(self, size=1):
        """This method returns response of fetchmany"""
        if self.first_call:
            self.first_call = False
            return [["table1"], ["table2"]]
        if self.is_connection_lost:
            raise Exception("Incomplete Read Error")
        return []

    async def scroll(self, *args, **kw):
        raise Exception("Incomplete Read Error")

    def execute(self, query):
        """This method returns future object"""
        futures_object = asyncio.Future()
        futures_object.set_result(mock.MagicMock())
        return futures_object

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


class Connection:
    """This class contains methods which returns dummy connection response"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    async def ping(self):
        """This method returns object of Result class"""
        return True

    async def cursor(self):
        """This method returns object of Result class"""
        return Cursor

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


class ConnectionPool:
    def close(self):
        pass

    async def wait_closed(self):
        pass


class MockSsl:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


async def mock_mysql_response():
    """Creates mock response

    Returns:
        Mock Object: Mock response
    """
    mock_response = asyncio.Future()
    mock_response.set_result(mock.MagicMock())

    return mock_response


@pytest.mark.asyncio
async def test_close_without_connection_pool():
    source = create_source(MySqlDataSource)

    source.connection_pool = None

    await source.close()


@pytest.mark.asyncio
async def test_close_with_connection_pool():
    source = create_source(MySqlDataSource)
    source.connection_pool = ConnectionPool()
    source.connection_pool.acquire = Connection

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_ping(patch_logger):
    source = create_source(MySqlDataSource)

    mock_response = asyncio.Future()
    mock_response.set_result(mock.MagicMock())

    source.connection_pool = await mock_response
    source.connection_pool.acquire = Connection

    with mock.patch.object(aiomysql, "create_pool", return_value=mock_response):
        # Execute
        await source.ping()


@pytest.mark.asyncio
async def test_ping_negative(patch_logger):
    source = create_source(MySqlDataSource)

    mock_response = asyncio.Future()
    mock_response.set_result(mock.Mock())

    source.connection_pool = await mock_response

    with mock.patch.object(aiomysql, "create_pool", return_value=mock_response):
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_connect_with_retry(patch_logger, patch_default_wait_multiplier):
    source = await setup_mysql_source(is_connection_lost=True)

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        streamer = source._connect(
            query="select * from database.table", fetch_many=True
        )

        with pytest.raises(Exception):
            async for response in streamer:
                response


@pytest.mark.asyncio
async def test_fetch_documents():
    source = await setup_mysql_source()

    query = "select * from table"

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = source._connect(query)

        mock.patch("source._connect", return_value=response)

    response = source.fetch_documents(database="database_name", table="table_name")

    # Assert
    document_list = []
    async for document in response:
        document_list.append(document)

    assert {
        "Database": "database_name",
        "Table": "table_name",
        "_id": "database_name_table_name_",
        "_timestamp": "table1",
        "database_name_table_name_Database": "table1",
    } in document_list


@pytest.mark.asyncio
async def test_fetch_rows_from_all_tables():
    source = await setup_mysql_source()

    query = "select * from table"

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = source._connect(query)

        mock.patch("source._connect", return_value=response)

    # Assert
    async for row in source.fetch_rows_from_all_tables(database="database_name"):
        assert "_id" in row


@pytest.mark.asyncio
async def test_get_docs_with_list(patch_validate_databases):
    source = await setup_mysql_source(["database_1"])

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        source.fetch_rows_from_all_tables = mock.MagicMock(
            return_value=AsyncIterator([{"a": 1, "b": 2}])
        )

        async for doc, _ in source.get_docs():
            assert doc == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_get_docs_with_str():
    source = await setup_mysql_source("database_1")

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        source.fetch_rows_from_all_tables = mock.MagicMock(
            return_value=AsyncIterator([{"a": 1, "b": 2}])
        )

    with mock.patch.object(MySqlDataSource, "validate_databases", return_value=([])):
        async for doc, _ in source.get_docs():
            assert doc == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_get_docs_with_empty():
    source = await setup_mysql_source("")

    with pytest.raises(Exception):
        async for doc, _ in source.get_docs():
            pass


@pytest.mark.asyncio
async def test_get_docs():
    source = await setup_mysql_source()

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        source.fetch_rows_from_all_tables = mock.MagicMock(
            return_value=AsyncIterator([{"a": 1, "b": 2}])
        )

    with mock.patch.object(MySqlDataSource, "validate_databases", return_value=([])):
        async for doc, _ in source.get_docs():
            assert doc == {"a": 1, "b": 2}


async def setup_mysql_source(databases=None, is_connection_lost=False):
    if databases is None:
        databases = []

    source = create_source(MySqlDataSource)
    if isinstance(databases, list):
        source.configuration.set_field(
            name="database", label="Databases", value=databases, type="list"
        )
    else:
        source.configuration.set_field(
            name="database", label="Databases", value=databases, type="str"
        )

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor
    source.connection_pool.acquire.cursor.is_connection_lost = is_connection_lost

    return source


def setup_available_docs(advanced_snippet):
    available_docs = []

    for database in advanced_snippet:
        tables = advanced_snippet[database]
        for table in tables:
            query = advanced_snippet[database][table]
            available_docs += MYSQL[database][table][query]

    return available_docs


@pytest.mark.parametrize(
    "filtering, expected_docs",
    [
        (
            # single db, single table, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": {
                            DB_ONE: {
                                TABLE_ONE: DB_ONE_TABLE_ONE_QUERY_ALL,
                            }
                        }
                    }
                }
            ),
            {DOC_ONE, DOC_TWO},
        ),
        (
            # single db, single table, single doc
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": {DB_ONE: {TABLE_ONE: DB_ONE_TABLE_ONE_QUERY_DOC_ONE}}
                    }
                }
            ),
            {DOC_ONE},
        ),
        (
            # single db, multiple tables, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": {
                            DB_ONE: {
                                TABLE_ONE: DB_ONE_TABLE_ONE_QUERY_DOC_ONE,
                                TABLE_TWO: DB_ONE_TABLE_TWO_QUERY_ALL,
                            }
                        }
                    }
                }
            ),
            {DOC_ONE, DOC_THREE, DOC_FOUR},
        ),
        (
            # multiple dbs, multiple tables, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": {
                            DB_ONE: {
                                TABLE_ONE: DB_ONE_TABLE_ONE_QUERY_DOC_ONE,
                                TABLE_TWO: DB_ONE_TABLE_TWO_QUERY_ALL,
                            },
                            DB_TWO: {
                                TABLE_ONE: DB_TWO_TABLE_ONE_QUERY_ALL,
                                TABLE_TWO: DB_TWO_TABLE_TWO_QUERY_ALL,
                            },
                        }
                    }
                }
            ),
            {DOC_ONE, DOC_THREE, DOC_FOUR, DOC_FIVE, DOC_SIX, DOC_SEVEN, DOC_EIGHT},
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(
    filtering, expected_docs, patch_validate_databases, patch_fetch_rows_for_table
):
    source = await setup_mysql_source([DB_ONE, DB_TWO])
    docs_in_db = setup_available_docs(filtering.get_advanced_rules())
    patch_fetch_rows_for_table.return_value = AsyncIterator(docs_in_db)

    yielded_docs = set()
    async for doc, _ in source.get_docs(filtering):
        yielded_docs.add(doc)

    assert yielded_docs == expected_docs


@pytest.mark.asyncio
async def test_validate_databases():
    """This function test _validate_databases method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    query = "SHOW DATABASES"

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = source._connect(query)

        mock.patch("source._connect", return_value=response)

    # Execute
    response = await source.validate_databases([])

    # Assert
    assert response == []


def test_validate_configuration():
    """This function test _validate_configuration method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="host", value="")

    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()


def test_validate_configuration_with_port():
    """This function test _validate_configuration method with port str input of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="port", value="port")

    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()


def test_ssl_context():
    """This function test _ssl_context with dummy certificate"""
    # Setup
    certificate = "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    source = create_source(MySqlDataSource)

    # Execute
    with mock.patch.object(ssl, "create_default_context", return_value=MockSsl()):
        source._ssl_context(certificate=certificate)


async def setup_fetch_tables_side_effect(
    accessible_databases, datasource, inaccessible_databases
):
    fetch_tables_side_effect = []
    for database in accessible_databases:
        fetch_tables_side_effect.append(
            map(
                lambda table: (table, None),
                list(datasource[ACCESSIBLE][database].keys()),
            )
        )
    for database in inaccessible_databases:
        fetch_tables_side_effect.append(
            map(
                lambda table: (table, None),
                list(datasource[INACCESSIBLE][database].keys()),
            )
        )
    return fetch_tables_side_effect


@pytest.mark.parametrize(
    "datasource, advanced_rules, expected_validation_result",
    [
        (
            {ACCESSIBLE: {DB_ONE: {}, DB_TWO: {}}},
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            {ACCESSIBLE: {DB_ONE: {TABLE_ONE: {}}, DB_TWO: {TABLE_ONE: {}}}},
            {DB_ONE: {TABLE_ONE: {}}, DB_TWO: {TABLE_ONE: {}}},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            {
                ACCESSIBLE: {DB_ONE: {TABLE_ONE: {}, TABLE_TWO: {}}},
                INACCESSIBLE: {DB_TWO: {}},
            },
            {DB_ONE: {TABLE_ONE: {}, TABLE_TWO: {}}},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            {},
            {DB_ONE: {}},
            SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Non-configured databases: {DB_ONE}. Configured databases: None.",
            ),
        ),
        (
            {ACCESSIBLE: {DB_ONE: {}}, INACCESSIBLE: {DB_TWO: {}}},
            {DB_ONE: {}, DB_TWO: {}},
            SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Inaccessible databases: {DB_TWO} for user 'root'.",
            ),
        ),
        (
            {ACCESSIBLE: {DB_ONE: {TABLE_ONE: {}}, DB_TWO: {TABLE_ONE: {}}}},
            {
                DB_ONE: {TABLE_ONE: {}, TABLE_TWO: {}, TABLE_THREE: {}},
                DB_TWO: {TABLE_ONE: {}, TABLE_TWO: {}, TABLE_THREE: {}},
            },
            SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible (database -> tables): ({DB_ONE} -> {TABLE_TWO}, {TABLE_THREE}), ({DB_TWO} -> {TABLE_TWO}, {TABLE_THREE}).",
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(
    datasource,
    advanced_rules,
    expected_validation_result,
    patch_configured_databases,
    patch_validate_databases,
    patch_fetch_tables,
    patch_ping,
):
    accessible_databases = list(datasource.get(ACCESSIBLE, {}).keys())
    inaccessible_databases = list(datasource.get(INACCESSIBLE, {}).keys())
    configured_databases = accessible_databases + inaccessible_databases

    patch_configured_databases.return_value = configured_databases
    patch_validate_databases.return_value = inaccessible_databases
    patch_fetch_tables.side_effect = await setup_fetch_tables_side_effect(
        accessible_databases, datasource, inaccessible_databases
    )

    source = create_source(MySqlDataSource)
    validation_result = await MySQLAdvancedRulesValidator(source).validate(
        advanced_rules
    )

    assert validation_result == expected_validation_result
