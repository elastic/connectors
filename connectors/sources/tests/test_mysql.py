#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import aiomysql
import pytest

from connectors.byoc import Filter
from connectors.filtering.validation import SyncRuleValidationResult
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.mysql import (
    MySQLAdvancedRulesValidator,
    MySQLClient,
    MySqlDataSource,
    parse_tables_string_to_list_of_tables,
)
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator
from connectors.utils import iso_utc


def immutable_doc(**kwargs):
    return frozenset(kwargs.items())


ADVANCED_SNIPPET = "advanced_snippet"

DATABASE = "database"

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

TABLE_ONE_QUERY_ALL = "query all db one table one"
TABLE_ONE_QUERY_DOC_ONE = "query doc one"
TABLE_TWO_QUERY_ALL = "query all db one table two"

DB_TWO_TABLE_ONE_QUERY_ALL = "query all db two table one"
DB_TWO_TABLE_TWO_QUERY_ALL = "query all db two table two"

ALL_DOCS = "all_docs"
ONLY_DOC_ONE = "only_doc_one"

ACCESSIBLE = "accessible"
INACCESSIBLE = "inaccessible"

MYSQL = {
    TABLE_ONE: {
        TABLE_ONE_QUERY_ALL: [DOC_ONE, DOC_TWO],
        TABLE_ONE_QUERY_DOC_ONE: [DOC_ONE],
    },
    TABLE_TWO: {TABLE_TWO_QUERY_ALL: [DOC_THREE, DOC_FOUR]},
}


def future_with_result(result):
    future = asyncio.Future()
    future.set_result(result)

    return future


def wrap_mock_client_in_context_manager(mock_client):
    context_manager = MagicMock()
    context_manager.__aenter__.return_value = mock_client
    context_manager.__aexit__.return_value = None

    return MagicMock(return_value=context_manager)


@pytest.fixture
def patch_ping():
    with patch.object(MySqlDataSource, "ping", return_value=AsyncMock()) as ping:
        yield ping


@pytest.fixture
def patch_default_wait_multiplier():
    with patch("connectors.sources.mysql.RETRY_INTERVAL", 0):
        yield


@pytest.fixture
def patch_connection_pool():
    connection_pool = Mock()
    connection_pool.close = Mock()
    connection_pool.wait_closed = AsyncMock()
    connection_pool.acquire = AsyncMock(return_value=Connection())

    with patch(
        "aiomysql.create_pool",
        return_value=future_with_result(connection_pool),
    ):
        yield connection_pool


def test_get_configuration():
    """Test get_configuration method of MySQL"""
    klass = MySqlDataSource

    config = DataSourceConfiguration(klass.get_default_configuration())

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
        futures_object.set_result(MagicMock())
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
    mock_response.set_result(MagicMock())

    return mock_response


async def mock_connection(mock_cursor):
    mock_conn = MagicMock(spec=aiomysql.Connection)
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__aenter__.return_value = mock_conn

    return mock_conn


def mock_cursor_fetchmany(rows_per_batch=None):
    if rows_per_batch is None:
        rows_per_batch = []

    mock_cursor = MagicMock(spec=aiomysql.Cursor)
    mock_cursor.fetchmany.side_effect = AsyncMock(side_effect=[*rows_per_batch, None])
    mock_cursor.__aenter__.return_value = mock_cursor

    return mock_cursor


@pytest.mark.asyncio
async def test_close_when_source_setup_correctly_does_not_raise_errors():
    source = await setup_mysql_source()

    await source.close()


@pytest.mark.asyncio
async def test_client_get_tables(patch_connection_pool):
    table_1 = "table_1"
    table_2 = "table_2"

    fetchall_tables_response = [
        (table_1,),
        (table_2,),
    ]

    mock_cursor = MagicMock(spec=aiomysql.Cursor)
    mock_cursor.fetchall = AsyncMock(return_value=fetchall_tables_response)
    mock_cursor.__aenter__.return_value = mock_cursor

    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()

    async with client:
        result = await client.get_all_table_names()
        expected_result = [table_1, table_2]

        assert result == expected_result


@pytest.mark.parametrize(
    "column_tuples, expected_column_names",
    [
        ([], []),
        ([("id",)], [f"{TABLE_ONE}_id"]),
        (
            [("group",), ("class",), ("name",)],
            [
                f"{TABLE_ONE}_group",
                f"{TABLE_ONE}_class",
                f"{TABLE_ONE}_name",
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_client_get_column_names(
    patch_connection_pool, column_tuples, expected_column_names
):
    mock_cursor = MagicMock(spec=aiomysql.Cursor)
    mock_cursor.fetchall = AsyncMock(return_value=column_tuples)
    mock_cursor.__aenter__.return_value = mock_cursor

    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()

    async with client:
        result = await client.get_column_names(TABLE_ONE)
        assert result == expected_column_names


@pytest.mark.asyncio
async def test_client_get_last_update_time(patch_connection_pool):
    last_update_time = iso_utc()

    mock_cursor = MagicMock(spec=aiomysql.Cursor)
    mock_cursor.fetchone = AsyncMock(return_value=(last_update_time, None))
    mock_cursor.__aenter__.return_value = mock_cursor

    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()

    async with client:
        assert await client.get_last_update_time("table") == last_update_time


@pytest.mark.asyncio
async def test_client_yield_rows_for_table(patch_connection_pool):
    rows_per_batch = [[DOC_ONE], [DOC_TWO], [DOC_THREE]]
    mock_cursor = mock_cursor_fetchmany(rows_per_batch)
    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()
    client.fetch_size = 1

    async with client:
        yielded_docs = []

        async for doc in client.yield_rows_for_table("table"):
            yielded_docs.append(doc)

        # 3 batches with rows, 4th batch empty
        num_batches = len(rows_per_batch) / client.fetch_size + 1

        assert len(yielded_docs) == len(rows_per_batch)
        assert mock_cursor.fetchmany.call_count == num_batches


@pytest.mark.asyncio
async def test_client_yield_rows_for_query(patch_connection_pool):
    rows_per_batch = [[DOC_ONE]]
    mock_cursor = mock_cursor_fetchmany(rows_per_batch)
    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()
    client.fetch_size = 1

    async with client:
        yielded_docs = []

        async for doc in client.yield_rows_for_query("SELECT * FROM db.table"):
            yielded_docs.append(doc)

        # 1 batch with rows, 2nd batch empty
        num_batches = len(rows_per_batch) / client.fetch_size + 1

        assert len(yielded_docs) == len(rows_per_batch)
        assert mock_cursor.fetchmany.call_count == num_batches


@pytest.mark.asyncio
async def test_client_ping(patch_logger, patch_connection_pool):
    client = await setup_mysql_client()

    async with client:
        await client.ping()


@pytest.mark.asyncio
async def test_client_ping_negative(patch_logger):
    client = await setup_mysql_client()

    mock_response = asyncio.Future()
    mock_response.set_result(Mock())

    client.connection_pool = await mock_response

    with patch.object(aiomysql, "create_pool", return_value=mock_response):
        with pytest.raises(Exception):
            await client.ping()


@pytest.mark.asyncio
async def test_fetch_documents(patch_connection_pool):
    last_update_time = "2023-01-18 17:18:56"
    primary_key_col = "pk"
    column = "column"

    document = {
        "Table": "table_name",
        "_id": "table_name_",
        "_timestamp": last_update_time,
        f"table_name_{column}": "table1",
    }

    client = MagicMock()
    client.get_primary_key_column_names = AsyncMock(side_effect=[primary_key_col])
    client.get_last_update_time = AsyncMock(return_value=last_update_time)
    client.get_column_names = AsyncMock(return_value=[column])
    client.yield_rows_for_query = AsyncIterator([document])

    source = await setup_mysql_source(DATABASE)
    source.mysql_client = wrap_mock_client_in_context_manager(client)

    query = "select * from table"

    document_list = []
    async for document in source.fetch_documents(table="table_name", query=query):
        document_list.append(document)

    assert document in document_list


@pytest.mark.asyncio
async def test_get_docs(patch_connection_pool):
    source = await setup_mysql_source(DATABASE)

    source.get_tables_to_fetch = AsyncMock(return_value=["table"])
    source.fetch_documents = AsyncIterator([{"a": 1, "b": 2}])

    async for doc, _ in source.get_docs():
        assert doc == {"a": 1, "b": 2}


async def setup_mysql_client():
    client = MySQLClient(
        host="host",
        port=123,
        user="user",
        password="password",
        ssl_enabled=False,
        ssl_certificate="",
    )

    return client


async def setup_mysql_source(database="", client=None):
    if client is None:
        client = MagicMock()

    source = create_source(MySqlDataSource)
    source.configuration.set_field(
        name="database", label="Database", value=database, type="str"
    )

    source.database = database
    source.mysql_client = client

    return source


def setup_available_docs(advanced_snippet):
    available_docs = []

    for table in advanced_snippet:
        query = advanced_snippet[table]
        available_docs += MYSQL[table][query]

    return available_docs


@pytest.mark.parametrize(
    "filtering, expected_docs",
    [
        (
            # single table, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": {
                            TABLE_ONE: TABLE_ONE_QUERY_ALL,
                        }
                    }
                }
            ),
            {DOC_ONE, DOC_TWO},
        ),
        (
            # single table, single doc
            Filter({ADVANCED_SNIPPET: {"value": {TABLE_ONE: TABLE_ONE_QUERY_DOC_ONE}}}),
            {DOC_ONE},
        ),
        (
            # multiple tables, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": {
                            TABLE_ONE: TABLE_ONE_QUERY_DOC_ONE,
                            TABLE_TWO: TABLE_TWO_QUERY_ALL,
                        }
                    }
                }
            ),
            {DOC_ONE, DOC_THREE, DOC_FOUR},
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_docs):
    source = await setup_mysql_source(DATABASE)
    docs_in_db = setup_available_docs(filtering.get_advanced_rules())
    source.fetch_documents = AsyncIterator(docs_in_db)

    yielded_docs = set()
    async for doc, _ in source.get_docs(filtering):
        yielded_docs.add(doc)

    assert yielded_docs == expected_docs


@pytest.mark.asyncio
async def test_validate_config_when_host_empty_then_raise_error():
    source = create_source(MySqlDataSource, host="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.parametrize(
    "datasource, advanced_rules, expected_validation_result",
    [
        (
            {},
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            {TABLE_ONE: {}},
            {TABLE_ONE: {}},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            {TABLE_ONE: {}, TABLE_TWO: {}},
            {TABLE_ONE: {}, TABLE_TWO: {}},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            {},
            {TABLE_ONE: {}},
            SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible: {TABLE_ONE}.",
            ),
        ),
        (
            {},
            {TABLE_ONE: {}, TABLE_TWO: {}},
            SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible: {TABLE_ONE}, {TABLE_TWO}.",
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_tables_validation(
    datasource,
    advanced_rules,
    expected_validation_result,
    patch_ping,
):
    source = await setup_mysql_source()

    client = MagicMock()
    client.get_all_table_names = AsyncMock(side_effect=[datasource.keys()])

    source.mysql_client = wrap_mock_client_in_context_manager(client)

    validation_result = await MySQLAdvancedRulesValidator(source).validate(
        advanced_rules
    )

    assert validation_result == expected_validation_result


@pytest.mark.parametrize("tables", ["*", ["*"]])
@pytest.mark.asyncio
async def test_get_tables_when_wildcard_configured_then_fetch_all_tables(tables):
    source = create_source(MySqlDataSource)
    source.tables = tables

    client = MagicMock()
    client.get_all_table_names = AsyncMock(return_value="table")

    source.mysql_client = wrap_mock_client_in_context_manager(client)

    await source.get_tables_to_fetch()

    assert client.get_all_table_names.call_count == 1


@pytest.mark.asyncio
async def test_validate_database_accessible_when_accessible_then_no_error_raised():
    source = create_source(MySqlDataSource)
    source.database = "test_database"

    cursor = AsyncMock()
    cursor.execute.return_value = None

    await source._validate_database_accessible(cursor)
    cursor.execute.assert_called_with(f"USE {source.database};")


@pytest.mark.asyncio
async def test_validate_database_accessible_when_not_accessible_then_error_raised():
    source = create_source(MySqlDataSource)

    cursor = AsyncMock()
    cursor.execute.side_effect = aiomysql.Error("Error")

    with pytest.raises(ConfigurableFieldValueError):
        await source._validate_database_accessible(cursor)


@pytest.mark.asyncio
async def test_validate_tables_accessible_when_accessible_then_no_error_raised():
    source = create_source(MySqlDataSource)
    source.tables = ["table_1", "table_2", "table_3"]

    client = MagicMock()
    client.get_all_table_names = AsyncMock(
        return_value=["table_1", "table_2", "table_3"]
    )

    context_manager = MagicMock()
    context_manager.__aenter__.return_value = client
    context_manager.__aexit__.return_value = None

    source.mysql_client = MagicMock(return_value=context_manager)

    cursor = AsyncMock()
    cursor.execute.return_value = None

    await source._validate_tables_accessible(cursor)


@pytest.mark.parametrize("tables", ["*", ["*"]])
@pytest.mark.asyncio
async def test_validate_tables_accessible_when_accessible_and_wildcard_then_no_error_raised(
    tables,
):
    source = create_source(MySqlDataSource)
    source.tables = tables
    source.get_tables_to_fetch = AsyncMock(
        return_value=["table_1", "table_2", "table_3"]
    )

    cursor = AsyncMock()
    cursor.execute.return_value = None

    await source._validate_tables_accessible(cursor)

    assert source.get_tables_to_fetch.call_count == 1


@pytest.mark.asyncio
async def test_validate_tables_accessible_when_not_accessible_then_error_raised():
    source = create_source(MySqlDataSource)
    source.tables = ["table1"]
    source.get_tables_to_fetch = AsyncMock(return_value=["table1"])

    cursor = AsyncMock()
    cursor.execute.side_effect = aiomysql.Error("Error")

    with pytest.raises(ConfigurableFieldValueError):
        await source._validate_tables_accessible(cursor)


@pytest.mark.parametrize(
    "tables_string, expected_tables_list",
    [
        (None, []),
        ("", []),
        ("table_1", ["table_1"]),
        ("table_1, ", ["table_1"]),
        ("`table_1,`,", ["`table_1,`"]),
        ("table_1, table_2", ["table_1", "table_2"]),
        ("`table_1,abc`", ["`table_1,abc`"]),
        ("`table_1,abc`, table_2", ["`table_1,abc`", "table_2"]),
        ("`table_1,abc`, `table_2,def`", ["`table_1,abc`", "`table_2,def`"]),
    ],
)
def test_parse_tables_string_to_list(tables_string, expected_tables_list):
    assert parse_tables_string_to_list_of_tables(tables_string) == expected_tables_list


@pytest.mark.parametrize(
    "row, primary_key_columns, expected_id",
    [
        ({"key_1": 1, "key_2": 2}, ["key_1"], f"{TABLE_ONE}_1_"),
        ({"key_1": 1, "key_2": 2}, ["key_1", "key_2"], f"{TABLE_ONE}_1_2_"),
        ({"key_1": 1, "key_2": 2}, ["key_1", "key_3"], f"{TABLE_ONE}_1_"),
    ],
)
def test_generate_id(row, primary_key_columns, expected_id):
    source = create_source(MySqlDataSource)

    row_id = source._generate_id(TABLE_ONE, row, primary_key_columns)

    assert row_id == expected_id
