#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import datetime
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import aiomysql
import pytest
from freezegun import freeze_time

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.logger import logger
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.mysql import (
    MySQLAdvancedRulesValidator,
    MySQLClient,
    MySqlDataSource,
    generate_id,
    parse_tables_string_to_list_of_tables,
    row2doc,
)
from connectors.utils import iso_utc
from tests.commons import AsyncIterator
from tests.sources.support import create_source


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
    frozenset([TABLE_ONE]): {
        TABLE_ONE_QUERY_ALL: [DOC_ONE, DOC_TWO],
        TABLE_ONE_QUERY_DOC_ONE: [DOC_ONE],
    },
    frozenset([TABLE_TWO]): {TABLE_TWO_QUERY_ALL: [DOC_THREE, DOC_FOUR]},
}

ALICE = {"id": 1, "name": "Alice", "age": 30}
BOB = {"id": 2, "name": "Bob", "age": 30}
TIME = "2023-01-18T17:18:56.814003+00:00"
TIMESTAMP = datetime.datetime(
    year=2023, month=1, day=2, hour=5, second=10, microsecond=3
)


def future_with_result(result):
    future = asyncio.Future()
    future.set_result(result)

    return future


def as_async_context_manager_mock(obj):
    context_manager = MagicMock()
    context_manager.__aenter__.return_value = obj
    context_manager.__aexit__.return_value = None

    return MagicMock(return_value=context_manager)


def mocked_mysql_client(
    pk_cols=None,
    table_cols=None,
    last_update_times=None,
    documents=None,
    custom_query=False,
):
    client = MagicMock()

    client.get_primary_key_column_names = AsyncMock(side_effect=pk_cols)
    client.get_last_update_time = AsyncMock(side_effect=last_update_times)

    if custom_query:
        client.get_column_names_for_query = AsyncMock(return_value=table_cols)
        client.yield_rows_for_query = AsyncIterator(documents)
    else:
        client.get_column_names_for_table = AsyncMock(return_value=table_cols)
        client.yield_rows_for_table = AsyncIterator(documents)

    return client


@pytest.fixture
def patch_ping():
    with patch.object(MySqlDataSource, "ping", return_value=AsyncMock()) as ping:
        yield ping


@pytest.fixture
def patch_row2doc():
    with patch("connectors.sources.mysql.row2doc", return_value=MagicMock()) as row2doc:
        yield row2doc


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
async def test_client_when_aexit_called_then_cancel_sleeps(patch_connection_pool):
    client = await setup_mysql_client()

    async with client:
        client._sleeps.cancel = Mock()
        pass

    client._sleeps.cancel.assert_called_once()


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
        ([("id",)], ["id"]),
        (
            [("group",), ("class",), ("name",)],
            [
                "group",
                "class",
                "name",
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_client_get_column_names_for_table(
    patch_connection_pool, column_tuples, expected_column_names
):
    mock_cursor = MagicMock(spec=aiomysql.Cursor)
    mock_cursor.description = column_tuples
    mock_cursor.__aenter__.return_value = mock_cursor

    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()

    async with client:
        result = await client.get_column_names_for_table(TABLE_ONE)
        assert result == expected_column_names


@pytest.mark.asyncio
async def test_client_get_column_names_for_query(patch_connection_pool):
    columns = [("id",), ("class",)]

    mock_cursor = MagicMock(spec=aiomysql.Cursor)
    mock_cursor.description = columns
    mock_cursor.__aenter__.return_value = mock_cursor

    patch_connection_pool.acquire.return_value = await mock_connection(mock_cursor)

    client = await setup_mysql_client()

    async with client:
        result = await client.get_column_names_for_query("SELECT * FROM *")
        expected_columns = list(map(lambda column: column[0], columns))

        assert result == expected_columns


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


@freeze_time(TIME)
@pytest.mark.asyncio
async def test_fetch_documents(patch_connection_pool):
    primary_key_col = "pk"
    column = "column"
    document = ["table1"]

    async with create_source(MySqlDataSource) as source:
        source.configuration.set_field(
            name="database", label="Database", value=DATABASE, type="str"
        )
        source.database = DATABASE
        source.mysql_client = as_async_context_manager_mock(
            mocked_mysql_client(
                pk_cols=[primary_key_col],
                table_cols=[column],
                last_update_times=[TIME],
                documents=[document],
            )
        )

        document_list = []
        async for document in source.fetch_documents(tables=[TABLE_ONE]):
            document_list.append(document)

        assert document in document_list


@freeze_time(TIME)
@pytest.mark.asyncio
async def test_fetch_documents_when_used_custom_query_then_sort_pk_cols(
    patch_connection_pool, patch_row2doc
):
    primary_key_col = ["cd", "ab"]
    column = "column"

    document = {
        "Table": "table_name",
        "_id": "table_name_",
        "_timestamp": TIME,
        f"table_name_{column}": "table1",
    }

    patch_row2doc.return_value = document

    async with create_source(MySqlDataSource) as source:
        source.configuration.set_field(
            name="database", label="Database", value=DATABASE, type="str"
        )
        source.database = DATABASE
        source.mysql_client = as_async_context_manager_mock(
            mocked_mysql_client(
                pk_cols=[primary_key_col],
                table_cols=[column],
                last_update_times=[TIME],
                documents=[document],
                custom_query=True,
            )
        )

        document_list = []
        async for document in source.fetch_documents(
            tables=[TABLE_ONE], query="SELECT * FROM *"
        ):
            document_list.append(document)

        assert document in document_list
        assert patch_row2doc.call_args.kwargs == {
            "row": {
                "Table": "table_name",
                "_id": "table_name_",
                "_timestamp": TIME,
                "table_name_column": "table1",
            },
            "column_names": ["column"],
            # primary key columns are now sorted
            "primary_key_columns": ["ab", "cd"],
            "table": ["table1"],
            "timestamp": TIME,
        }


@freeze_time(TIME)
@pytest.mark.asyncio
async def test_fetch_documents_when_custom_query_used_and_update_time_none(
    patch_connection_pool, patch_row2doc
):
    primary_key_col = ["cd", "ab"]
    column = "column"

    document = {
        "Table": "table_name",
        "_id": "table_name_",
        "_timestamp": TIME,
        f"table_name_{column}": "table1",
    }

    patch_row2doc.return_value = document

    async with create_source(MySqlDataSource) as source:
        source.configuration.set_field(
            name="database", label="Database", value=DATABASE, type="str"
        )
        source.database = DATABASE
        source.mysql_client = as_async_context_manager_mock(
            mocked_mysql_client(
                pk_cols=[primary_key_col],
                table_cols=[column],
                last_update_times=[None],
                documents=[document],
                custom_query=True,
            )
        )

        document_list = []
        async for document in source.fetch_documents(
            tables=[TABLE_ONE], query="SELECT * FROM *"
        ):
            document_list.append(document)

        assert document in document_list
        assert patch_row2doc.call_args.kwargs == {
            "row": {
                "Table": "table_name",
                "_id": "table_name_",
                "_timestamp": TIME,
                "table_name_column": "table1",
            },
            "column_names": ["column"],
            "primary_key_columns": ["ab", "cd"],
            "table": ["table1"],
            # Should be called with an empty timestamp without failing on a comparison needed for max(...)
            "timestamp": None,
        }


@pytest.mark.asyncio
async def test_get_docs(patch_connection_pool):
    async with create_source(MySqlDataSource) as source:
        source.configuration.set_field(
            name="database", label="Database", value=DATABASE, type="str"
        )
        source.database = DATABASE
        source.mysql_client = MagicMock()

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
        logger_=logger,
    )

    return client


def setup_available_docs(advanced_snippet):
    available_docs = []

    for tables_query in advanced_snippet:
        tables = tables_query["tables"]
        query = tables_query["query"]

        available_docs += MYSQL[frozenset(tables)][query]

    return available_docs


@pytest.mark.parametrize(
    "filtering, expected_docs",
    [
        (
            # single table, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [{"tables": [TABLE_ONE], "query": TABLE_ONE_QUERY_ALL}]
                    }
                }
            ),
            {DOC_ONE, DOC_TWO},
        ),
        (
            # single table, single doc
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {"tables": [TABLE_ONE], "query": TABLE_ONE_QUERY_DOC_ONE}
                        ]
                    }
                }
            ),
            {DOC_ONE},
        ),
        (
            # multiple tables, multiple docs
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {"tables": [TABLE_ONE], "query": TABLE_ONE_QUERY_DOC_ONE},
                            {"tables": [TABLE_TWO], "query": TABLE_TWO_QUERY_ALL},
                        ]
                    }
                }
            ),
            {DOC_ONE, DOC_THREE, DOC_FOUR},
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_docs):
    async with create_source(MySqlDataSource) as source:
        source.configuration.set_field(
            name="database", label="Database", value="", type="str"
        )
        source.database = ""
        source.mysql_client = MagicMock()
        docs_in_db = setup_available_docs(filtering.get_advanced_rules())
        source.fetch_documents = AsyncIterator(docs_in_db)

        yielded_docs = set()
        async for doc, _ in source.get_docs(filtering):
            yielded_docs.add(doc)

        assert yielded_docs == expected_docs


@pytest.mark.asyncio
async def test_validate_config_when_host_empty_then_raise_error():
    async with create_source(MySqlDataSource, host="") as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.parametrize(
    "tables_present_in_source, advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            [],
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: one custom query
            [TABLE_ONE],
            [{"tables": [TABLE_ONE], "query": "SELECT * FROM *"}],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: two custom queries
            [TABLE_ONE, TABLE_TWO],
            [
                {"tables": [TABLE_ONE], "query": "SELECT * FROM *"},
                {"tables": [TABLE_ONE, TABLE_TWO], "query": "SELECT * FROM *"},
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: additional property present
            [TABLE_ONE],
            [
                {
                    "tables": [TABLE_ONE],
                    "query": "SELECT * FROM *",
                    "additional_property": True,
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: tables field missing
            [TABLE_ONE],
            [{"query": "SELECT * FROM *"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: query field missing
            [TABLE_ONE],
            [{"tables": [TABLE_ONE]}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: query empty
            [TABLE_ONE],
            [{"tables": [TABLE_ONE], "query": ""}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: tables empty
            [TABLE_ONE],
            [{"tables": [], "query": "SELECT * FROM *"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: table missing in source
            [TABLE_ONE],
            [{"tables": [TABLE_ONE, TABLE_TWO], "query": "SELECT * FROM *"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: array of arrays -> wrong type
            [TABLE_ONE],
            [[]],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(
    tables_present_in_source,
    advanced_rules,
    expected_validation_result,
    patch_ping,
):
    async with create_source(MySqlDataSource) as source:
        source.configuration.set_field(
            name="database", label="Database", value="", type="str"
        )
        source.database = ""

        client = MagicMock()
        client.get_all_table_names = AsyncMock(return_value=tables_present_in_source)

        source.mysql_client = as_async_context_manager_mock(client)

        validation_result = await MySQLAdvancedRulesValidator(source).validate(
            advanced_rules
        )

        assert validation_result == expected_validation_result


@pytest.mark.parametrize("tables", ["*", ["*"]])
@pytest.mark.asyncio
async def test_get_tables_when_wildcard_configured_then_fetch_all_tables(tables):
    async with create_source(MySqlDataSource) as source:
        source.tables = tables

        client = MagicMock()
        client.get_all_table_names = AsyncMock(return_value="table")

        source.mysql_client = as_async_context_manager_mock(client)

        await source.get_tables_to_fetch()

        assert client.get_all_table_names.call_count == 1


@pytest.mark.asyncio
async def test_validate_database_accessible_when_accessible_then_no_error_raised():
    async with create_source(MySqlDataSource) as source:
        source.database = "test_database"

        cursor = AsyncMock()
        cursor.execute.return_value = None

        await source._validate_database_accessible(cursor)
        cursor.execute.assert_called_with(f"USE {source.database};")


@pytest.mark.asyncio
async def test_validate_database_accessible_when_not_accessible_then_error_raised():
    async with create_source(MySqlDataSource) as source:
        cursor = AsyncMock()
        cursor.execute.side_effect = aiomysql.Error("Error")

        with pytest.raises(ConfigurableFieldValueError):
            await source._validate_database_accessible(cursor)


@pytest.mark.asyncio
async def test_validate_tables_accessible_when_accessible_then_no_error_raised():
    async with create_source(MySqlDataSource) as source:
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
    async with create_source(MySqlDataSource) as source:
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
    async with create_source(MySqlDataSource) as source:
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
    "tables, row, primary_key_columns, expected_id",
    [
        (TABLE_ONE, {"key_1": 1, "key_2": 2}, ["key_1"], f"{TABLE_ONE}_1"),
        ([TABLE_ONE], {"key_1": 1, "key_2": 2}, ["key_1"], f"{TABLE_ONE}_1"),
        (
            [TABLE_ONE, TABLE_TWO],
            {"key_1": 1, "key_2": 2},
            ["key_1", "key_2"],
            f"{TABLE_ONE}_{TABLE_TWO}_1_2",
        ),
        (
            [TABLE_THREE, TABLE_ONE, TABLE_TWO],
            {"key_1": 1, "key_2": 2},
            ["key_1", "key_3"],
            f"{TABLE_ONE}_{TABLE_TWO}_{TABLE_THREE}_1",
        ),
        (
            [TABLE_ONE, TABLE_TWO, TABLE_THREE],
            {"key_1": 1, "key_2": 2},
            ["key_1", "key_3"],
            f"{TABLE_ONE}_{TABLE_TWO}_{TABLE_THREE}_1",
        ),
    ],
)
def test_generate_id(tables, row, primary_key_columns, expected_id):
    row_id = generate_id(tables, row, primary_key_columns)

    assert row_id == expected_id


@pytest.mark.parametrize(
    "row, column_names, primary_key_columns, tables, timestamp, expected_doc",
    [
        (
            (ALICE["id"], ALICE["name"], ALICE["age"]),
            list(ALICE.keys()),
            ["id"],
            TABLE_ONE,
            TIMESTAMP,
            {
                "_id": ANY,
                "_timestamp": TIMESTAMP,
                "id": ALICE["id"],
                "name": ALICE["name"],
                "age": ALICE["age"],
                "Table": TABLE_ONE,
            },
        ),
        (
            # multiple tables, missing timestamp should be replaced by iso_utc()
            (BOB["id"], BOB["name"], BOB["age"]),
            list(BOB.keys()),
            ["id"],
            [TABLE_ONE, TABLE_TWO],
            None,
            {
                "_id": ANY,
                "_timestamp": TIME,
                "id": BOB["id"],
                "name": BOB["name"],
                "age": BOB["age"],
                "Table": [TABLE_ONE, TABLE_TWO],
            },
        ),
    ],
)
@freeze_time(TIME)
def test_row2doc(
    row, column_names, primary_key_columns, tables, timestamp, expected_doc
):
    doc = row2doc(
        row=row,
        column_names=column_names,
        primary_key_columns=primary_key_columns,
        table=tables,
        timestamp=timestamp,
    )

    assert doc == expected_doc
