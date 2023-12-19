import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner
from elasticsearch import ApiError

from connectors import __version__  # NOQA
from connectors.cli.auth import CONFIG_FILE_PATH
from connectors.connectors_cli import cli, login
from connectors.protocol.connectors import Connector as ConnectorObject
from connectors.protocol.connectors import JobStatus
from connectors.protocol.connectors import SyncJob as SyncJobObject
from tests.commons import AsyncIterator


@pytest.fixture(autouse=True)
def mock_cli_config():
    with patch("connectors.connectors_cli.load_config") as mock:
        mock.return_value = {"elasticsearch": {"host": "http://localhost:9211/"}}
        yield mock


@pytest.fixture(autouse=True)
def mock_connector_es_client():
    with patch("connectors.cli.connector.ESClient") as mock:
        mock.return_value = AsyncMock()
        yield mock


@pytest.fixture(autouse=True)
def mock_job_es_client():
    with patch("connectors.cli.job.ESClient") as mock:
        mock.return_value = AsyncMock()
        yield mock


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["-v"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_help_page():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert "Usage:" in result.output
    assert "Options:" in result.output
    assert "Commands:" in result.output


def test_help_page_when_no_arguments():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert "Usage:" in result.output
    assert "Options:" in result.output
    assert "Commands:" in result.output


@patch("connectors.cli.auth.Auth._Auth__ping_es_client", AsyncMock(return_value=False))
def test_login_unsuccessful(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as temp_dir:
        result = runner.invoke(
            login, input="http://localhost:9200/\nwrong_username\nwrong_password\n"
        )
        assert result.exit_code == 0
        assert "Authentication failed" in result.output
        assert not os.path.isfile(os.path.join(temp_dir, CONFIG_FILE_PATH))


@patch("connectors.cli.auth.Auth._Auth__ping_es_client", AsyncMock(return_value=True))
def test_login_successful(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as temp_dir:
        result = runner.invoke(
            login, input="http://localhost:9200/\nwrong_username\nwrong_password\n"
        )
        assert result.exit_code == 0
        assert "Authentication successful" in result.output
        assert os.path.isfile(os.path.join(temp_dir, CONFIG_FILE_PATH))


@patch("click.confirm")
def test_login_when_credentials_file_exists(mocked_confirm, tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path) as temp_dir:
        mocked_confirm.return_value = True

        # Create config file
        os.makedirs(os.path.dirname(CONFIG_FILE_PATH))
        with open(os.path.join(temp_dir, CONFIG_FILE_PATH), "w") as f:
            f.write("fake config file")

        result = runner.invoke(
            login, input="http://localhost:9200/\ncorrect_username\ncorrect_password\n"
        )
        assert result.exit_code == 0
        assert mocked_confirm.called_once()


def test_connector_help_page():
    runner = CliRunner()
    result = runner.invoke(cli, ["connector", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "Options:" in result.output
    assert "Commands:" in result.output


@patch("connectors.cli.connector.Connector.list_connectors", AsyncMock(return_value=[]))
def test_connector_list_no_connectors():
    runner = CliRunner()
    result = runner.invoke(cli, ["connector", "list"])
    assert result.exit_code == 0
    assert "No connectors found" in result.output


def test_connector_list_one_connector():
    runner = CliRunner()
    connector_index = MagicMock()

    doc = {
        "_source": {
            "index_name": "test_connector",
            "service_type": "mongodb",
            "last_sync_status": "error",
            "status": "connected",
        },
        "_id": "test_id",
    }
    connectors = [ConnectorObject(connector_index, doc)]

    with patch(
        "connectors.protocol.ConnectorIndex.all_connectors", AsyncIterator(connectors)
    ):
        result = runner.invoke(cli, ["connector", "list"])

    assert result.exit_code == 0
    assert "test_connector" in result.output
    assert "test_id" in result.output
    assert "mongodb" in result.output
    assert "error" in result.output
    assert "connected" in result.output


@patch("click.confirm")
@patch(
    "connectors.cli.index.Index.index_or_connector_exists",
    MagicMock(return_value=[False, False]),
)
def test_connector_create(patch_click_confirm):
    runner = CliRunner()

    # configuration for the MongoDB connector
    input_params = "\n".join(
        [
            "test_connector",
            "mongodb",
            "en",
            "http://localhost/",
            "username",
            "password",
            "database",
            "collection",
            "False",
        ]
    )

    with patch(
        "connectors.protocol.connectors.ConnectorIndex.index",
        AsyncMock(return_value={"_id": "new_connector_id"}),
    ) as patched_create:
        result = runner.invoke(cli, ["connector", "create"], input=input_params)

        patched_create.assert_called_once()
        assert result.exit_code == 0

        assert "has been created" in result.output


@pytest.mark.parametrize(
    "native_flag, input_index_name, expected_index_name",
    (
        ["--native", "test", "search-test"],
        ["--native", "search-test", "search-search-test"],
        [None, "test", "test"],
        [None, "search-test", "search-test"],
    ),
)
@patch("click.confirm")
@patch(
    "connectors.cli.index.Index.index_or_connector_exists",
    MagicMock(return_value=[False, False]),
)
def test_connector_create_with_native_flags(
    patch_click_confirm, native_flag, input_index_name, expected_index_name
):
    runner = CliRunner()

    # configuration for the MongoDB connector
    input_params = "\n".join(
        [
            input_index_name,
            "mongodb",
            "en",
            "http://localhost/",
            "username",
            "password",
            "database",
            "collection",
            "False",
        ]
    )

    with patch(
        "connectors.protocol.connectors.ConnectorIndex.index",
        AsyncMock(return_value={"_id": "new_connector_id"}),
    ) as patched_create:
        args = ["connector", "create"]
        if native_flag:
            args.append(native_flag)

        result = runner.invoke(cli, args, input=input_params)

        patched_create.assert_called_once()
        assert result.exit_code == 0

        assert "has been created" in result.output
        assert expected_index_name in result.output


@patch("click.confirm")
@patch(
    "connectors.cli.index.Index.index_or_connector_exists",
    MagicMock(return_value=[True, False]),
)
@patch(
    "connectors.cli.connector.Connector._Connector__create_api_key",
    AsyncMock(return_value={"id": "new_api_key_id", "encoded": "encoded_api_key"}),
)
def test_connector_create_from_index(patch_click_confirm):
    runner = CliRunner()

    # configuration for the MongoDB connector
    input_params = "\n".join(
        [
            "test-connector",
            "mongodb",
            "en",
            "http://localhost/",
            "username",
            "password",
            "database",
            "collection",
            "False",
        ]
    )

    with patch(
        "connectors.protocol.connectors.ConnectorIndex.index",
        AsyncMock(return_value={"_id": "new_connector_id"}),
    ) as patched_create:
        result = runner.invoke(
            cli, ["connector", "create", "--from-index"], input=input_params
        )

        patched_create.assert_called_once()
        assert result.exit_code == 0

        assert "has been created" in result.output


@pytest.mark.parametrize(
    "index_exists, connector_exists, from_index_flag, expected_error",
    (
        [True, False, False, "Index for test-connector already exists"],
        [
            False,
            False,
            True,
            "The flag `--from-index` was provided but index doesn't exist",
        ],
        [False, True, False, "This index is already a connector"],
        [True, True, True, "This index is already a connector"],
    ),
)
@patch("click.confirm")
def test_connector_create_fails_when_index_or_connector_exists(
    patch_click_confirm, index_exists, connector_exists, from_index_flag, expected_error
):
    runner = CliRunner()

    # configuration for the MongoDB connector
    input_params = "\n".join(
        [
            "test-connector",
            "mongodb",
            "en",
            "http://localhost/",
            "username",
            "password",
            "database",
            "collection",
            "False",
        ]
    )

    with patch(
        "connectors.cli.index.Index.index_or_connector_exists",
        MagicMock(return_value=[index_exists, connector_exists]),
    ):
        with patch(
            "connectors.protocol.connectors.ConnectorIndex.index",
            AsyncMock(return_value={"_id": "new_connector_id"}),
        ) as patched_create:
            args = ["connector", "create"]
            if from_index_flag:
                args.append("--from-index")

            result = runner.invoke(cli, args, input=input_params)

            patched_create.assert_not_called()
            assert result.exit_code == 1

            assert expected_error in result.output


@patch(
    "connectors.cli.connector.Connector._Connector__create_api_key",
    AsyncMock(return_value={"id": "new_api_key_id", "encoded": "encoded_api_key"}),
)
@patch(
    "connectors.cli.index.Index.index_or_connector_exists",
    MagicMock(return_value=[False, False]),
)
def test_connector_create_from_file():
    runner = CliRunner()

    # configuration for the MongoDB connector
    input_params = "\n".join(
        [
            "test-connector",
            "mongodb",
            "en",
        ]
    )

    with patch(
        "connectors.protocol.connectors.ConnectorIndex.index",
        AsyncMock(return_value={"_id": "new_connector_id"}),
    ) as patched_create:
        with runner.isolated_filesystem():
            with open("mongodb.json", "w") as f:
                f.write(
                    json.dumps(
                        {
                            "host": "localhost",
                            "user": "test",
                            "password": "test",
                            "database": "test",
                            "collection": "test",
                            "direct_connection": False,
                            "ssl_enabled": False,
                        }
                    )
                )
            result = runner.invoke(
                cli,
                ["connector", "create", "--from-file", "mongodb.json"],
                input=input_params,
            )

            patched_create.assert_called_once()
            assert result.exit_code == 0

            assert "has been created" in result.output


@patch(
    "connectors.cli.connector.Connector._Connector__create_api_key",
    AsyncMock(return_value={"id": "new_api_key_id", "encoded": "encoded_api_key"}),
)
@patch(
    "connectors.cli.index.Index.index_or_connector_exists",
    MagicMock(return_value=[False, False]),
)
def test_connector_create_and_update_the_service_config():
    runner = CliRunner()
    connector_id = "new_connector_id"
    service_type = "mongodb"

    # configuration for the MongoDB connector
    input_params = "\n".join(
        [
            "test_connector",
            service_type,
            "en",
            "http://localhost/",
            "username",
            "password",
            "database",
            "collection",
            "False",
        ]
    )

    with patch(
        "connectors.protocol.connectors.ConnectorIndex.index",
        AsyncMock(return_value={"_id": connector_id}),
    ) as patched_create:
        with runner.isolated_filesystem():
            with open("config.yml", "w") as f:
                f.write(yaml.dump({}))

            result = runner.invoke(
                cli, ["connector", "create", "--update-config"], input=input_params
            )
            config = yaml.load(open("config.yml"), Loader=yaml.FullLoader)[
                "connectors"
            ][0]

            patched_create.assert_called_once()
            assert os.path.exists("config.yml") is True
            assert config["api_key"] == "encoded_api_key"
            assert config["connector_id"] == connector_id
            assert config["service_type"] == service_type
            assert result.exit_code == 0
            assert "has been created" in result.output


def test_index_help_page():
    runner = CliRunner()
    result = runner.invoke(cli, ["index", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "Options:" in result.output
    assert "Commands:" in result.output


@patch("connectors.cli.index.Index.list_indices", MagicMock(return_value=[]))
def test_index_list_no_indexes():
    runner = CliRunner()
    result = runner.invoke(cli, ["index", "list"])
    assert result.exit_code == 0
    assert "No indices found" in result.output


def test_index_list_one_index():
    runner = CliRunner()
    indices = {"indices": {"test_index": {"primaries": {"docs": {"count": 10}}}}}

    with patch(
        "connectors.es.client.ESClient.list_indices", AsyncMock(return_value=indices)
    ):
        result = runner.invoke(cli, ["index", "list"])

    assert result.exit_code == 0
    assert "test_index" in result.output


@patch("click.confirm", MagicMock(return_value=True))
def test_index_clean():
    runner = CliRunner()
    index_name = "test_index"
    with patch(
        "connectors.es.client.ESClient.clean_index", AsyncMock(return_value=True)
    ) as mocked_method:
        result = runner.invoke(cli, ["index", "clean", index_name])

        assert "The index has been cleaned" in result.output
        mocked_method.assert_called_once_with(index_name)
        assert result.exit_code == 0


@patch("click.confirm", MagicMock(return_value=True))
def test_index_clean_error():
    runner = CliRunner()
    index_name = "test_index"
    with patch(
        "connectors.es.client.ESClient.clean_index",
        side_effect=ApiError(500, meta="meta", body="error"),
    ):
        result = runner.invoke(cli, ["index", "clean", index_name])

        assert "Something went wrong." in result.output
        assert result.exit_code == 0


@patch("click.confirm", MagicMock(return_value=True))
def test_index_delete():
    runner = CliRunner()
    index_name = "test_index"
    with patch(
        "connectors.es.client.ESClient.delete_indices", AsyncMock(return_value=None)
    ) as mocked_method:
        result = runner.invoke(cli, ["index", "delete", index_name])

        assert "The index has been deleted" in result.output
        mocked_method.assert_called_once_with([index_name])
        assert result.exit_code == 0


@patch("click.confirm", MagicMock(return_value=True))
def test_delete_index_error():
    runner = CliRunner()
    index_name = "test_index"
    with patch(
        "connectors.es.client.ESClient.delete_indices",
        side_effect=ApiError(500, meta="meta", body="error"),
    ):
        result = runner.invoke(cli, ["index", "delete", index_name])

        assert "Something went wrong." in result.output
        assert result.exit_code == 0


def test_job_help_page():
    runner = CliRunner()
    result = runner.invoke(cli, ["job", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "Options:" in result.output
    assert "Commands:" in result.output


def test_job_help_page_without_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["job"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "Options:" in result.output
    assert "Commands:" in result.output


@patch("click.confirm", MagicMock(return_value=True))
def test_job_cancel():
    runner = CliRunner()
    job_id = "test_job_id"

    job_index = MagicMock()

    doc = {
        "_source": {
            "connector": {
                "index_name": "test_connector",
                "service_type": "mongodb",
                "last_sync_status": "error",
                "status": "connected",
            },
            "status": "running",
            "job_type": "full",
        },
        "_id": job_id,
    }

    job = SyncJobObject(job_index, doc)

    with patch(
        "connectors.cli.job.Job._Job__async_list_jobs", AsyncMock(return_value=[job])
    ):
        with patch.object(job, "_terminate") as mocked_method:
            result = runner.invoke(cli, ["job", "cancel", job_id])

            mocked_method.assert_called_once_with(JobStatus.CANCELING)
            assert "The job has been cancelled" in result.output
            assert result.exit_code == 0


@patch("click.confirm", MagicMock(return_value=True))
def test_job_cancel_error():
    runner = CliRunner()
    job_id = "test_job_id"
    with patch(
        "connectors.cli.job.Job._Job__async_list_jobs",
        side_effect=ApiError(500, meta="meta", body="error"),
    ):
        result = runner.invoke(cli, ["job", "cancel", job_id])

        assert "Something went wrong." in result.output
        assert result.exit_code == 0


def test_job_list_no_jobs():
    runner = CliRunner()
    connector_id = "test_connector_id"

    with patch(
        "connectors.cli.job.Job._Job__async_list_jobs", AsyncMock(return_value=[])
    ):
        result = runner.invoke(cli, ["job", "list", connector_id])

        assert "No jobs found" in result.output
        assert result.exit_code == 0


@patch("click.confirm", MagicMock(return_value=True))
def test_job_list_one_job():
    runner = CliRunner()
    job_id = "test_job_id"
    connector_id = "test_connector_id"
    index_name = "test_index_name"
    status = "canceled"
    deleted_document_count = 123
    indexed_document_count = 123123
    indexed_document_volume = 100500

    job_index = MagicMock()

    doc = {
        "_source": {
            "connector": {
                "id": connector_id,
                "index_name": index_name,
                "service_type": "mongodb",
                "last_sync_status": "error",
                "status": "connected",
            },
            "status": status,
            "deleted_document_count": deleted_document_count,
            "indexed_document_count": indexed_document_count,
            "indexed_document_volume": indexed_document_volume,
            "job_type": "full",
        },
        "_id": job_id,
    }

    job = SyncJobObject(job_index, doc)

    with patch(
        "connectors.protocol.connectors.SyncJobIndex.get_all_docs", AsyncIterator([job])
    ):
        result = runner.invoke(cli, ["job", "list", connector_id])

        assert job_id in result.output
        assert connector_id in result.output
        assert index_name in result.output
        assert status in result.output
        assert str(deleted_document_count) in result.output
        assert str(indexed_document_count) in result.output
        assert str(indexed_document_volume) in result.output
        assert result.exit_code == 0


@patch(
    "connectors.protocol.connectors.ConnectorIndex.fetch_by_id",
    AsyncMock(return_value=MagicMock()),
)
def test_job_start():
    runner = CliRunner()
    connector_id = "test_connector_id"
    job_id = 'test_job_id'

    with patch(
        "connectors.protocol.connectors.SyncJobIndex.create",
        AsyncMock(return_value=job_id),
    ) as patched_create:
        result = runner.invoke(cli, ["job", "start", "-i", connector_id, "-t", "full"])

        patched_create.assert_called_once()
        assert f"The job {job_id} has been started" in result.output
        assert result.exit_code == 0


def test_job_view():
    runner = CliRunner()
    job_id = "test_job_id"
    connector_id = "test_connector_id"
    index_name = "test_index_name"
    status = "canceled"
    deleted_document_count = 123
    indexed_document_count = 123123
    indexed_document_volume = 100500

    job_index = MagicMock()

    doc = {
        "_source": {
            "connector": {
                "id": connector_id,
                "index_name": index_name,
                "service_type": "mongodb",
                "last_sync_status": "error",
                "status": "connected",
            },
            "status": status,
            "deleted_document_count": deleted_document_count,
            "indexed_document_count": indexed_document_count,
            "indexed_document_volume": indexed_document_volume,
            "job_type": "full",
        },
        "_id": job_id,
    }

    job = SyncJobObject(job_index, doc)

    with patch(
        "connectors.protocol.connectors.SyncJobIndex.fetch_by_id",
        AsyncMock(return_value=job),
    ):
        result = runner.invoke(cli, ["job", "view", job_id])

        assert job_id in result.output
        assert connector_id in result.output
        assert index_name in result.output
        assert status in result.output
        assert str(deleted_document_count) in result.output
        assert str(indexed_document_count) in result.output
        assert str(indexed_document_volume) in result.output
        assert result.exit_code == 0
