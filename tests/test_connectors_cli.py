import os
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner

from connectors import __version__  # NOQA
from connectors.cli.auth import CONFIG_FILE_PATH
from connectors.connectors_cli import cli, login
from connectors.protocol.connectors import Connector as ConnectorObject
from tests.commons import AsyncIterator


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
    indices = {"test_index": {"primaries": {"docs": {"count": 10}}}}

    with patch(
        "connectors.cli.index.Index.list_indices", MagicMock(return_value=indices)
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
