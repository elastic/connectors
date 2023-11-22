from click.testing import CliRunner
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


from connectors.connectors_cli import cli, login
from connectors.cli.auth import Auth, CONFIG_FILE_PATH
import os
from connectors import __version__  # NOQA

def test_version():
  runner = CliRunner()
  result = runner.invoke(cli, ['-v'])
  assert result.exit_code == 0
  assert result.output.strip() == __version__

def test_help_page():
  runner = CliRunner()
  result = runner.invoke(cli, ['--help'])
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
    result = runner.invoke(login, input="http://localhost:9200/\nwrong_username\nwrong_password\n")
    assert result.exit_code == 0
    assert "Authentication failed" in result.output
    assert not os.path.isfile(os.path.join(temp_dir, CONFIG_FILE_PATH))


@patch("connectors.cli.auth.Auth._Auth__ping_es_client", AsyncMock(return_value=True))
def test_login_successful(tmp_path):
  runner = CliRunner()
  with runner.isolated_filesystem(temp_dir=tmp_path) as temp_dir:
    result = runner.invoke(login, input="http://localhost:9200/\nwrong_username\nwrong_password\n")
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

    result = runner.invoke(login, input="http://localhost:9200/\ncorrect_username\ncorrect_password\n")
    assert result.exit_code == 0
    assert mocked_confirm.called_once()
