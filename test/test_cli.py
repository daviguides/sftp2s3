import pytest
from unittest import mock
from click.testing import CliRunner
from sftp_to_s3_sync.cli import main

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert "--config-file" in result.output
    assert "--log-level" in result.output

def test_cli_invalid_log_level():
    runner = CliRunner()
    result = runner.invoke(main, ['--log-level', 'INVALID'])
    assert result.exit_code != 0
    assert "Invalid value for '--log-level'" in result.output

def test_cli_missing_config(monkeypatch):
    runner = CliRunner()
    monkeypatch.setenv('S3_SFTP_SYNC__S3_BUCKET', '')
    result = runner.invoke(main, ['--config-file', 'nonexistent.conf'])
    assert result.exit_code != 0
    assert "Configuration file not found" in result.output

@mock.patch('sftp_to_s3_sync.cli.connect_sftp')
@mock.patch('sftp_to_s3_sync.cli.connect_s3')
def test_sync_success(mock_connect_s3, mock_connect_sftp):
    # Mock S3 client
    s3_mock = mock.Mock()
    mock_connect_s3.return_value = s3_mock

    # Mock SFTP client
    sftp_mock = mock.MagicMock()
    open_sftp_mock = mock.MagicMock()
    sftp_mock.open_sftp.return_value.__enter__.return_value = open_sftp_mock
    open_sftp_mock.listdir_attr.return_value = []
    mock_connect_sftp.return_value = sftp_mock

    runner = CliRunner()
    with runner.isolated_filesystem():
        with open('config_mock.yaml', 'w') as f:
            f.write("dummy: valid content")
        result = runner.invoke(main, ['--config-file', 'config_mock.yaml', '--log-level', 'INFO'])

    assert result.exit_code == 0
