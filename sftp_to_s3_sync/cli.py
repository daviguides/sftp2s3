import sys
import os
import datetime
import hashlib
import yaml
import click
import boto3
import botocore
import paramiko
import stat
import logging

@click.command(name="sftp2s3")
@click.option('--config-file',
              default='./config.conf',
              help='Path to config file')
@click.option('--log-level', default='INFO',
              type=click.Choice(
                  ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set logging level')
def main(config_file: str, log_level: str) -> None:
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO), format="[%(asctime)s] [%(levelname)s] %(message)s")
    global logger
    logger = logging.getLogger(__name__)

    config = load_config(config_file)
    s3_client = connect_s3(config['s3'])
    sftp_client = connect_sftp(
        hostname=config['sftp']['hostname'],
        username=config['sftp']['username'],
        password=config['sftp']['password'],
        port=config['sftp'].get('port', 22)  # Pass the port from config, default to 22
    )

    try:
        sync_sftp_to_s3(sftp_client, s3_client, config)
    except Exception as e:
        raise click.ClickException(f"An error occurred: {str(e)}")
    finally:
        sftp_client.close()


def load_config(config_file: str) -> dict:
    if not os.path.exists(config_file):
        raise click.ClickException(f"Configuration file not found: {config_file}")
    logger.info(f"Loading config from {config_file}...")
    try:
        with open(config_file) as file:
            config = yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading configuration file: {str(e)}")
        raise click.ClickException("Unable to load configuration file. Please check the file path and permissions.")

    def safe_get(obj, key, default=None):
        return obj[key] if obj and key in obj else default

    config.setdefault('s3', {})
    config['s3']['bucket'] = os.getenv(
        'S3_SFTP_SYNC__S3_BUCKET',
        safe_get(config['s3'], 'bucket')
    )
    config['s3']['key_prefix'] = os.getenv(
        'S3_SFTP_SYNC__S3_KEY_PREFIX',
        safe_get(config['s3'], 'key_prefix')
    )
    config['s3']['aws_access_key_id'] = os.getenv(
        'S3_SFTP_SYNC__AWS_ACCESS_KEY_ID',
        safe_get(config['s3'], 'aws_access_key_id')
    )
    config['s3']['aws_secret_access_key'] = os.getenv(
        'S3_SFTP_SYNC__AWS_SECRET_ACCESS_KEY',
        safe_get(config['s3'], 'aws_secret_access_key')
    )
    config['s3']['endpoint_url'] = os.getenv(
        'S3_SFTP_SYNC__S3_ENDPOINT_URL',
        safe_get(config['s3'], 'endpoint_url')
    )

    config.setdefault('sftp', {})
    config['sftp']['hostname'] = os.getenv(
        'S3_SFTP_SYNC__SFTP_HOSTNAME',
        safe_get(config['sftp'], 'hostname')
    )
    config['sftp']['username'] = os.getenv(
        'S3_SFTP_SYNC__SFTP_USERNAME',
        safe_get(config['sftp'], 'username')
    )
    config['sftp']['password'] = os.getenv(
        'S3_SFTP_SYNC__SFTP_PASSWORD',
        safe_get(config['sftp'], 'password')
    )
    config['sftp']['port'] = os.getenv(
        'S3_SFTP_SYNC__SFTP_PORT',
        safe_get(config['sftp'], 'port', 22)  # Default to 22 if not provided
    )

    config.setdefault('incremental_sync', {})
    config['incremental_sync']['last_modified_s3_key'] = os.getenv(
        'S3_SFTP_SYNC__SFTP_LAST_MODIFIED_S3_KEY',
        safe_get(config['incremental_sync'], 'last_modified_s3_key')
    )

    return config


def connect_s3(s3_config: dict) -> botocore.client.BaseClient:
    bucket = s3_config['bucket']
    aws_access_key_id = s3_config['aws_access_key_id']
    aws_secret_access_key = s3_config['aws_secret_access_key']

    if not all([bucket, aws_access_key_id, aws_secret_access_key]):
        logger.error("S3 configuration is incomplete. Please verify your configuration file or environment variables.")
        raise click.ClickException("Incomplete S3 configuration. Please verify your settings.")

    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=s3_config.get('endpoint_url')
    )


def sync_sftp_to_s3(
    sftp_client: paramiko.SSHClient,
    s3_client: botocore.client.BaseClient,
    config: dict
) -> None:
    bucket = config['s3']['bucket']
    key_prefix = config['s3']['key_prefix'] or ''
    marker_key = config['incremental_sync']['last_modified_s3_key']

    start_time = load_start_time_from_s3(s3_client, bucket, marker_key)
    last_modified = start_time

    num_files_synced = 0
    num_bytes_synced = 0

    with sftp_client.open_sftp() as sftp:
        files = list_files_recursively(sftp)

        for file_path in files:
            stats = sftp.stat(file_path)
            mtime = stats.st_mtime
            size = stats.st_size

            if should_sync_file(mtime, start_time):
                with sftp.file(file_path) as file_obj:
                    if needs_upload(
                        s3_client,
                        bucket,
                        key_prefix,
                        file_path,
                        file_obj,
                        mtime,
                        start_time
                    ):
                        normalized_path = os.path.normpath(file_path).lstrip('/')
                        upload_file_to_s3(s3_client, bucket, key_prefix + normalized_path, file_obj, mtime)
                        num_files_synced += 1
                        num_bytes_synced += size
                    else:
                        logger.info(f"{file_path}: no changes detected.")

            if last_modified is None or mtime > last_modified:
                last_modified = mtime

    if marker_key and last_modified != start_time:
        update_last_modified_marker(s3_client, bucket, marker_key, last_modified)

    logger.info(f"Finished: {num_files_synced} files synced, {num_bytes_synced} bytes total.")


def load_start_time_from_s3(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    marker_key: str
) -> int | None:
    if not marker_key:
        return None
    try:
        response = s3_client.get_object(Bucket=bucket, Key=marker_key)
        start_time = int(response['Body'].read().decode('utf-8'))
        logger.info(f"Using incremental sync with start_time {start_time}.")
        return start_time
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error'].get('Code')
        if error_code == 'NoSuchKey':
            logger.warning(f"Incremental sync marker not found (NoSuchKey): {e}")
            return None
        logger.error(f"Error loading incremental sync marker: {e}")
        raise click.ClickException("Unable to load incremental sync marker. Please check your S3 configuration.")


def should_sync_file(mtime: int, start_time: int | None) -> bool:
    return start_time is None or mtime >= start_time


def needs_upload(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    key_prefix: str,
    file_path: str,
    file_obj,
    mtime: int,
    start_time: int | None
) -> bool:
    if mtime == start_time:
        s3_hash = s3_md5(s3_client, bucket, key_prefix + file_path)
        file_hash = file_md5(file_obj) if s3_hash else None
        return s3_hash != file_hash
    return True


def upload_file_to_s3(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    full_key: str,
    file_obj,
    mtime: int
) -> None:
    logger.info(f"Uploading {full_key}...")
    s3_client.put_object(
        Bucket=bucket,
        Key=full_key,
        Body=file_obj,
        Metadata={
            'sftp_mtime': str(mtime),
            'sftp_sync_time': datetime.datetime.utcnow().isoformat()
        }
    )


def update_last_modified_marker(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    marker_key: str,
    last_modified: int
) -> None:
    logger.info(f"Updating incremental sync marker to {last_modified}.")
    s3_client.put_object(
        Bucket=bucket,
        Key=marker_key,
        Body=str(last_modified).encode('utf-8')
    )


def connect_sftp(hostname: str, username: str, password: str, port: int) -> paramiko.SSHClient:
    logger.info(f"Connecting to SFTP {hostname} on port {port}...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=hostname,
            username=username,
            password=password,
            port=port,  # Use the provided port
            timeout=5
        )
        client.get_transport().set_keepalive(30)
        logger.info("SFTP connected.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to SFTP: {e}")
        raise click.ClickException("Unable to connect to the SFTP server. Please verify your SFTP details.")


def list_files_recursively(sftp, directory: str = ".") -> list:
    logger.info("Listing all files in SFTP recursively...")
    all_files = []

    def _list(path):
        for entry in sftp.listdir_attr(path):
            full_path = os.path.join(path, entry.filename)
            if stat.S_ISDIR(entry.st_mode):
                _list(full_path)
            else:
                all_files.append(full_path)

    _list(directory)
    return all_files


def s3_md5(s3_client: botocore.client.BaseClient, bucket: str, key: str) -> str | None:
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ETag'].strip('"').strip("'")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != '404':
            logger.error(f"Error fetching S3 object metadata: {e}")
            raise click.ClickException("Unable to fetch S3 object metadata. Please check your S3 configuration.")
        return None


def file_md5(file_obj) -> str:
    hash_md5 = hashlib.md5()
    while chunk := file_obj.read(10240):
        hash_md5.update(chunk)
    file_obj.seek(0)
    return hash_md5.hexdigest()