"""Daily BigQuery-to-SFTP feed pipeline.

Extracts data from BigQuery, stages it as CSV files, uploads the files
to a remote SFTP server, and archives a copy of each file in Cloud
Storage.

Deployment shape: this module is deployed as a Cloud Run function
(2nd gen) with a Pub/Sub (Eventarc) trigger. Cloud Scheduler publishes a
message to that Pub/Sub topic every day at 09:00, which invokes `main`
below and kicks off `run_pipeline`. See README.md for the full
architecture and deployment steps.
"""

import json
import logging
import os
import time
from datetime import datetime

import functions_framework
import paramiko
from google.cloud import bigquery, secretmanager, storage
from google.oauth2 import service_account
from jinja2 import Environment, FileSystemLoader

import configurations as config

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

# SFTP connection details are set as environment variables directly on the
# Cloud Run function (Console / `gcloud run deploy --set-env-vars`), not in
# configurations.py, so they never end up committed to source control.
SFTP_HOSTNAME = os.environ.get("HOSTNAME")
SFTP_USERNAME = os.environ.get("USERNAME")
SFTP_PASSWORD = os.environ.get("PASSWORD")
SFTP_PORT = os.environ.get("PORT_NUMBER")

LOCAL_STAGING_DIR = "/tmp/uploads"
REMOTE_STAGING_DIR = "/uploads"

SQL_DIR = "sql"

GCP_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/devstorage.full_control",
]


def get_gcp_credentials():
    """Builds GCP credentials from a service-account key stored in Secret Manager.

    The key is fetched lazily (not at import time) so that local testing
    and unit tests can import this module without making a network call.

    Returns:
        google.oauth2.service_account.Credentials: Credentials scoped for
        BigQuery, Drive and Cloud Storage access.
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_path = (
        f"projects/{config.PROJECT_ID}/secrets/{config.SECRET_NAME}"
        f"/versions/{config.SECRET_VERSION}"
    )
    response = client.access_secret_version(request={"name": secret_path})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))
    return service_account.Credentials.from_service_account_info(
        service_account_info, scopes=GCP_SCOPES
    )


class SFTPServerClient:
    """Context-manager wrapper around a paramiko SSH/SFTP connection."""

    def __init__(self, hostname, port, username, password):
        """Stores connection parameters; the connection is opened lazily.

        Args:
            hostname: SFTP server hostname or IP address.
            port: SFTP server port number.
            username: Username for authentication.
            password: Password for authentication.
        """
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client = paramiko.SSHClient()
        self.connected = False

    def connect(self):
        """Opens the SSH connection.

        Two non-default paramiko options are used here:

        - `set_missing_host_key_policy(paramiko.AutoAddPolicy())`: paramiko's
          default policy (`RejectPolicy`) refuses to connect to any host
          whose SSH public key isn't already in a known_hosts file. A Cloud
          Run container has no persistent filesystem/known_hosts between
          invocations, so without `AutoAddPolicy` every single run would
          fail with an "unknown host key" error. `AutoAddPolicy` accepts and
          trusts the server's host key for the duration of the connection
          instead of verifying it against a stored fingerprint. This trades
          away protection against man-in-the-middle attacks on the SSH
          transport, which is an acceptable trade-off for a known, fixed
          partner endpoint reached over a private/trusted network - it
          would not be appropriate for connecting to arbitrary or
          untrusted hosts.
        - `look_for_keys=False`: by default paramiko searches `~/.ssh/` for
          private keys (id_rsa, id_dsa, etc.) to try as additional auth
          options. The Cloud Run container has no such directory and no
          SSH keys, and this server is authenticated purely with a
          username/password, so disabling key lookup skips that filesystem
          scan and avoids noisy "no such file" warnings on every connect.

        Returns:
            bool: True if the connection succeeded, False otherwise.
        """
        try:
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                look_for_keys=False,
            )
            logging.info(f"Connected to server {self.hostname}:{self.port}:{self.username}")
            self.connected = True
            return True
        except Exception as e:
            logging.error(f"The connect function failed with error: {e}")
            return False

    def disconnect(self):
        """Closes the SSH connection."""
        try:
            self.ssh_client.close()
            self.connected = False
            logging.info("Disconnected from server.")
        except Exception as e:
            logging.error(f"The connection to server was not disconnected. Check errors: {e}")

    def __enter__(self):
        if not self.connected and not self.connect():
            raise ConnectionError("Connection to SFTP server failed")
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.disconnect()

    def upload_directory(self, local_dir, remote_dir):
        """Uploads every file in a local directory to a remote directory.

        Args:
            local_dir: Path to the local directory whose files are uploaded.
            remote_dir: Destination directory on the SFTP server.
        """
        with self.ssh_client.open_sftp() as sftp_client:
            for file_name in os.listdir(local_dir):
                local_path = os.path.join(local_dir, file_name)
                remote_path = f"{remote_dir}/{file_name}"
                try:
                    sftp_client.put(local_path, remote_path)
                    logging.info(f"File uploaded: {local_path} -> {remote_path}")
                except FileNotFoundError as e:
                    logging.error(f"File not found: {e}")
                except Exception as e:
                    logging.error(f"Error uploading file: {e}")


class BigQueryClient:
    """Runs the extraction SQL and publishes the results to GCS."""

    def __init__(self, project_id, credentials):
        """Initialises the BigQuery and Cloud Storage clients.

        Args:
            project_id: GCP project ID to bill queries against.
            credentials: google.oauth2.service_account.Credentials used for
                both BigQuery and Cloud Storage.
        """
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.storage_client = storage.Client(project=project_id, credentials=credentials)
        self.project_id = project_id

    def execute_query_and_store_in_tmp(self, poll_interval_seconds=5):
        """Renders and runs every SQL template in sql/, saving results as CSV.

        Each `<name>.sql` file in the sql/ directory is rendered as a Jinja2
        template, executed as a BigQuery query, and the result is written to
        `/tmp/uploads/<name>.csv`.

        Args:
            poll_interval_seconds: Seconds to wait between job status checks.

        Raises:
            Exception: If a BigQuery job fails or ends in an unexpected state.
        """
        env = Environment(
            loader=FileSystemLoader(os.path.join(os.getcwd(), SQL_DIR)),
            keep_trailing_newline=False,
            lstrip_blocks=True,
            trim_blocks=True,
        )
        for sql_file in os.listdir(SQL_DIR):
            sql_template = env.get_template(sql_file).render()
            query_job = self.client.query(query=sql_template)
            while True:
                query_job.reload()
                if query_job.state == "DONE":
                    if query_job.error_result is not None:
                        raise Exception(f"BigQuery job failed: {query_job.error_result}")
                    break
                if query_job.state in ("RUNNING", "PENDING"):
                    time.sleep(poll_interval_seconds)
                else:
                    raise Exception(f"Unexpected job state: {query_job.state}")

            output_file_name = f"{os.path.splitext(sql_file)[0]}.csv"
            output_file_path = os.path.join(LOCAL_STAGING_DIR, output_file_name)
            query_job.result().to_dataframe().to_csv(output_file_path, index=False)
            logging.info(f"Query result for {sql_file} written to {output_file_path}")

    def upload_csv_to_gcs(self, bucket_name):
        """Uploads every CSV in the local staging directory to Cloud Storage.

        Files are stored under a `YYYY-MM-DD/` prefix matching today's date.

        Args:
            bucket_name: Name of the destination GCS bucket.
        """
        bucket = self.storage_client.bucket(bucket_name)
        date_prefix = datetime.now().strftime("%Y-%m-%d")
        for file_name in os.listdir(LOCAL_STAGING_DIR):
            local_file_path = os.path.join(LOCAL_STAGING_DIR, file_name)
            blob = bucket.blob(f"{date_prefix}/{file_name}")
            blob.upload_from_filename(local_file_path, content_type="text/csv")
            logging.info(
                f"File {local_file_path} uploaded to gs://{bucket_name}/{date_prefix}/{file_name}."
            )


def run_pipeline():
    """Extracts data from BigQuery and ships the results to SFTP and GCS."""
    logging.info(f"Pipeline run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    os.makedirs(LOCAL_STAGING_DIR, exist_ok=True)

    credentials = get_gcp_credentials()
    bq_client = BigQueryClient(config.PROJECT_ID, credentials)
    bq_client.execute_query_and_store_in_tmp()

    with SFTPServerClient(
        hostname=SFTP_HOSTNAME, port=SFTP_PORT, username=SFTP_USERNAME, password=SFTP_PASSWORD
    ) as sftp:
        sftp.upload_directory(LOCAL_STAGING_DIR, REMOTE_STAGING_DIR)

    bq_client.upload_csv_to_gcs(config.GCS_BUCKET_NAME)
    logging.info("Pipeline run finished successfully")


@functions_framework.cloud_event
def main(cloud_event):
    """Cloud Run function entry point, invoked via the Pub/Sub Eventarc trigger.

    Cloud Scheduler publishes a message to the configured Pub/Sub topic
    every day at 09:00; the message payload itself is not used, it only
    acts as the daily trigger for `run_pipeline`.

    Args:
        cloud_event: cloudevents.http.CloudEvent delivered by Eventarc.
    """
    logging.info(f"Triggered by Pub/Sub event: {cloud_event['id']}")
    run_pipeline()


if __name__ == "__main__":
    # Lets the pipeline logic be exercised directly (`python main.py` or
    # `uv run main.py`) without going through the Functions Framework /
    # CloudEvent wrapper. See README.md for other local testing options.
    run_pipeline()
