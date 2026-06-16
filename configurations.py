"""GCP configuration for the feed pipeline.

Fill in the empty values below before running or deploying this project.
SFTP connection details (HOSTNAME, USERNAME, PASSWORD, PORT_NUMBER) are
NOT configured here - they are set as environment variables directly on
the Cloud Run function (or in your local shell for testing). See
README.md > "Configuration" for the full setup walkthrough.

PROJECT_ID (str):
    The GCP project that runs the BigQuery jobs and hosts the Cloud Run
    function, Pub/Sub topic and Secret Manager secret.
    Find it on the Cloud Console home page, or via:
        gcloud config get-value project

GCS_BUCKET_NAME (str):
    Name of the Cloud Storage bucket the daily CSV extracts are archived
    to (no "gs://" prefix). Create it with:
        gcloud storage buckets create gs://<your-bucket-name> \
            --project=<PROJECT_ID> --location=<region, e.g. australia-southeast1>

SECRET_NAME (str):
    Name of the Secret Manager secret holding the service-account JSON
    key used to authenticate to BigQuery and Cloud Storage. Create it
    with:
        gcloud secrets create <SECRET_NAME> \
            --data-file=<path-to-service-account-key.json> \
            --project=<PROJECT_ID>
    The Cloud Run function's runtime service account must be granted the
    "Secret Manager Secret Accessor" role on this secret.

SECRET_VERSION (str):
    Version of the secret to read, e.g. "1" for the first version, or
    "latest" to always use the most recently added version. List
    versions with:
        gcloud secrets versions list <SECRET_NAME> --project=<PROJECT_ID>
"""

PROJECT_ID = ""
GCS_BUCKET_NAME = ""
SECRET_NAME = ""
SECRET_VERSION = ""
