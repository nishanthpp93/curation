"""
Copy all tables from a source dataset to a destination dataset.

Uses service account impersonation.
"""
import logging

from google.cloud import bigquery
from google.oauth2 import service_account

from utils.pipeline_logging import setup_logger

LOGGER = logging.getLogger(__name__)
LIST_TABLES_MAX_RESULTS = 5000


def copy_tables(client: bigquery.Client, src_dataset: bigquery.DatasetReference,
                dest_dataset: bigquery.DatasetReference):
    """
    Copy all tables associated in src_dataset to dest_dataset

    :param client: Active bigquery client object
    :param src_dataset: the dataset to copy tables from
    :param dest_dataset: the dataset to copy tables to

    :return: Query job associated with removing all the records
    :raises RuntimeError if CDM tables associated with a site are not found in the dataset
    """
    LOGGER.debug(f'purge_hpo_data called with dataset={src_dataset.dataset_id}')
    all_tables = list(
        client.list_tables(dataset=src_dataset,
                           max_results=LIST_TABLES_MAX_RESULTS))

    copy_config = bigquery.job.CopyJobConfig(write_disposition='WRITE_TRUNCATE')
    errors = []
    for src_table in all_tables:
        table_id = src_table.table_id
        dest_table = dest_dataset.table(table_id)

        LOGGER.info(
            f"Copying... {table_id} from {src_dataset.project}.{src_dataset.dataset_id} "
            f"to {dest_dataset.project}.{dest_dataset.dataset_id}")

        job = client.copy_table(src_table,
                                dest_table,
                                job_id_prefix=table_id,
                                job_config=copy_config)
        job.result()
        if job.exception():
            errors.append(job.exception())
            LOGGER.error(
                f"FAILURE:  Unable to copy {table_id} "
                f"from {src_dataset.project}.{src_dataset.dataset_id} to "
                f"{dest_dataset.project}.{dest_dataset.dataset_id}")
        else:
            LOGGER.info(
                f"Copied {table_id} from {src_dataset.project}.{src_dataset.dataset_id} "
                f"to {dest_dataset.project}.{dest_dataset.dataset_id}")

    if errors:
        raise RuntimeError(
            "One or more tables failed to copy.  Check the logs.")


def main(credentials_file, project_id, src_dataset_id, dest_dataset_id):
    """
    Copy tables with impersonated credentials.

    :param credentials_file:  path to your service account key file
    :param project_id:  project id to copy tables between.
    :param src_dataset_id:  dataset containing tables to copy
    :param dest_dataset_id:  dataset to copy tables to
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file)
    client = bigquery.Client(project=project_id, credentials=credentials)
    src_dataset = bigquery.DatasetReference(project_id, src_dataset_id)
    dest_dataset = bigquery.DatasetReference(project_id, dest_dataset_id)
    copy_tables(client, src_dataset, dest_dataset)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-c',
                        '--credentials',
                        required=True,
                        help='Path to GCP credentials file')
    parser.add_argument('-p',
                        '--project_id',
                        required=True,
                        help='Identifies the project')
    parser.add_argument('-s',
                        '--src_dataset_id',
                        required=True,
                        help='Identifies the dataset to copy tables from')
    parser.add_argument('-d',
                        '--dest_dataset_id',
                        required=True,
                        help='Identifies the dataset to copy tables to')
    ARGS = parser.parse_args()

    setup_logger(['copy_logs.log'])
    main(ARGS.credentials, ARGS.project_id, ARGS.src_dataset_id,
         ARGS.dest_dataset_id)
