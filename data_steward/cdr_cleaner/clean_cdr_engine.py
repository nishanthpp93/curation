# Python imports
import inspect
import logging
from concurrent.futures import TimeoutError as TOError

# Third party imports
import google.cloud.bigquery as gbq
from google.cloud.exceptions import GoogleCloudError

# Project imports
import app_identity
from utils import bq
import sandbox
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from constants.cdr_cleaner import clean_cdr_engine as ce_consts

LOGGER = logging.getLogger(__name__)


def add_console_logging(add_handler=True):
    """

    This config should be done in a separate module, but that can wait
    until later.  Useful for debugging.

    """
    logging.basicConfig(
        level=logging.INFO,
        filename=ce_consts.FILENAME,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if add_handler:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        handler.setFormatter(formatter)
        logging.getLogger('').addHandler(handler)


def generate_job_config(project_id, query_dict):
    """
    Generates BigQuery job_configuration object

    :param project_id: Identifies the project
    :param query_dict: dictionary for the query
    :return: BQ job_configuration object
    """
    job_config = gbq.job.QueryJobConfig()
    if query_dict.get(cdr_consts.DESTINATION_TABLE) is None:
        return job_config

    destination_table = gbq.TableReference.from_string(
        f'{project_id}.{query_dict[cdr_consts.DESTINATION_DATASET]}.{query_dict[cdr_consts.DESTINATION_TABLE]}'
    )

    job_config.destination = destination_table
    job_config.use_legacy_sql = query_dict.get(cdr_consts.LEGACY_SQL, False)
    # allow_large_results can only be used if use_legacy_sql=True
    job_config.allow_large_results = job_config.use_legacy_sql
    job_config.write_disposition = query_dict.get(cdr_consts.DISPOSITION,
                                                  bq_consts.WRITE_EMPTY)
    return job_config


def run_queries(client, query_list, rule_info):
    """
    Runs queries from the list of query_dicts

    :param client: BigQuery client
    :param query_list: list of query_dicts generated by a cleaning rule
    :param rule_info: contains information about the query function
    :return: integers indicating the number of queries that succeeded and failed
    """
    query_count = len(query_list)
    jobs = []
    for query_no, query_dict in enumerate(query_list):
        try:
            LOGGER.info(
                ce_consts.QUERY_RUN_MESSAGE_TEMPLATE.render(
                    query_no=query_no, query_count=query_count, **rule_info))
            job_config = generate_job_config(client.project, query_dict)

            module_short_name = rule_info[cdr_consts.MODULE_NAME].split(
                '.')[-1][:10]
            query_job = client.query(query=query_dict.get(cdr_consts.QUERY),
                                     job_config=job_config,
                                     job_id_prefix=f'{module_short_name}_')
            jobs.append(query_job)
            LOGGER.info(f'Running {query_job.job_id}')
            # wait for job to complete
            query_job.result()
            if query_job.errors:
                raise RuntimeError(
                    ce_consts.FAILURE_MESSAGE_TEMPLATE.render(
                        client.project, query_job, **rule_info, **query_dict))
            LOGGER.info(
                ce_consts.SUCCESS_MESSAGE_TEMPLATE.render(
                    project_id=client.project,
                    query_job=query_job,
                    query_no=query_no,
                    query_count=query_count,
                    **rule_info))
        except (GoogleCloudError, TOError) as exp:
            LOGGER.exception(
                ce_consts.FAILURE_MESSAGE_TEMPLATE.render(
                    project_id=client.project,
                    **rule_info,
                    **query_dict,
                    exception=exp))
            raise exp
    return jobs


def infer_rule(clazz,
               project_id,
               dataset_id,
               sandbox_dataset_id,
               combined_dataset_id=None):
    """
    Extract information about the cleaning rule

    :param clazz: Clean rule class or old style clean function
    :param project_id: identifies the project
    :param dataset_id: identifies the dataset to clean
    :param sandbox_dataset_id: identifies the dataset to store sandbox tables
    :param combined_dataset_id: supply combined_dataset_id if cleaning rule requires
    :return:
        query_function: function that generates query_list
        setup_function: function that sets up the tables for the rule
        rule_info: dictionary of information about the rule
            keys:
                query_function: function that generates query_list
                setup_function: function that sets up the tables for the rule
                function_name: name of the query function
                module_name: name of the module containing the function
                line_no: points to the source line where query_function is
    """
    params = inspect.signature(clazz).parameters
    if inspect.isclass(clazz) and issubclass(clazz, BaseCleaningRule):
        if 'combined_dataset_id' in params:
            if len(params) > 3:
                instance = clazz(project_id, dataset_id, sandbox_dataset_id,
                                 combined_dataset_id)
            else:
                instance = clazz(project_id, dataset_id, combined_dataset_id)
        else:
            instance = clazz(project_id, dataset_id, sandbox_dataset_id)

        query_function = instance.get_query_specs
        setup_function = instance.setup_rule
        function_name = query_function.__name__
        module_name = inspect.getmodule(query_function).__name__
        line_no = inspect.getsourcelines(query_function)[1]
    else:
        function_name = clazz.__name__
        module_name = inspect.getmodule(clazz).__name__
        line_no = inspect.getsourcelines(clazz)[1]

        def query_function():
            """
            Imitates base class get_query_specs()

            :return: list of query dicts generated by rule
            """
            if 'combined_dataset_id' in params:
                if len(params) > 3:
                    return clazz(project_id, dataset_id, sandbox_dataset_id,
                                 combined_dataset_id)
                return clazz(project_id, dataset_id, combined_dataset_id)
            if len(params) == 3:
                return clazz(project_id, dataset_id, sandbox_dataset_id)
            return clazz(project_id, dataset_id)

        def setup_function(client):
            """
            Imitates base class setup_rule()
            """
            pass

    rule_info = {
        cdr_consts.QUERY_FUNCTION: query_function,
        cdr_consts.SETUP_FUNCTION: setup_function,
        cdr_consts.FUNCTION_NAME: function_name,
        cdr_consts.MODULE_NAME: module_name,
        cdr_consts.LINE_NO: line_no,
    }
    return query_function, setup_function, rule_info


def get_query_list(project_id=None,
                   dataset_id=None,
                   rules=None,
                   combined_dataset_id=None):
    """
    Generates list of all query_dicts that will be run on the dataset

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset to clean
    :param rules: a list of cleaning rule objects/functions as tuples
    :param combined_dataset_id: supply combined_dataset_id if cleaning rule requires
    :return list of all query_dicts that will be run on the dataset
    """
    if project_id is None or project_id == '' or project_id.isspace():
        project_id = app_identity.get_application_id()
        LOGGER.info(f"project_id not provided, using default {project_id}")

    sandbox_dataset_id = sandbox.get_sandbox_dataset_id(dataset_id)

    all_queries_list = []
    for rule in rules:
        clazz = rule[0]
        query_function, _, rule_info = infer_rule(clazz, project_id, dataset_id,
                                                  sandbox_dataset_id,
                                                  combined_dataset_id)
        query_list = query_function()
        all_queries_list.extend(query_list)
    return all_queries_list


def clean_dataset(project_id=None,
                  dataset_id=None,
                  rules=None,
                  combined_dataset_id=None):
    """
    Run the assigned cleaning rules and return list of BQ job objects

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset to clean
    :param rules: a list of cleaning rule objects/functions as tuples
    :param combined_dataset_id: identifies the combined_dataset if required
    :return all_jobs: List of BigQuery job objects
    """
    if project_id is None or project_id == '' or project_id.isspace():
        project_id = app_identity.get_application_id()
        LOGGER.info(f"project_id not provided, using default {project_id}")

    # Set up
    client = bq.get_client(project_id=project_id)
    sandbox_dataset_id = sandbox.get_sandbox_dataset_id(dataset_id)
    labels = {'type': 'sandbox', 'dataset_id': dataset_id}
    sandbox_dataset = bq.define_dataset(project_id,
                                        sandbox_dataset_id,
                                        description=f'Sandbox for {dataset_id}',
                                        label_or_tag=labels)
    sandbox_dataset = client.create_dataset(sandbox_dataset, exists_ok=True)
    sandbox_dataset_id = sandbox_dataset.dataset_id

    all_jobs = []
    for rule in rules:
        clazz = rule[0]
        query_function, setup_function, rule_info = infer_rule(
            clazz, project_id, dataset_id, sandbox_dataset_id,
            combined_dataset_id)
        setup_function(client)
        query_list = query_function()
        jobs = run_queries(client, query_list, rule_info)
        LOGGER.info(
            f"For clean rule {rule_info[cdr_consts.MODULE_NAME]}, {len(jobs)} jobs "
            f"were run successfully for {len(query_list)} queries")
        all_jobs.extend(jobs)
    return all_jobs
