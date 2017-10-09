#!/usr/bin/env python

import json
import six
from time import sleep, time
from httplib2 import Http
from hashlib import sha256

from googleapiclient.discovery import build, DISCOVERY_URI
from googleapiclient.errors import HttpError


def connect_client(json_key_file):
    """Return a client connection to the BigQuery API.
    A local JSON key file must be provided for authentication

    Args:
        json_file: A locally downloaded JSON file with connection
        /authentication info

    Returns:
        client: A BQ client object

    Raises:
    """

    # TODO check if json_key_file exists
    if json_key_file:
        with open(json_key_file, 'r') as key_file:
            json_key = json.load(key_file)
            credentials = _credentials().from_json_keyfile_dict(json_key,
                        scopes='https://www.googleapis.com/auth/bigquery')
            project_id = json_key['project_id']

    bq_service = _get_bq_service(credentials=credentials,
                                 service_url=DISCOVERY_URI)

    return BigQueryClient(bq_service, project_id)


def _credentials():
    """Import and return SignedJwtAssertionCredentials class"""
    from oauth2client.service_account import ServiceAccountCredentials

    return ServiceAccountCredentials


def _get_bq_service(credentials=None, service_url=None):
    """Construct an authorized BigQuery service object."""

    assert credentials, 'Must provide ServiceAccountCredentials'

    http = credentials.authorize(Http())
    service = build('bigquery', 'v2', http=http,
                    discoveryServiceUrl=service_url)

    return service


class BigQueryClient(object):

    def __init__(self, bq_service, project_id):
        self.bigquery = bq_service
        self.project_id = project_id

    def _submit_job(self, config):
        """ Submit a job to BigQuery

        Args:

            config: job config details

        Returns:

            BigQuery API execute response

        Raises:

            BigQueryTimeoutException on timeout
        """

        bq_jobs = self.bigquery.jobs()

        return bq_jobs.insert(
            projectId=self.project_id,
            body=config
        ).execute()

    def write_to_table(
            self,
            query,
            dataset=None,
            table=None,
            external_udf_uris=None,
            write_disposition=None,
    ):
        """
        Write query result to table. If dataset or table is not provided,
        bq will write the result to temporary table.


        Args:
            query : string
                BigQuery query string
            dataset : string, optional
                String id of the dataset
            table : string, optional
                String id of the table
            external_udf_uris : list(string)
                Contains external UDF URIs. If given, URIs must be Google Cloud
                Storage and have .js extensions.
            write_disposition : configuration parameter,
                see API (example WRITE_TRUNCATE)


        Returns:
            BigQuery job resource as dict

        Raises:

            JobInsertException
                On http/auth failures or error in result
        """

        configuration = {
            "query": query,
        }

        if dataset and table:
            configuration['destinationTable'] = {
                "projectId": self.project_id,
                "tableId": table,
                "datasetId": dataset
            }

        if external_udf_uris:
            configuration['userDefinedFunctionResources'] = \
                [{'resourceUri': u} for u in external_udf_uris]

        if write_disposition:
            configuration['writeDisposition'] = write_disposition

        body = {
            "configuration": {
                'query': configuration
            }
        }

        job_resource = self._submit_job(body)
        self._raise_insert_exception_if_error(job_resource)
        return job_resource

    def wait_for_job(self, job, interval=5, timeout=60):
        """
        Waits until the job indicated by job_resource is done or has failed

        Args:
            job : Union[dict, str]
                ``dict`` representing a BigQuery job resource, or a ``str``
                representing the BigQuery job id
            interval : float, optional
                Polling interval in seconds, default = 5
            timeout : float, optional
                Timeout in seconds, default = 60

        Returns:

            dict
                Final state of the job resource

        Raises:

             JobExecutingException or BigQueryTimeoutException
                On http/auth failures or timeout
        """
        complete = False
        job_id = str(job if isinstance(job,
                                       (six.binary_type, six.text_type, int))
                     else job['jobReference']['jobId'])
        job_resource = None

        start_time = time()
        elapsed_time = 0
        while not (complete or elapsed_time > timeout):
            sleep(interval)
            request = self.bigquery.jobs().get(projectId=self.project_id,
                                               jobId=job_id)
            job_resource = request.execute()
            self._raise_executing_exception_if_error(job_resource)
            complete = job_resource.get('status').get('state') == u'DONE'
            elapsed_time = time() - start_time

        # raise exceptions if timeout
        if not complete:
            raise BigQueryTimeoutException()

        return job_resource

    def export_data_to_uris(
                self,
                destination_uris,
                dataset,
                table,
                job=None,
                compression=None,
                destination_format=None,
                print_header=None,
                field_delimiter=None,
                ):
            """
            Export data from a BigQuery table to cloud storage.

            Args:
                destination_uris : Union[str, list]
                    ``str`` or ``list`` of ``str`` objects representing the URIs on
                    cloud storage of the form: gs://bucket/filename
                dataset : str
                    String id of the dataset
                table : str
                    String id of the table
                job : str, optional
                    String identifying the job (a unique jobid is automatically
                    generated if not provided)
                compression : str, optional
                    One of the JOB_COMPRESSION_* constants
                destination_format : str, optional
                    One of the JOB_DESTination_FORMAT_* constants
                print_header : bool, optional
                    Whether or not to print the header
                field_delimiter : str, optional
                    Character separating fields in delimited file

            Returns:
                    A BigQuery job resource

            Raises:
                JobInsertException
                    On http/auth failures or error in result
            """
            destination_uris = destination_uris \
                if isinstance(destination_uris, list) else [destination_uris]

            configuration = {
                "sourceTable": {
                    "projectId": self.project_id,
                    "tableId": table,
                    "datasetId": dataset
                },
                "destinationUris": destination_uris,
            }

            if compression:
                configuration['compression'] = compression

            if destination_format:
                configuration['destinationFormat'] = destination_format

            if print_header is not None:
                configuration['printHeader'] = print_header

            if field_delimiter:
                configuration['fieldDelimiter'] = field_delimiter

            if not job:
                hex = self._generate_hex_for_uris(destination_uris)
                job = "{dataset}-{table}-{digest}".format(
                    dataset=dataset,
                    table=table,
                    digest=hex
                )

            body = {
                "configuration": {
                    'extract': configuration
                },
                "jobReference": {
                    "projectId": self.project_id,
                    "jobId": job
                }
            }

            job_resource = self._submit_job(body)
            self._raise_insert_exception_if_error(job_resource)
            return job_resource

    def _generate_hex_for_uris(self, uris):
        """Given uris, generate and return hex version of it

        Parameters
        ----------
        uris : list
            Containing all uris

        Returns
        -------
        str
            Hexed uris
        """
        return sha256((":".join(uris) + str(time())).encode()).hexdigest()

    def get_dataset(self, dataset_id):
        """Retrieve a dataset if it exists, otherwise return an empty dict.

        Parameters
        ----------
        dataset_id : str
            Dataset unique id

        Returns
        -------
        dict
            Contains dataset object if it exists, else empty
        """
        try:
            dataset = self.bigquery.datasets().get(
                projectId=self.project_id, datasetId=dataset_id).execute()
        except HttpError:
            dataset = {}

        return dataset

    def get_table_schema(self, dataset, table):
        """Return the table schema.

        Args:

            dataset : str
                The dataset containing the `table`.
            table : str
                The table to get the schema for

        Returns:
            list:
                A ``list`` of ``dict`` objects that represent the table schema.
                If the table doesn't exist, None is returned.
        """

        try:
            result = self.bigquery.tables().get(
                projectId=self.project_id,
                tableId=table,
                datasetId=dataset).execute()
        except HttpError as e:
            if int(e.resp['status']) == 404:
                return None
            raise

        return result['schema']['fields']

    # HTTP/REST API errors
    def _raise_insert_exception_if_error(self, job):
        error_http = job.get('error')
        if error_http:
            raise JobInsertException(
                "Error in export job API request: {0}".format(error_http))
        # handle errorResult - API request is successful but error in result
        error_result = job.get('status').get('errorResult')
        if error_result:
            raise JobInsertException(
                "Reason:{reason}. Message:{message}".format(**error_result))

    def _raise_executing_exception_if_error(self, job):
        error_http = job.get('error')
        if error_http:
            raise JobExecutingException(
                "Error in export job API request: {0}".format(error_http))
        # handle errorResult - API request is successful but error in result
        error_result = job.get('status').get('errorResult')
        if error_result:
            raise JobExecutingException(
                "Reason:{reason}. Message:{message}".format(**error_result))


# Some query classes
class UnfinishedQueryException(Exception):
    pass


class BigQueryTimeoutException(Exception):
    pass


class JobInsertException(Exception):
    pass


class JobExecutingException(Exception):
    pass
