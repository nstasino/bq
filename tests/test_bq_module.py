#!/usr/bin/env python
import unittest

import mock
from bigquery import client
from bigquery.errors import (
    JobInsertException, JobExecutingException,
    BigQueryTimeoutException
)
from googleapiclient.errors import HttpError
from nose.tools import raises


class HttpResponse(object):
    def __init__(self, status, reason='There was an error'):
        """
        Args:
            :param int status: Integer HTTP response status
        """
        self.status = status
        self.reason = reason


class TestConnectClient(unittest.TestCase):
    def setUp(self):
        client._bq_client = None

        self.mock_bq_service = mock.Mock()
        self.mock_job_collection = mock.Mock()

        self.mock_bq_service.jobs.return_value = self.mock_job_collection

        self.client = client.BigQueryClient(self.mock_bq_service, 'project')

    def test_no_credentials(self):
        """Ensure an Exception is raised when no credentials are provided."""

        self.assertRaises(AssertionError, client.get_client, 'foo')

    @mock.patch('bigquery.client._credentials')
    @mock.patch('bigquery.client.build')
    def test_initialize_json_key_file(self, mock_open, mock_build,
                                      mock_return_cred):
        """Ensure that a BigQueryClient is initialized and returned with
        read/write permissions using a JSON key file.
        """
        import json

        mock_cred = mock.Mock()
        mock_http = mock.Mock()
        mock_service_url = mock.Mock()
        mock_cred.from_json_keyfile_dict.return_value.authorize.return_value = mock_http
        mock_bq = mock.Mock()
        mock_build.return_value = mock_bq
        json_key_file = 'key.json'
        json_key = {'client_email': 'mail', 'private_key': 'pkey'}
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(json_key)
        project_id = 'project_id'
        mock_return_cred.return_value = mock_cred

        bq_client = client.get_client(
            project_id, service_url=mock_service_url,
            json_key_file=json_key_file)

        mock_return_cred.assert_called_once_with()
        mock_cred.from_json_keyfile_dict.assert_called_once_with(json_key,
                        scopes='https://www.googleapis.com/auth/bigquery')
        self.assertTrue(
            mock_cred.from_json_keyfile_dict.return_value.authorize.called)
        mock_build.assert_called_once_with('bigquery', 'v2', http=mock_http,
                                           discoveryServiceUrl=mock_service_url)
        self.assertEquals(mock_bq, bq_client.bigquery)
        self.assertEquals(project_id, bq_client.project_id)



@mock.patch('bigquery.client.BigQueryClient.get_query_results')
class TestCheckJob(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.project_id = 'project'
        self.client = client.BigQueryClient(mock.Mock(), self.project_id)

    def test_job_incomplete(self, mock_exec):
        """Ensure that we return None if the job is not yet complete."""

        mock_exec.return_value = {'jobComplete': False}

        is_completed, total_rows = self.client.check_job(1)

        self.assertFalse(is_completed)
        self.assertEquals(total_rows, 0)

    def test_query_complete(self, mock_exec):
        """Ensure that we can handle a normal query result."""

        mock_exec.return_value = {
            'jobComplete': True,
            'rows': [
                {'f': [{'v': 'bar'}, {'v': 'man'}]},
                {'f': [{'v': 'abc'}, {'v': 'xyz'}]}
            ],
            'schema': {
                'fields': [
                    {'name': 'foo', 'type': 'STRING'},
                    {'name': 'spider', 'type': 'STRING'}
                ]
            },
            'totalRows': '2'
        }

        is_completed, total_rows = self.client.check_job(1)

        self.assertTrue(is_completed)
        self.assertEquals(total_rows, 2)


class TestWaitForJob(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.project_id = 'project'
        self.api_mock = mock.Mock()
        self.client = client.BigQueryClient(self.api_mock, self.project_id)

    def test_completed_jobs(self):
        """Ensure we can detect completed jobs"""

        return_values = [{'status': {'state': u'RUNNING'},
                          'jobReference': {'jobId': "testJob"}},
                         {'status': {'state': u'DONE'},
                          'jobReference': {'jobId': "testJob"}}]

        def side_effect(*args, **kwargs):
            return return_values.pop(0)

        self.api_mock.jobs().get().execute.side_effect = side_effect

        job_resource = self.client.wait_for_job(
            {'jobReference': {'jobId': "testJob"},
             'status': {'state': u'RUNNING'}},
            interval=.01,
            timeout=.05)

        self.assertEqual(self.api_mock.jobs().get().execute.call_count, 2)
        self.assertIsInstance(job_resource, dict)

    def test_timeout_error(self):
        """Ensure that timeout raise exceptions"""
        incomplete_job = {'status': {'state': u'RUNNING'},
                          'jobReference': {'jobId': "testJob"}}

        self.api_mock.jobs().get().execute.return_value = incomplete_job
        self.assertRaises(BigQueryTimeoutException, self.client.wait_for_job,
                          incomplete_job, .1, .25)

    def test_wait_job_http_error(self):
        """ Test wait job with http error"""
        job = {'status': {'state': u'RUNNING'},
               'jobReference': {'jobId': "testJob"}}

        expected_result = {
            "error": {
                "errors": [{
                    "domain": "global",
                    "reason": "required",
                    "message": "Required parameter is missing"
                }],
                "code": 400,
                "message": "Required parameter is missing"
            }
        }

        self.api_mock.jobs().insert().execute.return_value = expected_result
        self.assertRaises(JobExecutingException,
                          self.client.wait_for_job,
                          job,
                          interval=.01,
                          timeout=.01)


class TestExportDataToURIs(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.mock_api = mock.Mock()

        self.project_id = 'project'
        self.dataset_id = 'dataset'
        self.table_id = 'table'
        self.destination_format = "CSV"
        self.print_header = False
        self.client = client.BigQueryClient(self.mock_api,
                                            self.project_id)

    @mock.patch('bigquery.client.BigQueryClient._generate_hex_for_uris')
    def test_export(self, mock_generate_hex):
        """ Ensure that export is working in normal circumstances """
        expected_result = {
            'status': {'state': u'RUNNING'},
        }

        body = {
            "jobReference": {
                "projectId": self.project_id,
                "jobId": "%s-%s-destinationuri" % (self.dataset_id,
                                                   self.table_id)
            },
            "configuration": {
                "extract": {
                    "destinationUris": ["destinationuri"],
                    "sourceTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                        "tableId": self.table_id
                    },
                    "destinationFormat": self.destination_format,
                    "printHeader": self.print_header,
                }
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        mock_generate_hex.return_value = "destinationuri"
        result = self.client.export_data_to_uris(
            ["destinationuri"], self.dataset_id, self.table_id,
            destination_format=self.destination_format,
            print_header=self.print_header
        )

        self.mock_api.jobs().insert.assert_called_with(
            projectId=self.project_id,
            body=body
        )

        self.assertEqual(result, expected_result)

    def test_export_http_error(self):
        """ Test export with http error"""
        expected_result = {
            "error": {
                "errors": [{
                    "domain": "global",
                    "reason": "required",
                    "message": "Required parameter is missing"
                }],
                "code": 400,
                "message": "Required parameter is missing"
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        self.assertRaises(JobInsertException,
                          self.client.export_data_to_uris,
                          ["destinationuri"],
                          self.dataset_id,
                          self.table_id)


class TestWriteToTable(unittest.TestCase):

    def setUp(self):
        client._bq_client = None
        self.mock_api = mock.Mock()

        self.query = 'foo'
        self.project_id = 'project'
        self.dataset_id = 'dataset'
        self.table_id = 'table'
        self.external_udf_uris = ['gs://bucket/external_udf.js']
        self.client = client.BigQueryClient(self.mock_api,
                                            self.project_id)

    def test_write(self):
        """ Ensure that write is working in normal circumstances."""
        expected_result = {
            'status': {'state': u'RUNNING'},
        }

        body = {
            "configuration": {
                "query": {
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                        "tableId": self.table_id
                    },
                    "query": self.query,
                    "userDefinedFunctionResources": [{
                        "resourceUri": self.external_udf_uris[0]
                    }]
                }
            }
        }

        self.mock_api.jobs().insert().execute.return_value = expected_result
        result = self.client.write_to_table(self.query,
                                            self.dataset_id,
                                            self.table_id,
                                            external_udf_uris=self.external_udf_uris,
)
        self.mock_api.jobs().insert.assert_called_with(
            projectId=self.project_id,
            body=body
        )

        self.assertEqual(result, expected_result)


class TestGetTableSchema(unittest.TestCase):

    def setUp(self):
        self.mock_bq_service = mock.Mock()
        self.mock_tables = mock.Mock()
        self.mock_bq_service.tables.return_value = self.mock_tables
        self.table = 'table'
        self.project = 'project'
        self.dataset = 'dataset'
        self.client = client.BigQueryClient(self.mock_bq_service, self.project)

    def test_table_exists(self):
        """Ensure that the table schema is returned if the table exists."""

        expected = [
            {'type': 'FLOAT', 'name': 'test1', 'mode': 'NULLABLE'},
            {'type': 'STRING', 'name': 'test2', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'test3', 'mode': 'NULLABLE'},
        ]

        self.mock_tables.get.return_value.execute.return_value = \
            {'schema': {'fields': expected}}

        self.assertEqual(
            expected, self.client.get_table_schema(self.dataset, self.table))
        self.mock_tables.get.assert_called_once_with(
            projectId=self.project, tableId=self.table, datasetId=self.dataset)
        self.mock_tables.get.return_value.execute. \
            assert_called_once_with(num_retries=0)

