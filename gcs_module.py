#!/usr/bin/env python
from bq_module import *


def export_to_table(json_key,
                    sql,
                    dataset_name,
                    table_name,
                    external_udf_uris=None,
                    write_disposition=None):
    ''' Function to run query and export to BQ dataset table.
    Overwrite table data. Table schema is inferred by the query result

    Args:
        json_key: String, Json key filename with credentials

        sql: String, sql query to pose against dataset

        dataset_name: String, The BQ name

        table_name: String the table name we want to store
        query results data in

        external_udf_uris: A list (strings) of external resources
         in Google Storage

        write_disposition: String, the mode of writing data into
        the table eg WRITE_TRUNCATE


    Returns:
            job_resource: job resource object if write went through
                or None, if BigQueryTimeoutException

    Raises:
        BigQueryTimeoutException

    '''

    client = connect_client(json_key_file=json_key)
    # In this example just write to my_data table and
    job = client.write_to_table(
      sql,
      dataset_name,
      table_name,
      external_udf_uris,
      write_disposition
    )

    try:
        job_resource = client.wait_for_job(job, timeout=60)
        return job_resource
    except BigQueryTimeoutException:
        print "Timeout"
        return None


def export_to_gcs(json_key,
                  dataset_name,
                  table_name,
                  external_udf_uris=None,
                  ):
    ''' Function to run export from BQ dataset table to GCS file (CSV).
        Auto generate filename.csv if not given
    Args:
        json_key: String, Json key filename with credentials

        dataset_name: String, The from BQ dataset name

        table_name: String the table name we want to extract
        query results data from

        external_udf_uris: A list (strings) of external resources
         in Google Storage to store CSV in.


    Returns:
            job_resource: job resource object if write went through
                or None, if BigQueryTimeoutException

    Raises:
        BigQueryTimeoutException

    '''
    client = connect_client(json_key_file=json_key)
    job = client.export_data_to_uris(external_udf_uris,
                                     dataset_name,
                                     table_name)
    try:
        job_resource = client.wait_for_job(job, timeout=60)
        print job_resource
    except BigQueryTimeoutException:
        print "Timeout"