#!/usr/bin/env python
from bq_module import *
from sql_module import *
from gcs_module import *



if __name__ == '__main__':
    # TODO: Next version use argparse

    # JSON key file with credentials
    json_key = 'My First Project-8a317759be48.json'
    '''TODO:
    The Following (ugly) SQL statement can be provided as an Argument or a text file
    or through Unix pipe (not recommended )
    TODO: Validate the String SQL using a lib.


    For finding max/min daily temps
    over the US states
    for the years between 1990 and 2000
    '''
    sql = '''
#standardSQL
SELECT
  (max_table.max-32)*5/9 max_celsius,
  (min_table.min-32)*5/9 min_celsius,
  max_table.state
FROM (
  SELECT
    max,
    state,
    stn
  FROM (
    SELECT
      max,
      year,
      state,
      stn,
      ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
    FROM (
      SELECT
        max,
        year,
        stn,
        wban
      FROM
        `bigquery-public-data.noaa_gsod.gsod*`
      WHERE
        _TABLE_SUFFIX BETWEEN '1990'
        AND '2000') a
    JOIN
      `bigquery-public-data.noaa_gsod.stations` b
    ON
      a.stn=b.usaf
      AND a.wban=b.wban
    WHERE
      state IS NOT NULL
      AND max<1000
      AND country='US' )
  WHERE
    rn=1
  ORDER BY
    YEAR DESC ) max_table
LEFT JOIN (
  SELECT
    min,
    (min-32)*5/9 celsius,
    state,
    stn
  FROM (
    SELECT
      min,
      year,
      state,
      stn,
      ROW_NUMBER() OVER(PARTITION BY state ORDER BY min DESC) rn
    FROM (
      SELECT
        min,
        year,
        stn,
        wban
      FROM
        `bigquery-public-data.noaa_gsod.gsod*`
      WHERE
        _TABLE_SUFFIX BETWEEN '1990'
        AND '2000') a
    JOIN
      `bigquery-public-data.noaa_gsod.stations` b
    ON
      a.stn=b.usaf
      AND a.wban=b.wban
    WHERE
      state IS NOT NULL
      AND min<1000
      AND country='US' )
  WHERE
    rn=1
  ORDER BY
    YEAR DESC ) min_table
ON
  min_table.state = max_table.state'''

    write_disposition = 'WRITE_TRUNCATE'

    # if the export to bg table went through
    if export_to_table(json_key, sql, 'my_data', 'my_table', None,
                       write_disposition):
        # proceed with the export to GCS
        export_to_gcs(json_key, 'my_data', 'my_table',
                      ['gs://testweathernikos/app1.csv'])

    bq_client = connect_client(json_key_file=json_key)
    # Fetch table schema
    table_schema = bq_client.get_table_schema('my_data', 'my_table')
    # table_schema = [{u'type': u'FLOAT', u'name': u'max_celsius', u'mode': u'NULLABLE'}, {u'type': u'FLOAT', u'name': u'min_celsius', u'mode': u'NULLABLE'}, {u'type': u'STRING', u'name': u'state', u'mode': u'NULLABLE'}]
    # statement: CREATE TABLE USING JSON table_schema
    create_table_query = create_sql_from_json_schema(table_schema)
    # TODO Add operations code to test if ./cloudproxy is on.
    # TODO get credentials more securely and with argparse
    user = 'root'
    password =  '97103'
    database = 'demo'
    # Check if table already exists
    mysql_query_result = run_query(user, password, database, create_table_query)
    print "Dropping table"
    if mysql_query_result == 1050:
        run_query(user, password, database, 'DROP TABLE temps')
    # rerun the query
    mysql_query_result = run_query(user, password, database, create_table_query)

    # Call GCS client and download files (BigQuery result)
    gcs_client = GCSClient()
    bucket_name = 'testweathernikos'
    file_names = ['app1.csv']
    for f_n in file_names:
        gcs_client.download_file(bucket_name, f_n)


    # Insert into DB with local data infile
    load_data_query = "LOAD DATA LOCAL INFILE 'app1.csv' INTO TABLE temps CHARACTER SET 'utf8' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' IGNORE 1 LINES ; "
    mysql_load_data_result = run_query(user, password, database, load_data_query)
