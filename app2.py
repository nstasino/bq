#!/usr/bin/env python
from sql_module import *
from gcs_module import *

if __name__ == '__main__':
    json_key = 'My First Project-8a317759be48.json'
    gcs_client = connect_gcs_client(json_key)
    try:
        bucket = gcs_client.get_bucket('im-training')
    except google.cloud.exceptions.NotFound:
        print('Sorry, that bucket does not exist!')
    # Create a blob object from the filepath
    cities_blob = bucket.blob("im-training/cities.gz")
    countries_blob = bucket.blob("im-training/countries.gzip")
    regions_blob = bucket.blob("im-training/regions.csv")
    # Download the file to a destination
    cities_blob.download_to_filename("im-training/cities.gz")
    countries_blob.download_to_filename("im-training/countries.gzip")
    regions_blob.download_to_filename("im-training/regions.csv")

    # TODO: CREATE TABLE FROM UNZIPPED FILES

    # TODO: CALL MYSQL with CREATE TABLE for all three tables

    # TODO: SQL QUERY: LOAD DATA LOCAL INFILE with appropriate zipping

    # TODO: SQL query to join tables is in sql2.sql
