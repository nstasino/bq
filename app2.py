#!/usr/bin/env python
import os
import gzip
import csv
from sql_module import *
from gcs_module import *

if __name__ == '__main__':
    json_key = 'My First Project-8a317759be48.json'
    # Call GCS client and download files (lm-training)
    gcs_client = GCSClient()
    bucket_name = 'im-training'
    # I believe my access was revoked..
    # file_names = ['cities.gz', 'countries.gzip', 'regions.csv']
    # for f_n in file_names:
    #     filename, file_extension = os.path.splitext(f_n)
    #     gcs_client.download_file(bucket_name, f_n)

    with gzip.open('cities.gz','r') as f:
        with open('city.csv', 'wb') as csv_file:
            writer = csv.writer(csv_file)
            for line in f:
                writer.writerow(eval(line).values())
    f.close()

    f = gzip.open('countries.gzip', 'rb')
    file_content = f.read()
    # print file_content
    with open('countries.csv', 'wb') as outfile:
        outfile.write(file_content)
    f.close()


    sql1 = '''CREATE TABLE `cities` (
        city_id INTEGER,
        country_id INTEGER,
        region_id INTEGER,
        name VARCHAR(10),
        iso_code VARCHAR(10)
        ) DEFAULT CHARSET=utf8;'''

    sql2 = '''CREATE TABLE `countries` (
        country_id INTEGER,
        alpha2 VARCHAR(10),
        alpha3 VARCHAR(10),
        name VARCHAR(10),
        targetable INTEGER
        ) DEFAULT CHARSET=utf8;'''


    sql3 = '''CREATE TABLE `regions` (
        region_id INTEGER,
        country_id  INTEGER,
        name VARCHAR(10),
        iso_code VARCHAR(10)
        ) DEFAULT CHARSET=utf8;'''

    # CREATE TABLES
    # Check if table already exists
    mysql_query_result = run_query(user, password, database, sql1)
    # print "Dropping table"
    if mysql_query_result == 1050:
        run_query(user, password, database, 'DROP TABLE cities')
    # rerun the query
    mysql_query_result = run_query(user, password, database, sql1)

    # Check if table already exists
    mysql_query_result = run_query(user, password, database, sql2)
    # print "Dropping table"
    if mysql_query_result == 1050:
        run_query(user, password, database, 'DROP TABLE countries')
    # rerun the query
    mysql_query_result = run_query(user, password, database, sql2)

    # Check if table already exists
    mysql_query_result = run_query(user, password, database, sql3)
    # print "Dropping table"
    if mysql_query_result == 1050:
        run_query(user, password, database, 'DROP TABLE regions')
    # rerun the query
    mysql_query_result = run_query(user, password, database, sql3)


   # Insert into DB with local data infile
    load_data_query = "LOAD DATA LOCAL INFILE 'city.csv' INTO TABLE cities CHARACTER SET 'utf8' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' IGNORE 1 LINES ; "
    mysql_load_data_result = run_query(user, password, database, load_data_query)

    load_data_query = "LOAD DATA LOCAL INFILE 'countries.csv' INTO TABLE countries CHARACTER SET 'utf8' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' IGNORE 1 LINES ; "
    mysql_load_data_result = run_query(user, password, database, load_data_query)

    load_data_query = "LOAD DATA LOCAL INFILE 'regions.csv' INTO TABLE regions CHARACTER SET 'utf8' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' IGNORE 1 LINES ; "
    mysql_load_data_result = run_query(user, password, database, load_data_query)


    # SQL query to join tables is in sql2.sql
    join_sql = '''select
        cities.id,
        cities.name as city_name,
        cities.iso_code as city_iso_code,
        regions.name as region_name,
        regions.iso_code as region_iso_code,
        countries.name as country_name,
        countries.alpha2 as country_alpha2,
        countries.alpha3 as country_alpha3,
    from
        countries,
        regions,
        cities
    where
        cities.country_id = countries.id
        and cities.region_id = regions.id ;'''

