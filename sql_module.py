import pymysql
pymysql.install_as_MySQLdb()
# Ensure coud_proxy is run
# Connect to the database
#TODO: add DB credentias as args

def run_query(user, password, database, query):
    ''' Runs a pymysql query. Preliminary version

    Args:
        user: string, DB username
        password: string, DB password
        database: string, DB name
        query: string, query to be run
    Returns:
        connection: connect pymysql (error code contained)
    Raises:
    '''
    connection = pymysql.connect(host='localhost',
                                 user=user,
                                 password=password,
                                 db=database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            #sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
            sql = query
            cursor.execute(sql)

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

    finally:
        connection.close()
    return connection


def create_sql_from_json_schema(json_schema):
    ''' SQL CREATE TABLE statement from json_schema
    TODO: Expand and add more attribute datatypes

    Args:
        json_schema: string, forrmatted as JSON schema as f
        etched with dataset.get_table_schema

    Returns:
        sql_create_statement: Str, to be passed to MySQL database engine

    '''
    attributes = []
    sql_create_statement = 'CREATE TABLE `temps` ('
    for j in json_schema:
        datatype = j[u'type']
        name = j['name']
        is_null = True if j['mode'] == 'NULLABLE' else False
        if datatype == 'STRING':
            datatype = 'VARCHAR(10)'
        elif datatype == 'INTEGER':
            datatype = 'INTEGER'
        elif datatype == 'FLOAT':
            datatype = 'DOUBLE'
        elif datatype == 'DATE':
            datatype = 'DATE'
        else:
            datatype = 'TEXT'

        attribute_declaration = ' `%s` %s %s' % (name, datatype, '' if is_null else 'NOT NULL')
        attributes.append(attribute_declaration)
    sql_create_statement += (',').join(attributes)
    sql_create_statement += ') DEFAULT CHARSET=utf8 ;'
    return sql_create_statement
