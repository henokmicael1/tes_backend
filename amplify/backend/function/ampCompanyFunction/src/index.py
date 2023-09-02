import json
import boto3
import pymysql
import re

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])

GET_METHOD = 'GET'
POST_METHOD = 'POST'
PATCH_METHOD = 'PATCH'

HEALTH_PATH = '/company/health'
COMPANY_PATH = '/company'
FACTOR_PATH = '/company/factor'


def handler(event, context):
    HTTP_METHOD = event['httpMethod']
    PATH = event['path']
    conn = database_connect()

    if HTTP_METHOD == GET_METHOD and PATH == HEALTH_PATH:
        response = buildResponse(200, "SUCCESS")
    elif HTTP_METHOD == GET_METHOD and PATH == COMPANY_PATH:
        sub = event['queryStringParameters'].get('sub')
        if sub:
            response = getCompany(conn, sub)
        else:
            response = buildResponse(400, "Missing sub in query parameters")
    elif HTTP_METHOD == PATCH_METHOD and PATH == COMPANY_PATH:
        response = updateCompany(conn, event['body'])
    elif HTTP_METHOD == GET_METHOD and PATH == FACTOR_PATH:
        sub = event['queryStringParameters'].get('sub')
        if sub:
            response = getFactor(conn, sub)
        else:
            response = buildResponse(400, "Missing sub in query parameters")
    elif HTTP_METHOD == PATCH_METHOD and PATH == FACTOR_PATH:
        response = updateFactor(conn, event['body'])
    return response


def getCompany(conn, sub):
    try:
        # Execute a SELECT query to retrieve the company based on the provided sub
        with conn.cursor() as cursor:
            sql = "SELECT * FROM company WHERE sub = %s"
            cursor.execute(sql, (sub,))
            result = cursor.fetchone()
            if result:
                converted_result = {
                    'name': str(result['name']) if result['name'] is not None else '',
                    'phoneNumber': str(result['phone_number']) if result['phone_number'] is not None else '',
                    'dotNumber': str(result['dot_number']) if result['dot_number'] is not None else '',
                    'mcNumber': str(result['mc_number']) if result['mc_number'] is not None else '',
                    'insurance': float(result['insurance']) if result['insurance'] is not None else '',
                    'commission': float(result['commission']) if result['commission'] is not None else '',
                    'fax': str(result['fax']) if result['fax'] is not None else '',
                    'address1': str(result['address1']) if result['address1'] is not None else '',
                    'address2': str(result['address2']) if result['address2'] is not None else '',
                    'state': str(result['state']) if result['state'] is not None else '',
                    'city': str(result['city']) if result['city'] is not None else '',
                    'country': str(result['country']) if result['country'] is not None else '',
                    'zipCode': str(result['zip_code']) if result['zip_code'] is not None else '',
                    'sub': str(result['sub']) if result['sub'] is not None else '',
                    'tsCreated': str(result['ts_created']) if result['ts_created'] is not None else '',
                    'tsUpdated': str(result['ts_updated']) if result['ts_updated'] is not None else '',
                    'email': str(result['email']) if result['email'] is not None else '',
                }
                response = buildResponse(200, converted_result)
            else:
                response = buildResponse(404, "company not found")
    except pymysql.Error as e:
        response = buildResponse(500, str(e))
    cursor.close()
    return response


def getFactor(conn, sub):
    try:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM factor WHERE sub = %s"
            cursor.execute(sql, (sub,))
            result = cursor.fetchone()
            if result:
                converted_result = {
                    'name': str(result['name']) if result['name'] is not None else "",
                    'address1': str(result['address1']) if result['address1'] is not None else "",
                    'address2': str(result['address2']) if result['address2'] is not None else "",
                    'state': str(result['state']) if result['state'] is not None else "",
                    'city': str(result['city']) if result['city'] is not None else "",
                    'country': str(result['country']) if result['country'] is not None else "",
                    'phoneNumber': str(result['phone_number']) if result['phone_number'] is not None else "",
                    'email': str(result['email']) if result['email'] is not None else "",
                    'zipCode': str(result['zip_code']) if result['zip_code'] is not None else "",
                    'tsCreated': str(result['ts_created']) if result['ts_created'] is not None else "",
                    'tsUpdated': str(result['ts_updated']) if result['ts_updated'] is not None else "",
                    'sub': str(result['sub']) if result['sub'] is not None else ""
                }
                response = buildResponse(200, converted_result)
            else:
                response = buildResponse(404, "factor not found")
    except pymysql.Error as e:
        response = buildResponse(500, str(e))
    return response


def updateCompany(conn, body):
    try:
        data = json.loads(body)
        sub = data.get('sub')
        if sub:
            with conn.cursor() as cursor:
                # Check if the company with the provided sub exists
                sql = "SELECT * FROM company WHERE sub = %s"
                cursor.execute(sql, (sub,))
                result = cursor.fetchone()
                if result:
                    # Update the company based on the provided data
                    update_fields = []
                    params = []
                    for key, value in data.items():
                        if key not in ['sub', 'tsCreated']:
                            snake_key = camel2snake(key)
                            update_fields.append(f"{snake_key} = %s")
                            params.append(value)
                    if update_fields:
                        params.append(sub)
                        sql = f"UPDATE company SET {', '.join(update_fields)}, ts_updated = CURRENT_TIMESTAMP WHERE sub = %s"
                        cursor.execute(sql, params)
                        conn.commit()
                        response = buildResponse(200, "Company updated successfully")
                    else:
                        response = buildResponse(400, "No fields to update")
                else:
                    response = buildResponse(404, "Company not found")
        else:
            response = buildResponse(400, "Missing 'sub' field in request body")
    except (pymysql.Error, json.JSONDecodeError) as e:
        response = buildResponse(500, str(e))
    return response


def updateFactor(conn, body):
    try:
        data = json.loads(body)
        sub = data.get('sub')
        with conn.cursor() as cursor:
            sql = "SELECT * FROM factor WHERE sub = %s"
            cursor.execute(sql, (sub,))
            result = cursor.fetchone()
            if result:
                update_fields = []
                params = []
                for key, value in data.items():
                    if key not in ['sub', 'tsCreated']:
                        snake_key = camel2snake(key)
                        update_fields.append(f"{snake_key} = %s")
                        params.append(value)
                if update_fields:
                    params.append(sub)
                    sql = f"UPDATE factor SET {', '.join(update_fields)}, ts_updated = CURRENT_TIMESTAMP WHERE sub = %s"
                    cursor.execute(sql, params)
                    conn.commit()
                    response = buildResponse(200, "factor updated successfully")
                else:
                    response = buildResponse(400, "No fields to update")
            else:
                response = buildResponse(404, "factor not found")
    except (pymysql.Error, json.JSONDecodeError) as e:
        response = buildResponse(500, str(e))
    return response


def database_connect():
    print('database_connect')
    rds_host = db_secret_dict['host']
    name = db_secret_dict['username']
    password = db_secret_dict['password']
    db_name = db_secret_dict['dbName']
    conn = {}

    try:
        conn = pymysql.connect(user=name,
                               password=password,
                               host=rds_host,
                               database=db_name,
                               cursorclass=pymysql.cursors.DictCursor)

        print("SUCCESS: Successfully connected to MySQL instance.")
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    return conn


def camel2snake(name):
    return re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name)


def snake2camel(name):
    return re.sub(r'_([a-z])', lambda x: x.group(1).upper(), name)


def buildResponse(statusCode, body=None):
    # log.info("buildResponse method called with statusCode : {} and body : {}".format(statusCode,body))
    response = {
        'statusCode': statusCode,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body)
    return response
