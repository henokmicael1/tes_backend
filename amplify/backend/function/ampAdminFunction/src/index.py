import json
import boto3
import pymysql
from cognito import updateUserInCognito, getUserDetailsFromCognito

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(SecretId='dev/dispatch/app')
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

GET_METHOD = 'GET'
POST_METHOD = 'POST'
PATCH_METHOD = 'PATCH'

HEALTH_PATH = '/admin/health'


def handler(event, context):
    print('received event:')
    print(event)
    conn = database_connect()

    if 'triggerSource' in event:
        # Cognito trigger
        if event['triggerSource'] == 'PostConfirmation_ConfirmSignUp':
            if event['request']['userAttributes'].get('custom:role') == 'Admin':
                sub = event['request']['userAttributes']['sub']
                phone_number = event['request']['userAttributes']['phone_number']
                email = event['request']['userAttributes']['email']
                saveCompany(conn, sub, phone_number, email)
                saveFactor(conn, sub)
        return event

    HTTP_METHOD = event['httpMethod']
    PATH = event['path']

    if HTTP_METHOD == GET_METHOD and PATH == HEALTH_PATH:
        response = buildResponse(200, "SUCCESS")
    elif HTTP_METHOD == GET_METHOD:
        sub = event['queryStringParameters'].get('sub')
        if sub:
            response = getUserDetailsFromCognito(sub)
        else:
            response = buildResponse(400, "Missing admin_id or sub in query parameters")
    elif HTTP_METHOD == PATCH_METHOD:
        sub = json.loads(event['body'])['sub']
        response = updateUserInCognito(admin_id=sub, **json.loads(event['body']))
    else:
        response = buildResponse(404, 'Path Not Found')

    return response


def saveCompany(conn, sub, phone_number, email):
    cursor = conn.cursor()

    try:
        # Prepare the SQL statement
        sql = "INSERT INTO company (sub, phone_number, email) VALUES (%s, %s, %s)"

        # Execute the SQL statement
        cursor.execute(sql, (sub,phone_number,email))
        conn.commit()

        print("SUCCESS: Company record created successfully")
    except Exception as e:
        print("ERROR: Failed to save company record:", str(e))
        conn.rollback()

    cursor.close()


def saveFactor(conn, sub):
    cursor = conn.cursor()

    try:
        # Prepare the SQL statement
        sql = "INSERT INTO factor (sub) VALUES (%s)"

        # Execute the SQL statement
        cursor.execute(sql, (sub,))
        conn.commit()

        print("SUCCESS: Factor record created successfully")
    except Exception as e:
        print("ERROR: Failed to save factor record:", str(e))
        conn.rollback()

    cursor.close()


def database_connect():
    print('database_connect')
    rds_host = db_secret_dict['host']
    name = db_secret_dict['username']
    password = db_secret_dict['password']
    db_name = db_secret_dict['dbName']
    conn = {}

    try:
        conn = pymysql.connect(user=name, password=password,
                               host=rds_host, database=db_name, cursorclass=pymysql.cursors.DictCursor)
        print("SUCCESS: Successfully connected to MySQL instance.")
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    return conn


def buildResponse(statusCode, body=None):
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
