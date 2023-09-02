import json
import boto3
import pymysql
import stripe

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(SecretId='dev/dispatch/app')
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

GET_METHOD = 'GET'
POST_METHOD = 'POST'
PATCH_METHOD = 'PATCH'

HEALTH_PATH = '/subscription/health'


def handler(event, context):
    print('received event:')
    print(event)

    HTTP_METHOD = event['httpMethod']
    PATH = event['path']

    if HTTP_METHOD == GET_METHOD:
        if PATH == HEALTH_PATH:
            response = build_response(200, "SUCCESS")
        else:
            response = build_response(404, "Not Found")
    elif HTTP_METHOD == POST_METHOD:
        if PATH == '/subscription/customerPortal':
            conn = database_connect()
            response = get_customer_portal(conn, event['body'])
            conn.close()
        else:
            response = build_response(404, "Not Found")
    else:
        response = build_response(405, "Method Not Allowed")

    return response


def get_customer_portal(conn, request_body):
    try:
        data = json.loads(request_body)
        sub = data.get('sub')
        return_url = data.get('return_url')

        if sub and return_url:
            with conn.cursor() as cursor:
                sql = "SELECT customer_id FROM stripe_product_customer WHERE sub = %s"
                cursor.execute(sql, (sub,))
                result = cursor.fetchone()

                if result:
                    customer_id = result['customer_id']
                    session = create_billing_portal_session(customer_id, return_url)
                    return build_response(200, {'url': session.url})
                else:
                    return build_response(404, "Customer not found")
        else:
            return build_response(400, "Missing 'sub' or 'return_url' in request body")
    except Exception as e:
        return build_response(500, str(e))


def create_billing_portal_session(customer_id, return_url):
    stripe.api_key = db_secret_dict['stripeSecretKey']
    session = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=return_url
    )
    return session


def database_connect():
    print('database_connect')
    rds_host = db_secret_dict['host']
    name = db_secret_dict['username']
    password = db_secret_dict['password']
    db_name = db_secret_dict['dbName']

    try:
        conn = pymysql.connect(user=name, password=password,
                               host=rds_host, database=db_name, cursorclass=pymysql.cursors.DictCursor)
        print("SUCCESS: Successfully connected to MySQL instance.")
        return conn
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)


def build_response(status_code, body=None):
    response = {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body)
    return response
