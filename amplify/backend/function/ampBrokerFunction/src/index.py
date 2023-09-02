import datetime
import json
import boto3
import pymysql
from broker import Broker
from query import *
import re
from pydantic import ValidationError

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

getMethod = 'GET'
postMethod = 'POST'
putMethod = 'PUT'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/broker/health'


def handler(event, context):
    # log.info("Request event : {}".format(event))
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == getMethod and 'brokerId' in event['queryStringParameters']:
        response = getBroker(conn, event['queryStringParameters']['brokerId'],
                             event['queryStringParameters']['sub'])
    elif httpMethod == getMethod and 'limit' in event['queryStringParameters']:
        response = getBrokers(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                              event['queryStringParameters']['offset'])
    elif httpMethod == postMethod:
        try:
            broker = Broker(**json.loads(event['body']))
            response = onboardBroker(conn, event['body'])
        except ValidationError as ex:
            # log.error(ex)
            print("Validation error , error = {}".format(ex))
            response = buildResponse(400, "Invalid Input")
    elif httpMethod == patchMethod:
        response = updateBroker(conn, event['body'])
    elif httpMethod == deleteMethod:
        response = deleteBroker(conn, event['queryStringParameters']['brokerId'],
                                event['queryStringParameters']['sub'])

    # elif httpMethod == putMethod and path == brokerPath:
    #     response = modifyBroker(conn,NULL)
    else:
        buildResponse(404, 'Path Not Found')
    return response


def database_connect():
    print('database_connect')
    rds_host = db_secret_dict['host']
    name = db_secret_dict['username']
    password = db_secret_dict['password']
    db_name = db_secret_dict['dbName']
    conn = {}

    try:
        conn = pymysql.connect(user=name, password=password, host=rds_host, database=db_name)
        print("SUCCESS: Successfully connected to MySQL instance.")
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    return conn


def getBroker(connection, brokerId, sub):
    cursor = connection.cursor()
    cursor.execute(getBrokerByIdAndSub, (brokerId, sub))
    try:
        if cursor.rowcount != 0:
            broker = formatBrokerResponse(cursor.fetchone())
            return buildResponse(200, broker)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'BrokerId: {} not found for sub: {}'.format(brokerId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while fetching broker details for brokerId:{} and sub : {}".format(brokerId,
                                                                                                   sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching broker details for '
                                              'brokerId: {} and sub:{}'.format(brokerId, sub), 'Error': ex})


def getBrokers(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getBrokerBySub, (sub, int(limit), int(offset)))
    brokers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                brokers.append(formatBrokerResponse(row))
            brokersResponse = {
                "totalDbCount": getBrokersCount(connection, sub),
                "brokers": brokers
            }
            return buildResponse(200, brokersResponse)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No broker record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all broker details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all broker details for sub : {}'
                             .format(sub)})


def getBrokersCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getBrokerCountBySub, sub)
    return cursor.fetchone()[0]


def updateBroker(connection, bodyDetails):
    cursor = connection.cursor()
    if 'brokerId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'brokerId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryParameters = buildUpdateQueryParameters(body)
    print("updateQueryParameters")
    print(updateQueryParameters)
    try:
        cursor.execute(updateQueryParameters['query'], updateQueryParameters['values'])
        connection.commit()
        cursor.execute(getBrokerByIdAndSub, (body['brokerId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'broker': formatBrokerResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(404, {
                'Message': 'BrokerId: {} not found for sub: {}'.format(body['brokerId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating broker with brokerId:{} and sub : {} , Error : {}".format(
            body['brokerId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred  while updating broker with '
                                              'brokerId: {} and sub:{}'.format(body['brokerId'],
                                                                               body['sub'])})


def buildUpdateQueryParameters(body):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.broker set '
    values = ()

    brokerUpdateAttributes = ["name", "phoneNumber", "email", "birthDate",
                              "address1", "address2", "state", "city", "country",
                              "zipCode", "status"]
    for fieldName in brokerUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'brokerId' and fieldName != 'sub':
            keyValueList.append(camel2snake(fieldName) + '= %s')
            values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['brokerId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where broker_id = %s and sub = %s',
        "values": values
    }


def deleteBroker(connection, brokerId, sub):
    print(brokerId)
    print(sub)
    cursor = connection.cursor()
    try:
        cursor.execute(deleteBrokerByIdAndSub, ('DELETED', brokerId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'BrokerId: {} not found for sub: {}'.format(brokerId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting broker with brokerId:{} and sub : {},  Error : {}".format(
                brokerId, sub,
                ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting broker with '
                                              'brokerId: {} and sub:{}'.format(brokerId, sub)})


def onboardBroker(connection, bodyDetails):
    print('body')
    print(bodyDetails)
    body = json.loads(bodyDetails)
    cursor = connection.cursor()

    try:
        cursor.execute(insertBroker,
                       (body['name'], body['phoneNumber'], body['email'],
                        body['address1'], body['address2'],
                        body['state'],
                        body['city'], body['country'], body['zipCode'], datetime.datetime.now(),
                        datetime.datetime.now(),
                        body['status'], body['sub']))
        insertedBrokerId = connection.insert_id()
        print("Inserted broker ID : {}".format(insertedBrokerId))
    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving broker')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving broker', 'Error': ex})
    else:
        connection.commit()
        insertedBrokerDetails = getBroker(connection, insertedBrokerId, body['sub'])
        connection.close()

    return insertedBrokerDetails


def formatBrokerResponse(row):
    response = {'brokerId': row[0], 'name': str(row[1]), 'phoneNumber': str(row[2]), 'email': str(row[3]),
                'address1': str(row[4]), 'address2': str(row[5]), 'state': str(row[6]),
                'city': str(row[7]), 'country': str(row[8]), 'zipCode': str(row[9]),
                'tsCreated': str(row[10]), 'tsUpdated': str(row[11]), 'status': str(row[12]),
                'sub': str(row[13])}
    # response[‘event’] = event
    return response


def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])


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
