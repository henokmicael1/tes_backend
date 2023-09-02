import datetime
import boto3
import pymysql
from query import *
from search import *
from pydantic import ValidationError
from cognito import  signUpDispatcherCognito, deleteDispatcherCognito

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
healthPath = '/dispatcher/health'
searchDispatcherPath = '/dispatcher/search'



def handler(event, context):
    # log.info("Request event : {}".format(event))
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == getMethod and 'dispatcherId' in event['queryStringParameters']:
        response = getDispatcher(conn, event['queryStringParameters']['dispatcherId'],
                                 event['queryStringParameters']['sub'])
    
    elif httpMethod == getMethod and 'limit' in event['queryStringParameters']:
        response = getDispatchers(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                                  event['queryStringParameters']['offset'])
    elif httpMethod == postMethod and path == searchDispatcherPath:
        response = searchDispatcher(conn, event['queryStringParameters']['sub'],
                                    event['queryStringParameters']['limit'],
                                    event['queryStringParameters']['offset'], event['body'])

    elif httpMethod == postMethod:
        try:
            # dispatcher: Dispatcher = parse(event=event['body'], model=Dispatcher)
            response = onboardDispatcher(conn, event['body'])
        except ValidationError as ex:
            # log.error(ex)
            print("Validation error , error = {}".format(ex))
            response = buildResponse(400, "Invalid Input")
    elif httpMethod == patchMethod:
        response = updateDispatcher(conn, event['body'])
    elif httpMethod == deleteMethod:
        response = deleteDispatcher(conn, event['queryStringParameters']['dispatcherId'],
                                    event['queryStringParameters']['sub'])

    # elif httpMethod == putMethod and path == dispatcherPath:
    #     response = modifyDispatcher(conn,NULL)
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


def getDispatcher(connection, dispatcherId, sub):
    cursor = connection.cursor()
    cursor.execute(getDispatcherByIdAndSub, (dispatcherId, sub))
    try:
        if cursor.rowcount != 0:
            dispatcher = formatDispatcherResponse(cursor.fetchone())
            return buildResponse(200, dispatcher)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'DispatcherId: {} not found for sub: {}'.format(dispatcherId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while fetching dispatcher details for dispatcherId:{} and sub : {}".format(dispatcherId,
                                                                                                           sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching dispatcher details for '
                                              'dispatcherId: {} and sub:{}'.format(dispatcherId, sub), 'Error': ex})


def getDispatchers(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getDispatcherBySub, (sub, int(limit), int(offset)))
    dispatchers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                dispatchers.append(formatDispatcherResponse(row))
            dispatchersResponse = {
                "totalDbCount": getDispatchersCount(connection, sub),
                "dispatchers": dispatchers
            }
            return buildResponse(200, dispatchersResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No dispatcher record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all dispatcher details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all dispatcher details for sub : {}'
                             .format(sub)})


def updateDispatcher(connection, bodyDetails):
    cursor = connection.cursor()
    if 'dispatcherId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'dispatcherId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryParameters = buildUpdateQueryParameters(body, cursor)
    print("updateQueryParameters")
    print(updateQueryParameters)
    try:
        cursor.execute(updateQueryParameters['query'], updateQueryParameters['values'])
        connection.commit()
        cursor.execute(getDispatcherByIdAndSub, (body['dispatcherId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'dispatcher': formatDispatcherResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {
                'Message': 'DispatcherId: {} not found for sub: {}'.format(body['dispatcherId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating dispatcher with dispatcherId:{} and sub : {} , Error : {}".format(
            body['dispatcherId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred  while updating dispatcher with '
                                              'dispatcherId: {} and sub:{}'.format(body['dispatcherId'],
                                                                                   body['sub'])})


def buildUpdateQueryParameters(body, cursor):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.dispatcher set '
    values = ()
    dispatcherUpdateAttributes = ['firstName', 'lastName', 'phoneNumber', 'email',
                                  'birthDate', 'role', 'licenseNumber', 'licenseExpDate', 'address1', 'address2',
                                  'state', 'city', 'country', 'zipCode', 'full_name', 'documents']
    for fieldName in dispatcherUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'dispatcherId' and fieldName != 'sub':
            if fieldName == 'documents':
                documents = body[fieldName]
                cursor.execute("UPDATE dispatch_dev.dispatcher SET documents = %s WHERE dispatcher_id=%s AND sub=%s", (json.dumps(documents), body['dispatcherId'], body['sub']))
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (json.dumps(documents),)
            else:
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['dispatcherId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where dispatcher_id = %s and sub = %s',
        "values": values
    }


def deleteDispatcher(connection, dispatcherId, sub):
    print(dispatcherId)
    print(sub)
    cursor = connection.cursor()
    try:
        dispatcherDetails = getDispatcher(connection, dispatcherId, sub)
        body = json.loads(dispatcherDetails['body'])
        deleteDispatcherCognito(body)
        cursor.execute(deleteDispatcherByIdAndSub, ('DELETED', dispatcherId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'DispatcherId: {} not found for sub: {}'.format(dispatcherId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting dispatcher with dispatcherId:{} and sub : {},  Error : {}".format(
                dispatcherId, sub,
                ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting dispatcher with '
                                              'dispatcherId: {} and sub:{}'.format(dispatcherId, sub)})


def onboardDispatcher(connection, bodyDetails):
    print('body')
    print(bodyDetails)
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    fullName = body['firstName'] + ' ' + body['lastName']

    try:
        cursor.execute(insertDispatcher,
                       (body['firstName'], 
                        body['lastName'], 
                        body['phoneNumber'], 
                        body['email'],
                        body['birthDate'],
                        body['role'], 
                        body['licenseNumber'], 
                        body['licenseExpDate'], 
                        body['address1'],
                        body['address2'],
                        body['state'], 
                        body['city'], 
                        body['country'], 
                        body['zipCode'], 
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                        body['dispatcher_type'], 
                        body['commission_percentage'], 
                        body['monthly_salary'],
                        json.dumps(body['documents']),
                        body['sub'],
                        fullName))

        insertedDispatcherId = connection.insert_id()
        signUpResponse = signUpDispatcherCognito(body,insertedDispatcherId)
        print("Inserted dispatcher ID : {}".format(insertedDispatcherId))
        if signUpResponse['statusCode'] == 400:
            connection.rollback()
            connection.close()
            return buildResponse(signUpResponse['statusCode'],signUpResponse['body'])
        
    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving dispatcher')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving dispatcher', 'Error': ex})
    else:
        connection.commit()
        insertedDispatcherDetails = getDispatcher(connection, insertedDispatcherId, body['sub'])
        connection.close()

    return insertedDispatcherDetails










