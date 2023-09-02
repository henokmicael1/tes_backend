import json
import boto3
import pymysql
from query import *
import re
from trailer import Trailer
import datetime
from pydantic import ValidationError

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/trailer/health'
searchTrailerPath = '/trailer/search'


def handler(event, context):
    print('received event:')
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == postMethod and path == searchTrailerPath:
        response = searchTrailer(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                                 event['queryStringParameters']['offset'], event['body'])
    elif httpMethod == getMethod and 'trailerId' in event['queryStringParameters']:
        response = getTrailer(conn, event['queryStringParameters']['trailerId'], event['queryStringParameters']['sub'])
    elif httpMethod == getMethod and 'limit' in event['queryStringParameters']:
        response = getTrailers(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                               event['queryStringParameters']['offset'])
    elif httpMethod == postMethod:
        try:
            trailer = Trailer(**json.loads(event['body']))
            response = onboardTrailer(conn, event['body'])
        except ValidationError as ex:
            # log.error(ex)
            print("Validation error , error = {}".format(ex))
            response = buildResponse(400, "Invalid Input")
    elif httpMethod == patchMethod:
        response = updateTrailer(conn, event['body'])
    elif httpMethod == deleteMethod:
        response = deleteTrailer(conn, event['queryStringParameters']['trailerId'],
                                 event['queryStringParameters']['sub'])

    return response


def getTrailer(connection, trailerId, sub):
    cursor = connection.cursor()
    cursor.execute(getTrailerById, (trailerId, sub))
    try:
        if cursor.rowcount != 0:
            trailer = formatTrailerResponse(cursor.fetchone())

            cursor.execute(getVehicleById, trailer['vehicleFk'])
            trailer['vehicle'] = formatVehicleResponse(cursor.fetchone())
            del trailer['vehicleFk']

            return buildResponse(200, trailer)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'trailerId: {} not found for sub: {}'.format(trailerId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while fetching trailer details for trailerId:{} and sub : {}".format(trailerId, sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching trailer details for '
                                              'trailerId: {} and sub:{}'.format(trailerId, sub), 'Error': ex})


def getTrailers(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getTrailersBySub, (sub, int(limit), (int(limit) * int(offset))))
    trailers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                trailers.append(formatTrailerResponse(row))
            trailersResponse = {
                "totalDbCount": getTrailersCount(connection, sub),
                "trailers": trailers
            }
            return buildResponse(200, trailersResponse)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No trailer record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all trailer details for sub: {}".format(sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all trailers details for sub : {}'
                             .format(sub), 'Error': ex})


def searchTrailer(connection, sub, limit, offset, bodyDetails):
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    searchQueryParameters = buildSearchQuery(body['searchCriteria'])
    print("searchQueryParameters : {}".format(searchQueryParameters))
    finalValues = searchQueryParameters['values'] + (sub, 'ACTIVE', int(limit),(int(limit) * int(offset)))
    print("finalValues : {}".format(finalValues))

    cursor.execute(searchQueryParameters['query'], finalValues)
    trailers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                trailers.append(formatTrailerSearchResponse(row))
                count = row['count']
            trailersResponse = {
                "totalDbCount": count,
                "trailers": trailers
            }
            return buildResponse(200, trailersResponse)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No trailer record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all trailer details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all trailer details for sub : {}'
                             .format(sub)})


def buildSearchQuery(body):
    print(body)
    keyValueList = []
    baseQuery = 'SELECT trailer_id,trailer_number,license_plate_number,trailer_type,vin,sub, count(*) OVER() AS count FROM ' \
                'dispatch_dev.trailer '
    values = ()
    trailerSearchAttributes = ['trailerNumber', 'licensePlateNumber', 'trailerType', 'vin']
    for fieldName in trailerSearchAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'sub':
            keyValueList.append(camel2snake(fieldName) + ' like %s and ')
            values = values + ('%' + str(body[fieldName]) + '%',)
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + 'where ' + ' '.join(
            keyValueList) + 'sub = %s and status =  %s order by trailer_id limit '
                            '%s offset %s',
        "values": values
    }


def updateTrailer(connection, bodyDetails):
    cursor = connection.cursor()
    if 'trailerId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'trailerId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryParameters = buildUpdateQueryParameters(body, cursor)
    print("updateQueryParameters")
    print(updateQueryParameters)
    try:
        cursor.execute(updateQueryParameters['query'], updateQueryParameters['values'])
        connection.commit()
        cursor.execute(getTrailerById, (body['trailerId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'trailer': formatTrailerResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(404, {
                'Message': 'TrailerId: {} not found for sub: {}'.format(body['TrailerId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating trailer with trailerId:{} and sub : {} , Error : {}".format(
            body['trailerId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred  while updating trailer with '
                                              'trailerId: {} and sub:{}'.format(body['trailerId'],
                                                                                body['sub'])})


def buildUpdateQueryParameters(body, cursor):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.trailer set '
    values = ()
    trailerUpdateAttributes = ['trailerRegExp', 'licensePlateNumber', 'fedAnnualInspectionExp',
                               'stateAnnualInspectionExp', 'vehicleFk', 'status', 'solo', 'trailerType',
                               'insureStartDt', 'insureEndDt', 'vin', 'documents']
    for fieldName in trailerUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'trailerId' and fieldName != 'sub':
            if fieldName == 'documents':
                documents = body[fieldName]
                cursor.execute("UPDATE dispatch_dev.trailer SET documents = %s WHERE trailer_id=%s AND sub=%s", (json.dumps(documents), body['trailerId'], body['sub']))
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (json.dumps(documents),)
            else:
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['trailerId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where trailer_id = %s and sub = %s',
        "values": values
    }


def getTrailersCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getTrailerCountBySub, sub)
    return cursor.fetchone()['count']


def onboardTrailer(connection, bodyDetails):
    body = json.loads(bodyDetails)
    print("onboardTrailer body".format(body))
    cursor = connection.cursor()

    try:
        cursor.execute(insertTrailer,
                       (body['trailerRegExp'], body['licensePlateNumber'], body['fedAnnualInspectionExp'],
                        body['stateAnnualInspectionExp'],
                        body['vehicleFk'],
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                        body['sub'], body['trailerType'], body['insureStartDt'], body['insureEndDt'], body['vin'], json.dumps(body['documents'])))
        insertedTrailerId = connection.insert_id()
        print("Inserted trailer ID : {}".format(insertedTrailerId))
    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving trailer')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving trailer', 'Error': ex})
    else:
        connection.commit()
        insertedTrailerDetails = getTrailer(connection, insertedTrailerId, body['sub'])
        connection.close()

    return insertedTrailerDetails


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


def deleteTrailer(connection, trailerId, sub):
    print(trailerId)
    print(sub)
    cursor = connection.cursor()
    try:
        cursor.execute(deleteTrailerByIdAndSub, ('DELETED', trailerId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'TrailerId: {} not found for sub: {}'.format(trailerId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting trailer with trailerId:{} and sub : {},  Error : {}".format(trailerId,
                                                                                                           sub,
                                                                                                           ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting trailer with '
                                              'trailerId: {} and sub:{}'.format(trailerId, sub)})


def formatTrailerResponse(row):
    response = {'trailerId': row['trailer_id'], 
                'trailerNumber': row['trailer_number'],
                'trailerRegExp': str(row['trailer_reg_exp']), 
                'licensePlateNumber': str(row['license_plate_number']),
                'fedAnnualInspectionExp': str(row['fed_annual_inspection_exp']), 
                'stateAnnualInspectionExp': str(row['state_annual_inspection_exp']),
                'vehicleFk': row['vehicle_fk'], 
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']),
                'status': row['status'], 
                'sub': row['sub'], 
                'trailerType': row['trailer_type'], 
                'insureStartDt': str(row['insure_start_dt']),
                'insureEndDt': str(row['insure_end_dt']), 
                'vin': row['vin'],
                'documents': json.loads(row['documents']) if row['documents'] else []
                }
    # response[‘event’] = event
    return response


def formatTrailerSearchResponse(row):
    response = {'trailerId': row['trailer_id'], 
                'trailerNumber': row['trailer_number'],
                'licensePlateNumber': str(row['license_plate_number']), 
                'trailerType': str(row['trailer_type']),
                'vin': str(row['vin'])}
    return response


def formatVehicleResponse(row):
    response = {'vehicleId': row['vehicle_id'], 
                'make': str(row['make']), 
                'model': str(row['model']), 
                'year': str(row['year'])}
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
