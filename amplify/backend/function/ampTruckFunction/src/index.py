import json
import boto3
import pymysql
from query import *
import re
from truck import Truck
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
healthPath = '/truck/health'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
truckTableSearch = '/truck/tableViewSearch'


def handler(event, context):
    print('received event:')
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == postMethod and path == truckTableSearch:
        response = trucksTableViewSearch(conn, event['queryStringParameters']['sub'],
                                         event['queryStringParameters']['limit'],
                                         event['queryStringParameters']['offset'], event['body'])
    elif httpMethod == getMethod and 'truckId' in event['queryStringParameters']:
        response = getTruck(conn, event['queryStringParameters']['truckId'], event['queryStringParameters']['sub'])
    elif httpMethod == getMethod and 'limit' in event['queryStringParameters']:
        response = getTrucks(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                             event['queryStringParameters']['offset'])
    elif httpMethod == postMethod:
        try:
            truck = Truck(**json.loads(event['body']))
            response = onboardTruck(conn, event['body'])
        except ValidationError as ex:
            # log.error(ex)
            print("Validation error , error = {}".format(ex))
            response = buildResponse(400, "Invalid Input")
    elif httpMethod == patchMethod:
        response = updateTruck(conn, event['body'])
    elif httpMethod == deleteMethod:
        response = deleteTruck(conn, event['queryStringParameters']['truckId'], event['queryStringParameters']['sub'])

    return response


def getTruck(connection, truckId, sub):
    cursor = connection.cursor()
    cursor.execute(getTruckById, (truckId, sub))
    try:
        if cursor.rowcount != 0:
            truck = formatTruckResponse(cursor.fetchone())
            print("Driver 1 and sub".format(truck['driver1Fk'], truck['sub']))
            drivers = []
            
            
            
            if truck['driver1Fk'] is not None:
                cursor.execute(getDriverByIdAndSub, (truck['driver1Fk'], truck['sub']))
                drivers.append(formatDriverResponse(cursor.fetchone(), "primary"))
                print("driver1Fk".format(truck['driver1Fk']))
            
            print("driver2Fk".format(truck['driver2Fk']))
            if truck['driver2Fk'] is not None:
                cursor.execute(getDriverByIdAndSub, (truck['driver2Fk'], truck['sub']))
                drivers.append(formatDriverResponse(cursor.fetchone(), "secondary"))
                print("driver2Fk".format(truck['driver2Fk']))

            truck['drivers'] = drivers

            if truck['trailerFk'] is not None:
                cursor.execute(getTrailerById, (truck['trailerFk'], truck['sub']))
                truck['trailer'] = formatTrailerResponse(cursor.fetchone())

            cursor.execute(getVehicleById, truck['vehicleFk'])
            truck['vehicle'] = formatVehicleResponse(cursor.fetchone())

            del truck['driver1Fk']
            del truck['driver2Fk']
            del truck['trailerFk']
            del truck['vehicleFk']

            return buildResponse(200, truck)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'truckId: {} not found for sub: {}'.format(truckId, sub)})
    except Exception as ex:
        print("Exception occurred while fetching truck details for truckId:{} and sub : {}".format(truckId, sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching truck details for '
                                              'truckId: {} and sub:{}'.format(truckId, sub), 'Error': ex})


def getTrucks(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getTrucksBySub, (sub, int(limit), int(limit) *  int(offset)))
    trucks = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                trucks.append(formatTruckResponse(row))
            trucksResponse = {
                "totalDbCount": getTrucksCount(connection, sub),
                "trucks": trucks
            }
            return buildResponse(200, trucksResponse)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No truck record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all truck details for sub: {}".format(sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all trucks details for sub : {}'
                             .format(sub), 'Error': ex})


def trucksTableViewSearch(connection, sub, limit, offset, bodyDetails):
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    searchCriteria = body['searchCriteria']
    print(searchCriteria)
    values = (
        '%' + str(searchCriteria['truckNumber']) + '%',
        '%' + searchCriteria['truckPlateNumber'] + '%',
        '%' + searchCriteria['primaryDriverName'] + '%',
        '%' + searchCriteria['trailerPlateNumber'] + '%',
        '%' + searchCriteria['solo'] + '%',
        sub,
        int(limit),
        (int(limit) * int(offset)))
    print(values)
    cursor.execute(getTrucksBySubView, values)
    trucks = []
    count = 0
    try:
        print(cursor.rowcount)
        if cursor.rowcount != 0:
            for row in cursor:
                trucks.append(formatTruckViewResponse(row))
                count = row['count']
            trucksResponse = {
                "totalDbCount": count,
                "trucks": trucks
            }
            return buildResponse(200, trucksResponse)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No truck record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all truck details for sub: {}".format(sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all trucks details for sub : {}'
                             .format(sub), 'Error': ex})


def updateTruck(connection, bodyDetails):
    cursor = connection.cursor()
    if 'truckId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'truckId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryParameters = buildUpdateQueryParameters(body, cursor)
    print("updateQueryParameters")
    print(updateQueryParameters)
    try:
        cursor.execute(updateQueryParameters['query'], updateQueryParameters['values'])
        connection.commit()
        cursor.execute(getTruckById, (body['truckId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'truck': formatTruckResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(404, {
                'Message': 'TruckId: {} not found for sub: {}'.format(body['TruckId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating truck with truckId:{} and sub : {} , Error : {}".format(
            body['truckId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred  while updating truck with '
                                              'truckId: {} and sub:{}'.format(body['truckId'],
                                                                              body['sub'])})


def buildUpdateQueryParameters(body, cursor):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.truck set '
    values = ()
    truckUpdateAttributes = ['truckRegExp', 'licensePlateNumber', 'fedAnnualInspectionExp', 'stateAnnualInspectionExp',
                             'driver1Fk', 'driver2Fk', 'trailerFk', 'vehicleFk', 'status',
                             'solo', 'insureStartDt', 'insureEndDt', 'vin', 'documents']
    for fieldName in truckUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'truckId' and fieldName != 'sub':
            if fieldName == 'documents':
                documents = body[fieldName]
                cursor.execute("UPDATE dispatch_dev.truck SET documents = %s WHERE truck_id=%s AND sub=%s", (json.dumps(documents), body['truckId'], body['sub']))
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (json.dumps(documents),)
            else:
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['truckId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where truck_id = %s and sub = %s',
        "values": values
    }


def getTrucksCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getTruckCountBySub, sub)
    return cursor.fetchone()['count']


def onboardTruck(connection, bodyDetails):
    print('body')
    print(bodyDetails)
    body = json.loads(bodyDetails)
    cursor = connection.cursor()

    try:

        cursor.execute(insertTruck,
                       (body['truckRegExp'], 
                        body['licensePlateNumber'], 
                        body['fedAnnualInspectionExp'],
                        body['stateAnnualInspectionExp'],
                        body['driver1Fk'], 
                        body['driver2Fk'], 
                        body['trailerFk'], 
                        body['vehicleFk'],
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                        body['sub'], 
                        body['solo'], 
                        body['insureStartDt'], 
                        body['insureEndDt'],
                        body['vin'],
                        json.dumps(body['documents'])))
        insertedTruckId = connection.insert_id()
        print("Inserted truck ID : {}".format(insertedTruckId))
    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving truck')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving truck', 'Error': ex})
    else:
        connection.commit()
        insertedTruckDetails = getTruck(connection, insertedTruckId, body['sub'])
        connection.close()

    return insertedTruckDetails


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


def deleteTruck(connection, truckId, sub):
    print(truckId)
    print(sub)
    cursor = connection.cursor()
    try:
        cursor.execute(deleteTruckByIdAndSub, ('DELETED', truckId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'TruckId: {} not found for sub: {}'.format(truckId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting truck with truckId:{} and sub : {},  Error : {}".format(truckId, sub,
                                                                                                       ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting truck with '
                                              'truckId: {} and sub:{}'.format(truckId, sub)})


def formatTruckResponse(row):
    response = {'truckId': row['truck_id'], 
                'truckNumber':row['truck_number'],
                'truckRegExp': str(row['truck_reg_exp']), 
                'licensePlateNumber': str(row['license_plate_number']),
                'fedAnnualInspectionExp': str(row['fed_annual_inspection_exp']), 
                'stateAnnualInspectionExp': str(row['state_annual_inspection_exp']),
                'driver1Fk': row['driver1_fk'], 
                'driver2Fk': row['driver2_fk'], 
                'trailerFk': row['trailer_fk'], 
                'vehicleFk': row['vehicle_fk'],
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']),
                'status': row['status'], 
                'sub': row['sub'], 
                'solo': bool(row['solo']), 
                'insureStartDt': str(row['insure_start_dt']),
                'insureEndDt': str(row['insure_end_dt']), 
                'vin': row['vin'],
                'documents': json.loads(row['documents']) if row['documents'] else []
                }
    return response


def formatTruckViewResponse(row):
    print(row)
    response = {'truckId': row['truckId'],
                'truckNumber':row['truckNumber'],
                'truckPlateNumber': str(row['truckPlateNumber']), 
                'primaryDriverName': str(row['primaryDriverName']),
                'trailerPlateNumber': str(row['trailerPlateNumber']), 
                'solo': str(row['solo']),
                'trailerNumber': str(row['trailerNumber']),
                
                }
    return response


def formatDriverResponse(row, driverRole):
    response = {'driverId': row['driver_id'], 
                'fullName': str(row['full_name']), 
                "driverRole": driverRole, 
                'sub': str(row['sub'])}
    print("formatDriverResponse response ".format(response))
    return response


def formatTrailerResponse(row):
    response = {'trailerId': row['trailer_id'], 
                'licensePlateNumber': str(row['license_plate_number']), 
                'sub': str(row['sub'])}
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


# def buildUpdateQueryParameters(body):
#     print(body)
#     keyValueList = []
#     baseQuery = 'update dispatch_dev.truck set '
#     values = ()
#     truckUpdateAttributes = ['truckRegExp', 'licensePlateNumber', 'fedAnnualInspectionExp', 'stateAnnualInspectionExp',
#                              'driver1Fk', 'driver2Fk', 'trailerFk', 'vehicleFk', 'status',
#                              'solo', 'insureStartDt', 'insureEndDt', 'vin', 'documents']
#     for fieldName in truckUpdateAttributes:
#         print(fieldName)
#         print(fieldName in body)
#         if fieldName in body and fieldName != 'truckId' and fieldName != 'sub':
#             if fieldName == 'documents':
#                 documents = body[fieldName]
#                 documentList = []
#                 for document in documents:
#                     documentData = {}
#                     for field, value in document.items():
#                         documentData[camel2snake(field)] = value
#                     documentList.append(documentData)
#                 keyValueList.append(camel2snake(fieldName) + '= %s')
#                 values = values + (json.dumps(documentList),)
#             else:
#                 keyValueList.append(camel2snake(fieldName) + '= %s')
#                 values = values + (body[fieldName],)
#     if len(keyValueList) > 0:
#         keyValueList.append('ts_updated = %s')
#         values = values + (datetime.datetime.now(),)
#     values = values + (body['truckId'], body['sub'])
#     print(','.join(keyValueList))
#     print(values)
#     return {
#         "query": baseQuery + ','.join(keyValueList) + ' where truck_id = %s and sub = %s',
#         "values": values
#     }
