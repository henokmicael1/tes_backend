import datetime
import json
import boto3
import pymysql
from driver import Driver
from query import *
import re
from cognito import  signUpDriverCognito, deleteDriverCognito
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
healthPath = '/driver/health'
searchDriverPath = '/driver/search'
updatedriverprofilepicture = '/driver/driverprofile'

def handler(event, context):
    # log.info("Request event : {}".format(event))
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()
    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == postMethod and path == searchDriverPath:
        response = searchDriver(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                                event['queryStringParameters']['offset'], event['body'])
    elif httpMethod == getMethod and 'driverId' in event['queryStringParameters']:
        response = getDriver(conn, event['queryStringParameters']['driverId'], event['queryStringParameters']['sub'])
    elif httpMethod == getMethod and 'limit' in event['queryStringParameters']:
        response = getDrivers(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                              event['queryStringParameters']['offset'])
    
    elif httpMethod == patchMethod and path == updatedriverprofilepicture:
        response = changeDriverProfile(conn, event['queryStringParameters']['driverid'],
                           event['queryStringParameters']['sub'], event['body'])

    elif httpMethod == postMethod:
        try:
            # driver = Driver(**json.loads(event['body']))
            print(event['body'])
            print(event['body'])
            response = onboardDriver(conn, event['body'])
        except ValidationError as ex:
            # log.error(ex)
            print("Validation error , error = {}".format(ex))
            response = buildResponse(400, "Invalid Input")
    elif httpMethod == patchMethod:
        response = updateDriver(conn, event['body'])
    elif httpMethod == deleteMethod:
        response = deleteDriver(conn, event['queryStringParameters']['driverId'], event['queryStringParameters']['sub'])

    # elif httpMethod == putMethod and path == driverPath:
    #     response = modifyDriver(conn,NULL)
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


def getDriver(connection, driverId, sub):
    cursor = connection.cursor()
    cursor.execute(getDriverByIdAndSub, (driverId, sub))
    try:
        if cursor.rowcount != 0:
            driver = formatDriverMobileResponse(cursor.fetchone())
            # driver['trucks']['trailer'] = {}
            print("driver", driver)
            if 'trucks' in driver and isinstance(driver['trucks'], dict) and len(driver['trucks']) > 0:
                print("INSIDEIF2")
                trailerId = driver['trucks']['trailer_fk']
                print("trailerId", trailerId)
                if trailerId is not None:
                    print("INSIDEIF")
                    cursor.execute(getTrailerforEachTruck, (trailerId, driver['sub']))
                    driver['trucks']['trailer'] = getTrailerDataResponse(cursor.fetchone())      
                else:
                    driver['trucks']['trailer'] = {}
                del trailerId
            
            return buildResponse(200, driver)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'DriverId: {} not found for sub: {}'.format(driverId, sub)})
    except Exception as ex:
        print("Exception occurred while fetching driver details for driverId:{} and sub : {}".format(driverId, sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching driver details for '
                                              'driverId: {} and sub:{}'.format(driverId, sub), 'Error': ex})

def changeDriverProfile(connection, driverid, sub, bodyDetails):
    print("changeDriverProfile")
    cursor = connection.cursor()
    cursor.execute(getDriverByIdAndSub, (driverid, sub))
    try:
       bodyParam = json.loads(bodyDetails)
       profile_url = bodyParam['profile_url']
       if cursor.rowcount != 0:
            updateDriverPicture = "update dispatch_dev.driver SET profile_url = %s where driver_id = %s and sub = %s"
            cursor.execute(updateDriverPicture, (profile_url, driverid, sub))
            loadsResponse = {"Message":"Profile picture updated"}
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching the driver details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching the driver details for sub : {}'
                             .format(sub)})


def getDrivers(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getDriverBySub, (sub, int(limit), (int(limit) * int(offset))))
    drivers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                drivers.append(formatDriverResponse(row))
            driversResponse = {
                "totalDbCount": getDriversCount(connection, sub),
                "drivers": drivers
            }
            return buildResponse(200, driversResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No driver record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all driver details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all driver details for sub : {}'
                             .format(sub)})


def searchDriver(connection, sub, limit, offset, bodyDetails):
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    print("searchCriteria : {}".format(body['searchCriteria']))
    searchQueryParameters = buildSearchQuery(body['searchCriteria'])
    print("searchQueryParameters : {}".format(searchQueryParameters))
    finalValues = searchQueryParameters['values'] + (sub, 'ACTIVE', int(limit), (int(limit) * int(offset)))
    print("finalValues : {}".format(finalValues))

    cursor.execute(searchQueryParameters['query'], finalValues)
    drivers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                count = row['count']
                drivers.append(formatTruckSearchResponse(row))
            driversResponse = {
                "totalDbCount": count,
                "drivers": drivers
            }
            return buildResponse(200, driversResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No driver record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all driver details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all driver details for sub : {}'
                             .format(sub)})


def getDriversCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getDriverCountBySub, sub)
    return cursor.fetchone()['count']


def updateDriver(connection, bodyDetails):
    cursor = connection.cursor()
    if 'driverId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'driverId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryParameters = buildUpdateQueryParameters(body, cursor)
    print("updateQueryParameters")
    print(updateQueryParameters)
    try:
        cursor.execute(updateQueryParameters['query'], updateQueryParameters['values'])
        connection.commit()
        cursor.execute(getDriverByIdAndSub, (body['driverId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'driver': formatDriverResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {
                'Message': 'DriverId: {} not found for sub: {}'.format(body['driverId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating driver with driverId:{} and sub : {} , Error : {}".format(
            body['driverId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred  while updating driver with '
                                              'driverId: {} and sub:{}'.format(body['driverId'],
                                                                               body['sub'])})


def buildUpdateQueryParameters(body, cursor):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.driver set '
    values = ()
    driverUpdateAttributes = ["firstName", "fullName", "lastName", "phoneNumber", "email", "birthDate",
                              "cdlLicenseNumber", "cdlLicenseExpDate", "cdlState", "einNumber", "medCardExp", "mvrExp",
                              "drugClearExp", "address1", "address2", "state", "city", "country", "zipCode","status", "documents","profile_url"]
    for fieldName in driverUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'driverId' and fieldName != 'sub':
            if fieldName == 'documents':
                documents = body[fieldName]
                cursor.execute("UPDATE dispatch_dev.driver SET documents = %s WHERE driver_id=%s AND sub=%s", (json.dumps(documents), body['driverId'], body['sub']))
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (json.dumps(documents),)
            else:
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (body[fieldName],)

    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['driverId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where driver_id = %s and sub = %s',
        "values": values
    }


def buildSearchQuery(body):
    print(body)
    keyValueList = []                
    baseQuery = 'select driver_id,driver_number,full_name,phone_number,email,address1,address2,state,city,country,zip_code,sub, count(*) OVER() AS count FROM dispatch_dev.driver '

    values = ()
    driverSearchAttributes = ["firstName",'driverNumber', "lastName","fullName", 'phoneNumber',"email", "cdlLicenseNumber", "einNumber", "state"]
    for fieldName in driverSearchAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'sub':
            keyValueList.append(camel2snake(fieldName) + ' like %s and ')
            values = values + ('%' + str(body[fieldName]) + '%',)
            
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + 'where ' + ' '.join(
            keyValueList) + 'sub = %s and status =  %s order by driver_id limit '
                            '%s offset %s',
        "values": values
    }


def deleteDriver(connection, driverId, sub):
    print(driverId)
    print(sub)
    cursor = connection.cursor()
    try:
        driverDetails = getDriver(connection, driverId, sub)
        body = json.loads(driverDetails['body'])
        deleteDriverCognito(body)
        cursor.execute(deleteDriverByIdAndSub, ('DELETED', driverId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'DriverId: {} not found for sub: {}'.format(driverId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting driver with driverId:{} and sub : {},  Error : {}".format(driverId, sub,
                                                                                                         ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting driver with '
                                              'driverId: {} and sub:{}'.format(driverId, sub)})


def onboardDriver(connection, bodyDetails):
    print('body')
    print(bodyDetails)
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    
     
    fullName = body['firstName'] + ' ' + body['lastName']
    try:
        cursor.execute(insertDriver,
                       (body['firstName'], 
                        fullName, 
                        body['lastName'], 
                        body['phoneNumber'], 
                        body['email'],
                        body['birthDate'],
                        body['cdlLicenseNumber'], 
                        body['cdlLicenseExpDate'], 
                        body['cdlState'], 
                        body['einNumber'],
                        body['medCardExp'],
                        body['mvrExp'], 
                        body['drugClearExp'], 
                        body['address1'], 
                        body['address2'],
                        body['state'],
                        body['city'], 
                        body['country'], 
                        body['zipCode'], 
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                        json.dumps(body['documents']),
                        body['sub']))
        insertedDriverId = connection.insert_id()
        
        signUpResponse = signUpDriverCognito(body,insertedDriverId)
        print("Inserted driver ID : {}".format(insertedDriverId))
        if signUpResponse['statusCode'] == 400:
            connection.rollback()
            connection.close()
            return buildResponse(signUpResponse['statusCode'],signUpResponse['body'])
        
    except Exception as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving driver')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving driver', 'Error': ex})
    else:
        connection.commit()
        cursor.execute("select * from dispatch_dev.driver  where driver_id = %s and sub = %s", (insertedDriverId, body['sub']))
        result = formatDriverResponse(cursor.fetchone())
        connection.close()

    return buildResponse(200, result)


def formatDriverResponse(row):
    response = {'driverId': row['driver_id'], 
                'driverNumber': row['driver_number'],
                'firstName': str(row['first_name']), 
                'fullName': str(row['full_name']),
                'lastName': str(row['last_name']), 
                'phoneNumber': str(row['phone_number']), 
                'email': str(row['email']),
                'birthDate': str(row['birth_date']), 
                'cdlLicenseNumber': str(row['cdl_license_number']), 
                'cdlLicenseExpDate': str(row['cdl_license_exp_date']),
                'cdlState': str(row['cdl_state']), 
                'einNumber': str(row['ein_number']), 
                'medCardExp': str(row['med_card_exp']),
                'mvrExp': str(row['mvr_exp']), 
                'drugClearExp': str(row['drug_clear_exp']),
                'address1': str(row['address1']), 
                'address2': str(row['address2']), 
                'state': str(row['state']),
                'city': str(row['city']), 
                'country': str(row['country']), 
                'zipCode': str(row['zip_code']),
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']), 
                'status': str(row['status']),
                'sub': str(row['sub']),
                'documents': json.loads(row['documents']) if row['documents'] else [],
                'profile_url': str(row['profile_url']),
                }
    # response[‘event’] = event
    return response

def formatDriverMobileResponse(row):
    print("formatDriverMobileResponse", row)
    response = {
        'driverId': row['driver_id'],
        'driverNumber': row['driver_number'],
        'firstName': str(row['first_name']),
        'fullName': str(row['full_name']),
        'lastName': str(row['last_name']),
        'phoneNumber': str(row['phone_number']),
        'email': str(row['email']),
        'birthDate': str(row['birth_date']),
        'cdlLicenseNumber': str(row['cdl_license_number']),
        'cdlLicenseExpDate': str(row['cdl_license_exp_date']),
        'cdlState': str(row['cdl_state']),
        'einNumber': str(row['ein_number']),
        'medCardExp': str(row['med_card_exp']),
        'mvrExp': str(row['mvr_exp']),
        'drugClearExp': str(row['drug_clear_exp']),
        'address1': str(row['address1']),
        'address2': str(row['address2']),
        'state': str(row['state']),
        'city': str(row['city']),
        'country': str(row['country']),
        'zipCode': str(row['zip_code']),
        'tsCreated': str(row['ts_created']),
        'tsUpdated': str(row['ts_updated']),
        'status': str(row['status']),
        'sub': str(row['sub']),
        'documents': json.loads(row['documents']) if row['documents'] else [],
        'profile_url': str(row['profile_url']),
        'trucks': None,
    }
    if row['truck_id'] is not None:
        print("with truck")
        truck = {
            'truck_id': row['truck_id'] if row['truck_id'] is not None else None,
            'truck_reg_exp': str(row['truck_reg_exp']) if row['truck_reg_exp'] is not None else None,
            'license_plate_number': str(row['license_plate_number']) if row['license_plate_number'] is not None else None,
            'fed_annual_inspection_exp': str(row['fed_annual_inspection_exp']) if row['fed_annual_inspection_exp'] is not None else None,
            'state_annual_inspection_exp': str(row['state_annual_inspection_exp']) if row['state_annual_inspection_exp'] is not None else None,
            'trailer_fk': row['trailer_fk'] if row['trailer_fk'] is not None else None,
            'solo': str(row['solo']) if row['solo'] is not None else None,
            'insure_start_dt': str(row['insure_start_dt']) if row['insure_start_dt'] is not None else None,
            'insure_end_dt': str(row['insure_end_dt']) if row['insure_end_dt'] is not None else None,
            'vin': str(row['vin']) if row['vin'] is not None else None,
            'make': str(row['truck_vehicle_make']) if row['truck_vehicle_make'] is not None else None,
            'model': str(row['truck_vehicle_model']) if row['truck_vehicle_model'] is not None else None,
            'year': str(row['truck_vehicle_year']) if row['truck_vehicle_year'] is not None else None,
        }
        response['trucks'] = truck
    else:
        print("without")
        response['trucks'] = {}
    return response

def getTrailerDataResponse(row):
    print("FORMATTRAILERRESPONSE", row)
    response = {
        'trailer_id': row['trailer_id'],
        'license_plate_number': str(row['trailer_license_plate_number']),
        'fed_annual_inspection_exp': str(row['trailer_fed_annual_inspection_exp']),
        'trailer_vins': str(row['trailer_vin']),
        'trailer_state_annual_inspection_exps': str(row['trailer_state_annual_inspection_exp']),
        'trailer_reg_exps': str(row['trailer_reg_exp']),
        'trailer_insure_start_dt': str(row['trailer_insure_start_dt']),
        'trailer_insure_end_dt': str(row['trailer_insure_end_dt']),
        'trailer_types': str(row['trailer_type']),
        'trailer_vehicle_make': str(row['trailer_vehicle_make']),
        'trailer_vehicle_model': str(row['trailer_vehicle_model']),
        'trailer_vehicle_year': str(row['trailer_vehicle_year'])
    }

    return response


def formatTruckSearchResponse(row):
    response = {'driverId': row['driver_id'], 
                'driverNumber': row['driver_number'],
                'fullName': str(row['full_name']), 
                'phoneNumber': str(row['phone_number']),
                'email': str(row['email']),
                'address1': str(row['address1']),
                'address2': str(row['address2']),
                'state': str(row['state']),
                'city': str(row['city']),
                'country': str(row['country']),
                'zipCode': str(row['zip_code']),
                'sub': str(row['sub'])
                }
    return response

def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])


# def signUpDriver(body):
#     print("signUpDriver")
#     idp_client = boto3.client('cognito-idp')
#     user_pool_id = 'us-east-1_xJnVekLO8'
#
#     response = idp_client.admin_create_user(
#         DesiredDeliveryMediums=['EMAIL'],
#         UserPoolId=user_pool_id,
#         Username=body['email'],
#         UserAttributes=[
#             {
#                 'Name': 'email',
#                 'Value': body['email']
#             }, {
#                 'Name': 'email_verified',
#                 'Value': 'true'
#             }
#         ],
#         TemporaryPassword='3jdu3sdfwerew-1'
#     )
#
#     print(response)


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
