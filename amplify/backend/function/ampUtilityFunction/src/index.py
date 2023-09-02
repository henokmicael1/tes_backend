import json
import boto3
import pymysql
from query import *
import re
import datetime
# Constants
GET_METHOD = 'GET'
POST_METHOD = 'POST'
PATCH_METHOD = 'PATCH'
DELETE_METHOD = 'DELETE'
HEALTH_PATH = '/utility/health'
CITIES_PATH = '/utility/cities'
STATES_PATH = '/utility/states'
VEHICLE_MAKE_PATH = '/utility/vehicle/make'
VEHICLE_MODEL_PATH = '/utility/vehicle/model'
VEHICLE_YEAR_PATH = '/utility/vehicle/year'
VEHICLE_PATH = '/utility/vehicle'
PRICING_PLANS = '/utility/pricingPlans'
CONTACT_US = '/utility/contactus'

# Secrets Manager
DB_CLIENT = boto3.client('secretsmanager')
DB_SECRET = DB_CLIENT.get_secret_value(SecretId='dev/dispatch/app')
DB_SECRET_DICT = json.loads(DB_SECRET['SecretString'])


def handler(event, context):
    print('received event:')
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    if httpMethod == GET_METHOD and path == HEALTH_PATH:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == GET_METHOD and path == STATES_PATH:
        response = getStates(conn)
    elif httpMethod == GET_METHOD and path == PRICING_PLANS:
        response = getPricingPlans(conn)
    elif httpMethod == GET_METHOD and path == CITIES_PATH and 'state' in event['queryStringParameters']:
        response = getCities(conn, event['queryStringParameters']['state'])
    elif httpMethod == GET_METHOD and path == VEHICLE_MAKE_PATH:
        response = getVehicleMake(conn)
    elif httpMethod == GET_METHOD and path == VEHICLE_MODEL_PATH:
        response = getVehicleModel(conn, event['queryStringParameters']['make'])
    elif httpMethod == GET_METHOD and path == VEHICLE_YEAR_PATH:
        response = getVehicleYear(conn, event['queryStringParameters']['make'], event['queryStringParameters']['model'])
    elif httpMethod == GET_METHOD and path == VEHICLE_PATH:
        response = getVehicle(conn, event['queryStringParameters']['vehicleId'])
    elif httpMethod == POST_METHOD and path == CONTACT_US:
        response = contactUs(conn, event['body'])
    return response


def getStates(connection):
    cursor = connection.cursor()
    cursor.execute(getAllStates)
    states = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                states.append(formatStateResponse(row))
            return buildResponse(200, states)
    except Exception as ex:
        print("Exception occurred while fetching states")
        return buildResponse(500, {'Message': 'Exception occurred while fetching states', 'Error': ex})


def getCities(connection, state):
    cursor = connection.cursor()
    cursor.execute(getCitiesByState, state)
    cities = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                cities.append(row[0])
            return buildResponse(200, cities)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No cities  found for state : {}'.format(state)})
    except Exception as ex:
        print("Exception occurred while fetching cities for  state: {}".format(state))
        return buildResponse(500, {'Message': 'Exception occurred while fetching cities for state : {}'
                             .format(state), 'Error': ex})


def getVehicleMake(connection):
    cursor = connection.cursor()
    cursor.execute(getAllVehicleMake)
    vehicles = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                vehicles.append(row[0])
            return buildResponse(200, vehicles)
    except Exception as ex:
        print("Exception occurred while fetching vehicle make data")
        return buildResponse(500, {'Message': 'Exception occurred while fetching vehicle make data', 'Error': ex})


def getVehicleModel(connection, make):
    cursor = connection.cursor()
    cursor.execute(getVehicleModelByMake, make)
    vehicleModel = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                vehicleModel.append(row[0])
            return buildResponse(200, vehicleModel)
    except Exception as ex:
        print("Exception occurred while fetching vehicle model data for make: {}".format(make))
        return buildResponse(500, {
            'Message': 'Exception occurred while fetching vehicle model data for make: {}'.format(make), 'Error': ex})


def getVehicle(connection, vehicleId):
    cursor = connection.cursor()
    cursor.execute(getVehicleById, vehicleId)
    try:
        if cursor.rowcount != 0:
            vehicle = formatVehiclesResponse(cursor.fetchone())
            return buildResponse(200, vehicle)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'vehicleId: {} not found'.format(vehicleId)})
    except Exception as ex:
        print("Exception occurred while fetching vehicle details for vehicleId:{}".format(vehicleId))
        return buildResponse(500, {'Message': 'Exception occurred while fetching vehicle details for '
                                              'vehicleId: {} and sub:{}'.format(vehicleId), 'Error': ex})


def getVehicleYear(connection, make, model):
    """
    Retrieves the year(s) of a given vehicle make and model from a database connection.

    Args:
        connection (pymysql.connections.Connection): The database connection to use.
        make (str): The make of the vehicle to retrieve year information for.
        model (str): The model of the vehicle to retrieve year information for.

    Returns:
        dict: A dictionary containing either the retrieved year(s) or an error message.
    """
    cursor = connection.cursor()
    cursor.execute(getVehicleYearByMakeAndModel, (make, model))
    vehicle_years = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                vehicle_years.append(formatYearResponse(row))
            return buildResponse(200, vehicle_years)
    except Exception as ex:
        error_msg = 'Exception occurred while fetching vehicle year data for make: {} and for model: {}'.format(make,
                                                                                                                model)
        return buildResponse(500, {'Message': error_msg, 'Error': ex})


def contactUs(connection, bodyDetails):
    print('body')
    print(bodyDetails)
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    try:
        cursor.execute(insertContactDetails, (
            body['fullname'],
            body['email'],
            body['phone_number'],
            body['describe_issue'],
            body['driver1_fk'],
            body['sub'],
            datetime.datetime.now(),
            datetime.datetime.now()
        ))

    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving contact us information')
        return buildResponse(500, {'Message': 'Exception occurred while saving contact us information', 'Error': ex})

    else:
        connection.commit()
        connection.close()
        print('Contact us information saved successfully')  
        return buildResponse(200, {'Message': 'Contact us information saved successfully'})


def database_connect():
    """
    Function to connect to a MySQL instance hosted on AWS RDS using credentials stored in a dictionary.
    Returns a connection object if successful.
    """
    # print message for debugging purposes
    print('database_connect')

    # get database connection details from dictionary
    rds_host = DB_SECRET_DICT['host']
    name = DB_SECRET_DICT['username']
    password = DB_SECRET_DICT['password']
    db_name = DB_SECRET_DICT['dbName']

    # initialize connection object
    conn = {}

    try:
        # establish connection using pymysql library
        conn = pymysql.connect(user=name, password=password, host=rds_host, database=db_name)
        # print success message if connection is established
        print("SUCCESS: Successfully connected to MySQL instance.")
    except pymysql.MySQLError as e:
        # print error message if connection fails
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)

    # print success message regardless of connection status
    print("SUCCESS: Connection to RDS MySQL instance succeeded")

    # return connection object
    return conn


def getPricingPlans(connection):
    """
    Function to return all supported pricing plans .
    """

    print("getPricingPlans")
    cursor = connection.cursor()
    cursor.execute(getAllPricePlans)

    # fetch all the rows returned by the query
    rows = cursor.fetchall()

    # extract the column names from the cursor object
    columns = [column[0] for column in cursor.description]

    # convert the rows to a list of dictionaries, where each dictionary represents a row
    data = [dict(zip(columns, row)) for row in rows]
    # convert the features and price columns to dictionaries
    for row in data:
        row['features'] = json.loads(row['features'])
        row['price'] = json.loads(row['price'])

    return buildResponse(200, data)


def formatVehiclesResponse(row):
    response = {'vehicleId': row[0],
                'make': str(row[1]),
                'model': str(row[2]),
                'year': row[3]}
    # response[‘event’] = event
    return response


def formatStateResponse(row):
    response = {'stateCode': str(row[0]), 'stateName': str(row[1])}
    return response


def formatYearResponse(row):
    response = {'vehicleId': row[0], 'year': row[1]}
    return response


def buildResponse(statusCode, body=None):
    print("buildResponse method called with statusCode : {} and body : {}".format(statusCode, body))
    response = {
        'statusCode': statusCode,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT'
        },
        'body': None
    }
    if body is not None:
        response['body'] = json.dumps(body)
    return response
