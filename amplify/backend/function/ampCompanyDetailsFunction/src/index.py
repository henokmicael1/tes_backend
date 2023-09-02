import re
import datetime
import boto3
import json
import pymysql
from query import *
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
healthPath = '/company/health'


# SUMMARY API'S
addCompanyDetails = '/company/addcompanydetails'
getAllCompanywithSub = '/company/getallcompany'
getAllCompanywithIdandSub = '/company/getcompanybyid'
updateCompanyWithIdandSub = '/company/updatecompany'


def handler(event, context):
    # log.info("Request event : {}".format(event))
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")

    elif httpMethod == getMethod and path == getAllCompanywithSub:
        response = getAllCompanys(conn, event['queryStringParameters']['sub'])

    elif httpMethod == getMethod and path == getAllCompanywithIdandSub:
        response = getcompanybysubanditsid(
            conn, event['queryStringParameters']['companyId'], event['queryStringParameters']['sub'])

    elif httpMethod == postMethod and path == addCompanyDetails:
        response = companydetails(conn, event['body'])

    elif httpMethod == patchMethod and path == updateCompanyWithIdandSub:
        response = updateCompany(conn, event['body'])

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
        conn = pymysql.connect(user=name, password=password,
                               host=rds_host, database=db_name)
        print("SUCCESS: Successfully connected to MySQL instance.")
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    return conn


# Company details settings tab.
def companydetails(connection, bodyDetails):
    print("TEST")
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    print("booooo", body)
    try:
        values = (
            body['commission'],
            body['insurancerate'],
            body['factoringinfo'],
            body['companyname'],
            body['companyaddress'],
            datetime.datetime.now(),
            datetime.datetime.now(),
            body['status'],
            body['sub'],
            body['telephone'],
            body['fax'],
        )
        cursor.execute(insertCompanyDetails, values)
        print("booooo1")
        insertedCompanyId = connection.insert_id()
        print("booooo2")
        print("Dataxxx", insertedCompanyId)
    except pymysql.Error as ex:
        connection.rollback()
        connection.close()
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving company', 'Error': ex})
    else:
        connection.commit()
        cursor.execute(
            "SELECT * FROM dispatch_dev.companydetails WHERE companyId = %s", (insertedCompanyId,))
        result = formatCreatedCompanyResponse(cursor.fetchone())
        connection.close()
        return buildResponse(200, result)


def getAllCompanys(connection, sub):
    print("TESTING__GET_ALL_SETTINGS")
    cursor = connection.cursor()
    cursor.execute(getCompanyBySub, sub)
    companys = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                companys.append(formatCreatedCompanyResponse(row))
            loadsResponse = {
                "companysInformation": companys
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No company record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all company details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all company details for sub : {}'
                             .format(sub)})


def getcompanybysubanditsid(connection, companyId, sub):
    print("TESTING__GET_BY_ID_SETTINGS")
    cursor = connection.cursor()
    cursor.execute(getCompanyByIdandSub, (companyId, sub))
    try:
        if cursor.rowcount != 0:
            expnse = formatCreatedCompanyResponse(cursor.fetchone())
            return buildResponse(200, expnse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'company: {} not found for sub: {}'.format(companyId, sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while fetching company details for companyId : {companyId} , Error: {ex}'})


def formatCreatedCompanyResponse(row):
    # print("formatCreatedCompanyResponse = {}".format(row))
    response = {'companyId': row[0],
                'commission': str(row[1]),
                'insurancerate': str(row[2]),
                'factoringinfo': str(row[3]),
                'companyname': str(row[4]),
                'companyaddress': str(row[5]),
                'ts_created': str(row[6]),
                'ts_updated': str(row[7]),
                'status': str(row[8]),
                'sub': str(row[9]),
                'telephone': str(row[10]),
                'fax': str(row[11]),
                }
    # response[‘event’] = event
    return response


def updateCompany(connection, bodyDetails):
    cursor = connection.cursor()
    if 'companyId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'companyId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryCompanyParameters = buildUpdateQueryParametersCompany(body)
    print("updateQueryCompanyParameters")
    print(updateQueryCompanyParameters)
    try:
        cursor.execute(
            updateQueryCompanyParameters['query'], updateQueryCompanyParameters['values'])
        connection.commit()
        cursor.execute(getCompanyByIdandSub, (body['companyId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'company': formatCreatedCompanyResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {
                'Message': 'companyId: {} not found for sub: {}'.format(body['companyId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating company with companyId:{} and sub : {} , Error : {}".format(
            body['companyId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred while updating company with '
                                              'companyId: {} and sub:{}'.format(body['companyId'],
                                                                                body['sub'])})


def buildUpdateQueryParametersCompany(body):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.companydetails set '
    values = ()
    companyUpdateAttributes = ['commission', 'insurancerate', 'factoringinfo', 'companyname',
                               'companyaddress', 'status', 'telephone', 'fax']
    for fieldName in companyUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'companyId' and fieldName != 'sub':
            keyValueList.append(camel2snake(fieldName) + '= %s')
            values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['companyId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where companyId = %s and sub = %s',
        "values": values
    }


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


def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])
