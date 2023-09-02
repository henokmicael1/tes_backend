import datetime
import json
import pymysql
import boto3
from query import *
from search import *
from common import *
from cognito import signUpAccountantCognito, deleteAccountantCognito
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
healthPath = '/accountant/health'
searchAccountantPath = '/accountant/accountant/search'

# ACCOUNTANT API'S
addAccountantDetails = '/accountant/addaccountant'
getAllAccountantsDetails = '/accountant/getallaccounts'
getAccountantsDetailsbyIdandSub = '/accountant/getaccountant'
updateAccountantDetails = '/accountant/updateaccountantdetails'
deleteAccountantInformation = '/accountant/deleteaccountant'


def handler(event, context):
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")

    elif httpMethod == getMethod and path == getAllAccountantsDetails:
        response = getAllAccountants(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                                     event['queryStringParameters']['offset'])

    elif httpMethod == getMethod and path == getAccountantsDetailsbyIdandSub:
        response = getaccountantbysubanditsid(
            conn, event['queryStringParameters']['accId'], event['queryStringParameters']['sub'])

    elif httpMethod == postMethod and path == searchAccountantPath:
        response = searchAccountant(conn, event['queryStringParameters']['sub'],
                                    event['queryStringParameters']['limit'],
                                    event['queryStringParameters']['offset'], event['body'])

    elif httpMethod == postMethod and path == addAccountantDetails:
        response = addaccountantinformation(conn, event['body'])

    elif httpMethod == patchMethod and path == updateAccountantDetails:
        response = updateAccountantInformation(conn, event['body'])

    elif httpMethod == deleteMethod and path == deleteAccountantInformation:
        response = deleteAccountant(conn, event['queryStringParameters']['accountantId'],
                                    event['queryStringParameters']['sub'])
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



# ACCOUNTANTS API'S.
def addaccountantinformation(connection, bodyDetails):
    print('addaccountantinformation')
    print(bodyDetails)
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    fullName = body['firstName'] + ' ' + body['lastName']

    try:
        cursor.execute(insertAccountant,
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
                        json.dumps(body['documents']),
                        body['sub'],
                        fullName,
                        body['status'],
                        body['accountantType'],
                        body['commissionPercentage'],
                        body['monthlySalary'],
                        ))

        insertedAccountantId = connection.insert_id()
        signUpResponse = signUpAccountantCognito(body,insertedAccountantId)
        print("Inserted accountant ID : {}".format(insertedAccountantId))
        if signUpResponse['statusCode'] == 400:
            connection.rollback()
            connection.close()
            return buildResponse(signUpResponse['statusCode'],signUpResponse['body'])
        
    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving accountant')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving accountant', 'Error': ex})
    else:
        connection.commit()
        cursor.execute("SELECT * FROM dispatch_dev.accountantdetails WHERE accountantId = %s and sub = %s", (insertedAccountantId, body['sub']))
        result = formatAccountantsResponse(cursor.fetchone())
        connection.close()
        return buildResponse(200, result)



def getAllAccountants(connection, sub, limit, offset):
    print("TESTING__GET_ALL_ACCOUNTANTS")
    cursor = connection.cursor()
    cursor.execute(getAccountantsBySub, (sub, int(limit),  (int(limit) * int(offset))))
    accountants = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                accountants.append(formatAccountantsResponse(row))
            loadsResponse = {
                "totalDbCount": getAccountantCount(connection, sub),
                "accountantsInformation": accountants
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No accountants record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all accountants details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all accountants details for sub : {}'
                             .format(sub)})


def getAccountantCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getAccountantCountBySub, sub)
    return cursor.fetchone()[0]

def getaccountantbysubanditsid(connection,accId,sub):
    print("TESTING__GET_BY_ID_ACCOUNTANTS")
    cursor = connection.cursor()
    cursor.execute(getAccountantByIdandSub,(accId,sub))
    try:
        if cursor.rowcount != 0:
            accountant = formatAccountantsResponse(cursor.fetchone())
            return buildResponse(200, accountant)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'accountant: {} not found for sub: {}'.format(accId, sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while fetching accountant details for accId : {accId} , Error: {ex}'})




def updateAccountantInformation(connection, bodyDetails):
    print("testingupdate>><<>><<>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    cursor = connection.cursor()
    if 'accountantId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'accountantId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryAccountantParameters = buildUpdateQueryParametersAccountants(body, cursor)
    print("updateQueryAccountantParameters")
    print(updateQueryAccountantParameters)
    try:
        cursor.execute(updateQueryAccountantParameters['query'], updateQueryAccountantParameters['values'])
        connection.commit()
        cursor.execute(getAccountantByIdandSub, (body['accountantId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'company': formatAccountantsResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {
                'Message': 'accountantId: {} not found for sub: {}'.format(body['accountantId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating accountant details with accountantId:{} and sub : {} , Error : {}".format(
            body['accountantId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred while updating accountant details with '
                                              'accountantId: {} and sub:{}'.format(body['accountantId'],
                                                                                   body['sub'])})


def buildUpdateQueryParametersAccountants(body, cursor):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.accountantdetails set '
    values = ()
    accountantUpdateAttributes = ['firstName', 'lastName', 'phoneNumber', 'email',
                                  'birthDate', 'role', 'licenseNumber', 'licenseExpDate', 'address1', 'address2', 'state', 'city', 'country', 'zipCode', 'status', 'documents']
    for fieldName in accountantUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        print(fieldName in body)
        if fieldName in body and fieldName != 'accountantId' and fieldName != 'sub':
            if fieldName == 'documents':
                documents = body[fieldName]
                cursor.execute("UPDATE dispatch_dev.accountantdetails SET documents = %s WHERE accountantId=%s AND sub=%s", (json.dumps(documents), body['accountantId'], body['sub']))
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (json.dumps(documents),)
            else:
                keyValueList.append(camel2snake(fieldName) + '= %s')
                values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['accountantId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where accountantId = %s and sub = %s',
        "values": values
    }




def deleteAccountant(connection, accountantId, sub):
    print("TESTING_DELETE")
    print(accountantId)
    print(sub)
    cursor = connection.cursor()
    try:
        accountantDetails = getaccountantbysubanditsid(connection, accountantId, sub)
        body = json.loads(accountantDetails['body'])
        deleteAccountantCognito(body)
        cursor.execute(deleteAccountantByIdAndSub, ('DELETED', accountantId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'accountantId: {} not found for sub: {}'.format(accountantId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting accountant with accountantId:{} and sub : {},  Error : {}".format(
                accountantId, sub,
                ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting accountant with '
                                              'accountantId: {} and sub:{}'.format(accountantId, sub)})


# def buildUpdateQueryParametersAccountants(body):
#     print(body)
#     keyValueList = []
#     baseQuery = 'update dispatch_dev.accountantdetails set '
#     values = ()
#     accountantUpdateAttributes = ['firstName', 'lastName', 'phoneNumber', 'email',
#                                   'birthDate', 'role', 'licenseNumber', 'licenseExpDate', 'address1', 'address2', 'state', 'city', 'country', 'zipCode', 'status']
#     for fieldName in accountantUpdateAttributes:
#         print(fieldName)
#         print(fieldName in body)
#         if fieldName in body and fieldName != 'accountantId' and fieldName != 'sub':
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
#     values = values + (body['accountantId'], body['sub'])
#     print(','.join(keyValueList))
#     print(values)
#     return {
#         "query": baseQuery + ','.join(keyValueList) + ' where accountantId = %s and sub = %s',
#         "values": values
#     }