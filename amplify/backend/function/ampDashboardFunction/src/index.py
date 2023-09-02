import json
import boto3
import pymysql
from query import *
import re
import datetime

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
healthPath = '/dashboard/health'
countPath = '/dashboard/count'
totalCount = '/dashboard/totalloadcount'
totalCountByMonth = '/dashboard/totalloadcountbymonth'
alertNotify='/dashboard/alert/notify'

# Profile API'S.
createprofile='/dashboard/createprofile'
getprofile='/dashboard/getuserprofile'
updateProfile = '/dashboard/profileupdate'
# getdispatcherprofile='/dashboard/getcurrentdispatcherprofile'

def handler(event, context):
    print('received event:')
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == getMethod and path == countPath:
        dispatcherid = event['queryStringParameters']['dispatcherid']
        sub = event['queryStringParameters']['sub']
        year = event['queryStringParameters'].get('year', '')
        start_date = event['queryStringParameters'].get('start_date', '')
        end_date = event['queryStringParameters'].get('end_date', '')
        if year:
            year = event['queryStringParameters']['year']
        else:
            year = ''
        response = getLoads(conn, dispatcherid, sub, year, start_date, end_date)
    elif httpMethod == getMethod and path == totalCount:
        response = getTotalLoadsCount(conn, event['queryStringParameters']['dispatcherid'], event['queryStringParameters']['startDate'], event['queryStringParameters']['endDate'], event['queryStringParameters']['sub'])
    elif httpMethod == getMethod and path == totalCountByMonth:
        response = getTotalLoadsCountByMonth(conn, event['queryStringParameters']['dispatcherid'], event['queryStringParameters']['sub'], event['queryStringParameters']['year'])
    elif httpMethod == getMethod and path == alertNotify:
        response = getTenDaysPriorExpiredDocuments(conn, event['queryStringParameters']['type'], event['queryStringParameters']['sub'])

    elif httpMethod == getMethod and path == getprofile:
        response = getCurrentUserProfile(conn, event['queryStringParameters']['sub'])

    # elif httpMethod == getMethod and path == getdispatcherprofile:
    #     response = getDispatcherCurrentUserProfile(conn, event['queryStringParameters']['dispatcherid'], event['queryStringParameters']['sub'])

    elif httpMethod == patchMethod and path == updateProfile:
        response = profileUpdate(conn, event['body'])

    elif httpMethod == postMethod and path == createprofile:
        response = createUsersProfile(conn, event['body'])
    return response



def getLoads(connection, dispatcherid, sub, year, start_date, end_date):
    try:
        if sub and dispatcherid == 'Null':
            loadsResponse = {
                "unassignedLoads": getLoadsCountOnlySub(connection, sub, year, start_date, end_date),
                "assignedLoads": getAssignedLoadCountOnlySub(connection, sub, year, start_date, end_date),
                "inTransitLoads": getIntransitsLoadCountOnlySub(connection, sub, year, start_date, end_date),
                "invoicedLoads": getInvoiceLoadCountOnlySub(connection, sub, year, start_date, end_date),
                "completedLoads": getCompletedLoadCountOnlySub(connection, sub, year, start_date, end_date),
                "totalCount": totalCountWithSubData(connection, sub, year, start_date, end_date),
                "totalPaidCount": totalPaidPaymentCount(connection, sub, year, start_date, end_date),
            }
        else:
            loadsResponse = {
                "unassignedLoads": getLoadsCount(connection, dispatcherid, sub, year, start_date, end_date),
                "assignedLoads": assignedLoadsCount(connection, dispatcherid, sub, year, start_date, end_date),
                "inTransitLoads": inTransitsLoadsCount(connection, dispatcherid, sub, year, start_date, end_date),
                "invoicedLoads": invoicesLoadsCount(connection, dispatcherid, sub, year, start_date, end_date),
                "completedLoads": completedLoadsCount(connection, dispatcherid, sub, year, start_date, end_date),
                "totalCount": totalCountWithSubAndIdData(connection, dispatcherid, sub, year, start_date, end_date),
            }
        return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})
                             

# Dashboard query count with dispatcher Id and sub.
def getLoadsCount(connection, dispatcherid, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getLoadCountBySubDispatcherIdWithYearAndDate, (dispatcherid, sub, year, start_date, end_date))
    elif year:
        cursor.execute(getLoadCountBySubDispatcherIdWithYear, (dispatcherid, sub, year))
    elif start_date and end_date:
        cursor.execute(getLoadCountBySubDispatcherIdWithDateRange, (dispatcherid, sub, start_date, end_date))
    else:
        cursor.execute(getLoadCountBySub, (dispatcherid, sub,))
    return cursor.fetchone()[0]

def assignedLoadsCount(connection, dispatcherid, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getAssignedLoadCountByDispatcherIdWithYearAndDate, (dispatcherid, sub, year, start_date, end_date))
    elif year:
        cursor.execute(getAssignedLoadCountByDispatcherIdWithYear, (dispatcherid, sub, year))
    elif start_date and end_date:
        cursor.execute(getAssignedLoadCountByDispatcherIdWithDateRange, (dispatcherid, sub, start_date, end_date))
    else:
        cursor.execute(getAssignedLoadLoadCountBySub, (dispatcherid, sub,))
    return cursor.fetchone()[0]

def inTransitsLoadsCount(connection, dispatcherid, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getInTransistLoadCountDispatcherIdWithYearAndDate, (dispatcherid, sub, year, start_date, end_date))
    elif year:
        cursor.execute(getInTransistLoadCountDispatcherIdWithYear, (dispatcherid, sub, year))
    elif start_date and end_date:
        cursor.execute(getInTransistLoadCountDispatcherIdWithDateRange, (dispatcherid, sub, start_date, end_date))
    else:
        cursor.execute(getInTransistLoadLoadCountBySub, (dispatcherid, sub,))
    return cursor.fetchone()[0]

def invoicesLoadsCount(connection, dispatcherid, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(invoicesLoadCountByDispatcherIdWithYearAndDate, (dispatcherid, sub, year, start_date, end_date))
    elif year:
        cursor.execute(invoicesLoadCountByDispatcherIdWithYear, (dispatcherid, sub, year))
    elif start_date and end_date:
        cursor.execute(invoicesLoadCountByDispatcherIdWithDateRange, (dispatcherid, sub, start_date, end_date))
    else:
        cursor.execute(invoicesLoadCountBySub, (dispatcherid, sub,))
    return cursor.fetchone()[0]

def completedLoadsCount(connection, dispatcherid, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(completedLoadCountByDispatcherIdWithYearAndDate, (dispatcherid, sub, year, start_date, end_date))
    elif year:
        cursor.execute(completedLoadCountByDispatcherIdWithYear, (dispatcherid, sub, year))
    elif start_date and end_date:
        cursor.execute(completedLoadCountByDispatcherIdWithDateRange, (dispatcherid, sub, start_date, end_date))
    else:
        cursor.execute(completedLoadCountBySub, (dispatcherid, sub,))
    return cursor.fetchone()[0]

def totalCountWithSubAndIdData(connection, dispatcherid, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(totalCountWithSubAndDispatcherIdWithYearAndDate, (dispatcherid, sub, year, start_date, end_date))
    elif year:
        cursor.execute(totalCountWithSubAndDispatcherIdWithYear, (dispatcherid, sub, year))
    elif start_date and end_date:
        cursor.execute(totalCountWithSubAndDispatcherIdWithDateRange, (dispatcherid, sub, start_date, end_date))
    else:
        cursor.execute(totalCountWithSubAndId, (dispatcherid, sub,))
    return cursor.fetchone()[0]

# Dashboard query count with only sub.
def getLoadsCountOnlySub(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getLoadCountBySubOnlySubAndYearAndDate, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(getLoadCountBySubOnlySubAndYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(getLoadCountBySubandDateRange, (sub, start_date, end_date))
    else:
        cursor.execute(getLoadCountBySubOnlySub, (sub,))
    return cursor.fetchone()[0]

def getAssignedLoadCountOnlySub(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getAssignedLoadCountByOnlySubAndYearAndDate, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(getAssignedLoadCountByOnlySubAndYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(getAssignedLoadCountBySubandDateRange, (sub, start_date, end_date))
    else:
        cursor.execute(getAssignedLoadCountByOnlySub, (sub,))
    return cursor.fetchone()[0]

def getIntransitsLoadCountOnlySub(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getInTransistLoadCountByOnlySubAndYearAndDate, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(getInTransistLoadCountByOnlySubAndYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(getInTransistLoadCountBySubandDateRange, (sub, start_date, end_date))
    else:
        cursor.execute(getInTransistLoadCountByOnlySub, (sub,))
    return cursor.fetchone()[0]

def getInvoiceLoadCountOnlySub(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getInvoicesLoadCountByOnlySubAndYearAndDate, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(getInvoicesLoadCountByOnlySubAndYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(getInvoicesLoadCountBySubandDateRange, (sub, start_date, end_date))
    else:
        cursor.execute(getInvoicesLoadCountByOnlySub, (sub,))
    return cursor.fetchone()[0]

def getCompletedLoadCountOnlySub(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(getCompletedLoadCountByOnlySubAndYearAndDate, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(getCompletedLoadCountByOnlySubAndYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(getCompletedLoadCountBySubandDateRange, (sub, start_date, end_date))
    else:
        cursor.execute(getCompletedLoadCountByOnlySub, (sub,))
    return cursor.fetchone()[0]

def totalCountWithSubData(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(totalCountWithSubAndYearAndDate, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(totalCountWithSubAndYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(totalCountWithSubAndDaterange, (sub, start_date, end_date))
    else:
        cursor.execute(totalCountWithSub, (sub,))
    return cursor.fetchone()[0]

def totalPaidPaymentCount(connection, sub, year, start_date=None, end_date=None):
    cursor = connection.cursor()
    if year and (start_date or end_date):
        cursor.execute(totalPaidPaymentWithYearAndDaterRange, (sub, year, start_date, end_date))
    elif year:
        cursor.execute(totalPaidPaymentWithYear, (sub, year))
    elif start_date and end_date:
        cursor.execute(totalPaidPaymentWithYearAndDate, (sub, start_date, end_date))
    else:
        cursor.execute(totalPaidPayment, (sub,))
    sum_value = cursor.fetchone()[0]
    return float(sum_value) if sum_value is not None else 0.0


def getTotalLoadsCount(connection, dispatcherid, startDate, endDate, sub):
    try:
        if sub and startDate and endDate and dispatcherid == 'Null':
            loadsResponse = {
                "totalLoad": getTotalLoadsCountByDateAndOnlySub(connection, startDate, endDate, sub)
            }
        else:
            loadsResponse = {
                "totalLoad": getTotalLoadsCountByDate(connection, dispatcherid, startDate, endDate, sub)
            }
        return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})

def createUsersProfile(connection, bodyDetails):
    print("TESTCREATEPROFILE")
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    print("TestBody---------------->>>", body)
    try:
        values = (
            body['profilename'],
            body['companyname'],
            datetime.datetime.now(),
            datetime.datetime.now(),
            body['dot'],    
            body['mc'],    
            body['telephonenumber'],    
            body['faxnumber'],    
            body['address'],    
            body['status'] ,
            body['sub'],
            body['profileurl'],
            body['userid'],
                  )
        cursor.execute(insertprofile, values)
        insertedProfileId = connection.insert_id()
        print("Dataxxx", insertedProfileId)
    except pymysql.Error as ex:
        connection.rollback()
        connection.close()
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving profile', 'Error': ex})
    else:
        connection.commit()
        cursor.execute("SELECT * FROM dispatch_dev.profiles WHERE profileid = %s", (insertedProfileId,))
        result = formatCreatedProfileResponse(cursor.fetchone())
        connection.close()
        return buildResponse(200, result)


def getCurrentUserProfile(connection,sub):
    cursor = connection.cursor()
    cursor.execute(getProfileWithSubId, sub)
    try:
        if cursor.rowcount != 0:
            profile = formatCreatedProfileResponse(cursor.fetchone())
            loadsResponse = {
                "profile": profile
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No profile record found for sub : {}'.format(sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': 'Exception occurred while fetching the company profile for sub : {}'.format(sub, ex)})

# def getDispatcherCurrentUserProfile(connection, dispatcherid, sub):
#     cursor = connection.cursor()
#     cursor.execute(getProfileWithSubId, sub)
#     try:
#         if cursor.rowcount != 0:
#             profile = formatCreatedProfileResponse(cursor.fetchone())
#             loadsResponse = {
#                 "profile": profile
#             }
#             return buildResponse(200, loadsResponse)
#         elif cursor.rowcount == 0:
#             return buildResponse(200, {'Message': 'No profile record found for dispatcher and sub : {}'.format(sub)})
#     except Exception as ex:
#         return buildResponse(500, {'Message': 'Exception occurred while fetching the dispatcher profile dispatcher for sub : {}'.format(sub, ex)})


def profileUpdate(connection, bodyDetails):
    print("testingupdate<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    cursor = connection.cursor()
    if 'profileid' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'profileid and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryCompanyParameters = buildUpdateQueryParametersProfile(body)
    print("updateQueryCompanyParameters")
    print(updateQueryCompanyParameters)
    try:
        cursor.execute(updateQueryCompanyParameters['query'], updateQueryCompanyParameters['values'])
        connection.commit()
        cursor.execute(getProfileByIdandSub, (body['profileid'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'company': formatCreatedProfileResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {
                'Message': 'profileid: {} not found for sub: {}'.format(body['profileid'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating profile with profileid:{} and sub : {} , Error : {}".format(
            body['profileid'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred while updating profile with '
                                              'profileid: {} and sub:{}'.format(body['profileid'],
                                                                                   body['sub'])})


def buildUpdateQueryParametersProfile(body):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.profiles set '
    values = ()
    companyUpdateAttributes = ['profilename', 'companyname', 'mc', 'dot',
                                  'telephonenumber', 'faxnumber', 'address', 'status', 'profileurl']
    for fieldName in companyUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'profileid' and fieldName != 'sub':
            keyValueList.append(camel2snake(fieldName) + '= %s')
            values = values + (body[fieldName],)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['profileid'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where profileid = %s and sub = %s',
        "values": values
    }


def getTotalLoadsCountByDate(connection, dispatcherid, startDate, endDate, sub):
    cursor = connection.cursor()
    cursor.execute(getTotalLoadsCountByDateRange, (dispatcherid, startDate, endDate, sub))
    return cursor.fetchone()[0]


def getTotalLoadsCountByDateAndOnlySub(connection, startDate, endDate, sub):
    cursor = connection.cursor()
    cursor.execute(getTotalLoadsCountByDateRangeAndOnlySub, (startDate, endDate, sub))
    return cursor.fetchone()[0]

def getTotalLoadsCountByMonth(connection, dispatcherid, sub, year):
    cursor = connection.cursor()
    if sub and year and dispatcherid == 'Null':
        cursor.execute(getTotalLoadsCountByMonthandYearWithSub, (sub, year))
    else:
        cursor.execute(getTotalLoadsCountByMonthandYear, (dispatcherid, sub, year))
    counts = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                counts.append(formatCountResponse(row))
            zeroLoad = []
            notZero = []
            for i in counts:
                if i['totalLoad'] == 0:
                    zeroLoad.append(i)
                else:
                    notZero.append(i)
            for i in notZero:
                for j in zeroLoad:
                    if i['monthName'] in j['monthName']:
                        j['totalLoad'] = i['totalLoad']
            loadsResponse = {
                "Year": year,
                "monthlyReport": zeroLoad
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(404, {'Message': 'No load record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        {col: row[idx] if row[idx] is not None else 0 for idx, col in enumerate(columns)}
        for row in cursor.fetchall()
    ]

def getTenDaysPriorExpiredDocuments(connection, type, sub):
    try:
        print(type)
        cursor = connection.cursor()
        if type == 'all':
            print("all")
            cursor.callproc('dispatch_dev.allNew', (10, sub))
        elif type == 'truck':
            print("truck")
            cursor.callproc('dispatch_dev.truckExpiredDocumentNew', (10, sub))
        elif type == 'driver':
            print("driver")
            cursor.callproc('dispatch_dev.driverExpiredDocumentNew', (10, sub))
        elif type == 'trailer':
            print("trailer")
            cursor.callproc('dispatch_dev.trailerExpiredDocumentNew', (10, sub))
        elif type == 'dispatcher':
            print("dispatcher")
            cursor.callproc('dispatch_dev.dispatcherExpiredDocumentNew', (10, sub))
        procedureData = json.loads(json.dumps(dictfetchall(cursor), default=str))
        sorted_data = sorted(procedureData, key=lambda x: int(x['daysLeftToExp']))
        if cursor.rowcount != 0:
            loadsResponse = {
                "documents": sorted_data,
                "count": len(sorted_data)
            }
            return buildResponse(200, loadsResponse)
        else:
            loadsResponse = {
                "documents": []
            }
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(type, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(type, ex)})



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


def formatCountResponse(row):
    response = {'monthName': row[0], 'totalLoad': int(row[3])}
    # response[‘event’] = event
    return response

def formatCreatedProfileResponse(row):
    response = {'profileid': row[0], 
                'profilename': str(row[1]),
                'companyname': str(row[2]), 
                'ts_created': str(row[3]), 
                'ts_updated': str(row[4]), 
                'dot': str(row[5]), 
                'mc': str(row[6]), 
                'telephonenumber': str(row[7]), 
                'faxnumber': str(row[8]), 
                'address': str(row[9]), 
                'status': str(row[10]), 
                'sub': str(row[11]), 
                'profileurl': str(row[12]), 
                'userid': str(row[13]), 
                }
    return response

def buildResponse(statusCode, body=None):
    print("buildResponse method called with statusCode : {} and body : {}".format(statusCode, body))
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
