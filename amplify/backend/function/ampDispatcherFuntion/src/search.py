import json
from common import *


def searchDispatcher(connection, sub, limit, offset, bodyDetails):
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    print("searchCriteria : {}".format(body['searchCriteria']))
    searchQueryParameters = buildSearchQuery(body['searchCriteria'])
    print("searchQueryParameters : {}".format(searchQueryParameters))
    finalValues = searchQueryParameters['values'] + (sub, 'ACTIVE', int(limit), int(offset))
    print("finalValues : {}".format(finalValues))
    cursor.execute(searchQueryParameters['query'], finalValues)
    drivers = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                count = row[12]
                drivers.append(formatDispatcherSearchResponse(row))
            dispatcherResponse = {
                "totalDbCount": count,
                "dispatchers": drivers
            }
            return buildResponse(200, dispatcherResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No dispatcher record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all dispatcher details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all dispatcher details for sub : {}'
                             .format(sub)})


def buildSearchQuery(body):
    print(body)
    keyValueList = []
    baseQuery = 'select dispatcher_id,dispatcher_number,full_name,phone_number,email,address1,address2,state,city,country,zip_code,sub, count(*) OVER() AS count FROM dispatch_dev.dispatcher '
    # baseQuery = 'select * from dispatch_dev.dispatcher '
    values = ()
    dispatcherSearchAttributes = ["firstName", "lastName","fullName","dispatcherNumber", "email","phoneNumber","licenseNumber", "state"]
    for fieldName in dispatcherSearchAttributes:
        print(fieldName)
        print(fieldName in body)    
        if fieldName in body and fieldName != 'sub':
            keyValueList.append(camel2snake(fieldName) + ' like %s and ')
            values = values + (body[fieldName] + '%',)
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + 'where ' + ' '.join(
            keyValueList) + 'sub = %s and status =  %s order by dispatcher_id limit '
                            '%s offset %s',
        "values": values
    }

# ACCOUNTANT
def searchAccountant(connection, sub, limit, offset, bodyDetails):
    print("TEST_SEARCH_ACCOUNTANT")
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    print("searchCriteria : {}".format(body['searchCriteria']))
    searchQueryParameters = buildAccountantantSearchQuery(body['searchCriteria'])
    print("searchQueryParameters : {}".format(searchQueryParameters))
    finalValues = searchQueryParameters['values'] + (sub, 'ACTIVE', int(limit), int(offset))
    print("finalValues : {}".format(finalValues))
    print("2")
    cursor.execute(searchQueryParameters['query'], finalValues)
    accountant = []
    print("3")
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                accountant.append(formatAccountantSearchResponse(row))
            accountantResponse = {
                "count" :len(accountant),
                "accountants": accountant
            }
            return buildResponse(200, accountantResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No accountant record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all accountant details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all accountant details for sub : {}'
                             .format(sub)})


def buildAccountantantSearchQuery(body):
    print(body)
    keyValueList = []
    # baseQuery = 'select dispatcher_id,dispatcher_number,full_name,phone_number,email,address1,address2,state,city,country,zip_code,sub, count(*) OVER() AS count FROM dispatch_dev.dispatcher '
    baseQuery = 'select accountantId, first_name, full_name,phone_number,email,address1,address2,state,city,country,zip_code,sub, count(*) OVER() AS count FROM dispatch_dev.accountantdetails '
    # baseQuery = 'select accountantId, first_name, last_name, phone_number, email, birth_date, role, license_number, license_exp_date, address1, address2, state, city, country, full_name,phone_number,email,address1,address2,state,city,country,zip_code, ts_created, ts_updated, documents, sub, full_name, status, accountant_type, commission_percentage, monthly_salary, count(*) OVER() AS count FROM dispatch_dev.accountantdetails  '
    # baseQuery = 'select * from dispatch_dev.dispatcher '
    values = ()
    AccountantSearchAttributes = ["accountantId","first_name","full_name", "email","phone_number"]
    print("355")
    for fieldName in AccountantSearchAttributes:
        print(fieldName)
        print(fieldName in body)    
        if fieldName in body and fieldName != 'sub':
            keyValueList.append(fieldName + ' like %s and ')
            values = values + (body[fieldName] + '%',)
    print(',ccccc'.join(keyValueList))
    print("1",values)
    return {
        "query": baseQuery + 'where ' + ' '.join(
            keyValueList) + 'sub = %s and status =  %s order by accountantId limit '
                            '%s offset %s',
        "values": values
    }
