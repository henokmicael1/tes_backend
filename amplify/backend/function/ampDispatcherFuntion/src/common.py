import re
from query import getDispatcherCountBySub
import json


def formatDispatcherResponse(row):
    response = {'dispatcherId': row[0], 
                'firstName': str(row[1]), 
                'lastName': str(row[2]), 
                'phoneNumber': str(row[3]), 
                'email': str(row[4]),
                'birthDate': str(row[5]), 
                'role': str(row[6]), 
                'licenseNumber': str(row[7]),
                'licenseExpDate': str(row[8]), 
                'address1': str(row[9]), 
                'address2': str(row[10]),
                'state': str(row[11]), 
                'city': str(row[12]), 
                'country': str(row[13]),
                'zipCode': str(row[14]), 
                'tsCreated': str(row[15]), 
                'tsUpdated': str(row[16]),
                'status': str(row[17]), 
                'sub': str(row[18]),
                'dispatcherNumber': str(row[19]),
                'fullName': str(row[20]),
                'dispatcher_type': str(row[21]),
                'commission_percentage': str(row[22]),
                'monthly_salary': str(row[23]),
                'documents': json.loads(row[24]) if row[24] else []
                }
    # response[‘event’] = event
    return response





def formatDispatcherSearchResponse(row):
    response = {'dispatcherId': row[0],
                'dispatcherNumber': str(row[1]), 
                'fullName': str(row[2]),
                'phoneNumber': str(row[3]), 
                'email': str(row[4]),
                'address1': str(row[5]), 
                'address2': str(row[6]),
                'state': str(row[7]), 
                'city': str(row[8]), 
                'country': str(row[9]),
                'zipCode': str(row[10]), 
                'sub': str(row[11])}
    return response



def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])


def getDispatchersCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getDispatcherCountBySub, sub)
    return cursor.fetchone()[0]


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
