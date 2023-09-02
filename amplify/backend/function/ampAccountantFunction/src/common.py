import re
from query import *
import json

def formatAccountantsResponse(row):
    response = {'accountantId': row[0], 
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
                'documents': json.loads(row[17]),
                'sub': str(row[18]),
                'fullName': str(row[19]),
                'status': str(row[20]), 
                'accountantType': str(row[21]), 
                'commissionPercentage': str(row[22]), 
                'monthlySalary': str(row[23]), 
                }
    # response[‘event’] = event
    return response

def formatAccountantSearchResponse(row):
    response = {'accountantId': row[0],
                'firstName': str(row[1]), 
                'fullName': str(row[2]), 
                'phoneNumber': str(row[3]), 
                'email': str(row[4]),
                'address1': str(row[5]), 
                'address2': str(row[6]),
                # 'lastName': str(row[2]), 
                # 'birthDate': str(row[5]), 
                # 'role': str(row[6]), 
                # 'licenseNumber': str(row[7]),
                # 'licenseExpDate': str(row[8]), 
                # 'address1': str(row[9]), 
                # 'address2': str(row[10]),
                # 'sub': str(row[11])
                
                }
    return response

def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])




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

