import boto3
import secrets
import string
from botocore.exceptions import ClientError


def generateTempPass():
    alphabet = string.ascii_letters + string.digits + string.punctuation
    pwd_length = 12
    pwd = ''
    for i in range(pwd_length):
        pwd += ''.join(secrets.choice(alphabet))
    return pwd


# ACCOUNTANTS
def signUpAccountantCognito(body,insertedAccountantId):
    print("signUpAccountant")
    idp_client = boto3.client('cognito-idp')
    user_pool_id = 'us-east-1_xJnVekLO8'
    
    try:
        successResponse = idp_client.admin_create_user(
        DesiredDeliveryMediums=['EMAIL'],
        UserPoolId=user_pool_id,
        Username=body['email'],
        UserAttributes=[
            {
                'Name': 'email',
                'Value': body['email']
            }, {
                'Name': 'email_verified',
                'Value': 'false'
            }, {
                'Name': 'phone_number',
                'Value': body['phoneNumber']
            }, {
                'Name': 'phone_number_verified',
                'Value': 'false'
            },
            {
                'Name':'custom:role',
                'Value': 'Accountant'
            },
            {
                'Name':'custom:unique_id',
                'Value': str(insertedAccountantId)
            },
            {
                'Name':'custom:parent_sub',
                'Value': body['sub']
            },
             {
                'Name':'custom:first_name',
                'Value': body['firstName']
            }
        ],
        TemporaryPassword=generateTempPass()
        )
    except ClientError as ex:
        print("UsernameExistsException ".format(ex))
        error_message = ex.response['Error']['Message']
        print("error_message",error_message)
        if 'An account with the given email already exists' in error_message:
            error_message = 'An account with the given email already exists'
        elif 'Invalid phone number format.' in error_message:
            error_message = 'Invalid phone number format.'
        else:
            error_message = str(ex)

        response = {
            'statusCode': 400,
            'body': {
                "message": error_message
            }
        }
        
        return response
    
    print(successResponse)
    response = {
            'statusCode': 200,
            'body': successResponse
        }
   
    return response



def deleteAccountantCognito(body):
    idp_client = boto3.client('cognito-idp')
    user_pool_id = 'us-east-1_xJnVekLO8'
    print(body['email'])

    response = idp_client.admin_delete_user(
        UserPoolId=user_pool_id,
        Username=body['email'],
    )
    print(response)
    return response