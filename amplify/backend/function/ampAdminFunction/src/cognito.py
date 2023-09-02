import json
import boto3
import re


def updateUserInCognito(admin_id, **kwargs):
    try:
        # Check if any Cognito relevant fields are updated
        print("test")
        cognito_fields = ['name', 'firstName', 'lastName', 'phoneNumber', 'email', 'profileurl']
        updated_fields = [field for field in kwargs if field in cognito_fields]

        if 'role' in kwargs:
            updated_fields.append('custom:role')

        if updated_fields:
            # Initialize the Cognito client
            cognito_client = boto3.client('cognito-idp')

            # Prepare the attribute updates
            attribute_updates = []
            for field, value in kwargs.items():
                if field in updated_fields or field == 'profileurl':
                    attribute_name = camel2snake(field)  # Convert to snake_case

                    # Check if it is the custom 'role' or 'profileUrl' attribute
                    if field == 'role':
                        attribute_name = 'custom:role'
                    elif field == 'profileurl':
                        attribute_name = 'custom:profileurl'

                    if field == 'firstName':
                        attribute_updates.append({
                            'Name': 'given_name',
                            'Value': value
                        })
                    elif field == 'lastName':
                        attribute_updates.append({
                            'Name': 'family_name',
                            'Value': value
                        })
                    else:
                        attribute_updates.append({
                            'Name': attribute_name,
                            'Value': value
                        })

            # Update the user in Cognito
            cognito_client.admin_update_user_attributes(
                UserPoolId='us-east-1_xJnVekLO8',
                Username=admin_id,
                UserAttributes=attribute_updates
            )

            # Return success message as JSON
            return buildResponse(200, {'success': True, 'message': 'User attributes updated successfully', 'profileurl': kwargs.get('profileurl')})
        else:
            # No Cognito relevant fields are updated, skip Cognito update
            return buildResponse(200, {'success': False, 'message': 'No Cognito relevant fields provided'})
    except Exception as e:
        error_message = f"Failed to update user in Cognito: {str(e)}"
        error_response = {'success': False, 'message': error_message}
        print("Error:", error_response)
        return buildResponse(500, error_response)


def getUserDetailsFromCognito(user_id):
    try:
        print("getUserDetailsFromCognito for sub:", user_id)
        # Initialize the Cognito client
        cognito_client = boto3.client('cognito-idp')

        # Get the user details from Cognito
        response = cognito_client.admin_get_user(
            UserPoolId='us-east-1_xJnVekLO8',
            Username=user_id
        )

        # Extract and return the user attributes
        user_attributes = response['UserAttributes']
        # Print user attributes for debugging
        print("User attributes:", user_attributes)
        user_details = {
            'name': None,
            'firstName': None,
            'lastName': None,
            'phoneNumber': None,
            'email': None,
            'role': None,
            'profileurl': None,
        }
        for attribute in user_attributes:
            attribute_name = attribute['Name']
            attribute_value = attribute['Value']
            if attribute_name == 'name':
                user_details['name'] = attribute_value
            elif attribute_name == 'given_name':
                user_details['firstName'] = attribute_value
            elif attribute_name == 'family_name':
                user_details['lastName'] = attribute_value
            elif attribute_name == 'phone_number':
                user_details['phoneNumber'] = attribute_value
            elif attribute_name == 'email':
                user_details['email'] = attribute_value
            elif attribute_name == 'custom:role':
                user_details['role'] = attribute_value
            elif attribute_name == 'custom:profileurl':
                user_details['profileurl'] = attribute_value

        # Print user details for debugging
        print("User details:", user_details)

        return buildResponse(200, user_details)

    except Exception as e:
        error_message = f"Failed to get user details from Cognito: {str(e)}"
        error_response = {'success': False, 'message': error_message}
        print("Error:", error_response)
        return buildResponse(500, error_response)

def camel2snake(name):
    return re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name)


def snake2camel(name):
    return re.sub(r'_([a-z])', lambda x: x.group(1).upper(), name)


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
