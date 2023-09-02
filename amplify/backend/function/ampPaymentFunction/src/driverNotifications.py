import requests
import boto3
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])



def send(available_device_tokens, message_title, message_desc):
    # DISPATCH
    fcm_key = db_secret_dict['fcm_key']
    url = db_secret_dict['fcm_url']

    headers = {
        "Content-Type": "application/json",
        "Authorization": 'key='+fcm_key}

    payload = {
        "registration_ids": available_device_tokens,
        "priority": "high",
        "notification": {
            "body": message_desc,
            "title": message_title,
            "image": "https://prashantbuckettest.s3.us-east-1.amazonaws.com/public/1677824397652truckerDispatchingManagement-logo.png",
            "icon": "https://prashantbuckettest.s3.us-east-1.amazonaws.com/public/1677824397652truckerDispatchingManagement-logo.png",

        }
    }

    result = requests.post(url,  data=json.dumps(payload), headers=headers)
    print("Success", result.json())


def send_notifications(message):
    print("Sending notification", message)
    getMessagetitle = message['getMessagetitle']
    getMessagebody = message['getMessagebody']
    users_device_tokens = message['userToken']
    device_tokens = [users_device_tokens]
    send(device_tokens, getMessagetitle, getMessagebody)
    message = "Notification sent successfully."
    print("TESTMESSAGE")


