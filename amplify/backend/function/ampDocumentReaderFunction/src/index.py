import json
import boto3
import pymysql
from query import *
import pdfplumber
import openai
import json
import re
import ast
import time
import os
import re
import datetime

db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

healthPath = '/documentReader/health'
getMethod = 'GET'
postMethod = 'POST'
putMethod = 'PUT'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
readPdfDocument = '/documentReader/extractpdfdata'


def handler(event, context):
    print('received event:')
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")

    elif httpMethod == postMethod and path == readPdfDocument:
        response = convertPdfToText(conn, event['body'])

    else:
        buildResponse(404, 'Path Not Found')
    return response


def convertPdfToText(connection, bodyDetails):
    s3 = boto3.client('s3')
    bucket_name = 'prashantbuckettest'
    bodyParam = json.loads(bodyDetails)
    s3_url = bodyParam['s3_url']
    file_name = s3_url.split('.com/')[-1]
    tempfilename = os.path.basename(s3_url)
    API_KEY = db_secret_dict['chatGptApiKey']
    model = db_secret_dict['model']

    openai.api_key = API_KEY

    tempFile = f'/tmp/{tempfilename}'
    try: 
        with open(tempFile, 'wb') as f:
            s3.download_fileobj(bucket_name, file_name, f)
        with pdfplumber.open(tempFile) as pdf:
            all_text = ''
            for page in pdf.pages:
                text = page.extract_text()
                all_text += text
        pdf.close()
        os.remove(tempFile)

        # Helper function to convert character stopNumber to number
        def map_stop_number(character, stop_numbers):
            if character in stop_numbers:
                return stop_numbers[character]
            else:
                stop_numbers[character] = len(stop_numbers) + 1
                return stop_numbers[character]

        # Initialize stop_number to a default value
        stop_number = 1

        # Extracting stopNumber from the text
        stop_numbers = {}
        if "stopNumber" in all_text:
            stop_number_text = all_text.split("stopNumber")[-1].split()[0]
            if stop_number_text.isdigit():
                stop_number = int(stop_number_text)
            else:
                stop_number = map_stop_number(stop_number_text, stop_numbers)

        prompt = """Analyze the following input and return load details from input.
        Your response should be in JSON format with parameters {'loadNumber','rate', 'brokerName': 'name',
        'address':[{'stopNumber':""" + str(stop_number) + """,'locationType':(only possible values 'pickup' or dropoff),'address1', 'address2','city','state','zipCode','country','phone','stopDate','stopTime'}]}. \n\n""" + all_text

        response = openai.ChatCompletion.create(
            model=model,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}]
        )
        jsonResponse = json.loads(response.choices[0]["message"]["content"])
        ratex = jsonResponse["rate"]
        if "USD" in ratex or "$" in ratex:
            ratex_float = float(ratex.replace("$", "").replace("USD", "").replace(",", ""))
            print("OpenAI response:", ratex_float)
            jsonResponse["rate"] = ratex_float
        elif ratex == '':
            jsonResponse["rate"] = 0
        else:
            jsonResponse["rate"] = 0
        print("jsonResponse", jsonResponse)
        loadsResponse = {
            "Message": "SUCCESS",
            "data": jsonResponse,
        }
        return buildResponse(200, loadsResponse)
    except Exception as ex:
        errorResponse = {
            "Message": "ERROR",
            "Error": str(ex)
        }
        return buildResponse(500, errorResponse)


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


def buildResponse(statusCode, body=None):
    print("buildResponse method called with statusCode : {} and body : {}".format(
        statusCode, body))
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
