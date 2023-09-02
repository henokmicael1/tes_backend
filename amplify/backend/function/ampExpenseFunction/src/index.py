from ast import Pass
import datetime
import json
import boto3
import pymysql
from query import *
import re
from pydantic import ValidationError
import os
import boto3
import os


# from aws_lambda_powertools.utilities.parser import ValidationError, parse

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
healthPath = '/expense/health'


addDriverExpense = '/expense/addexpense'
deleteExpense = '/expense/deletetheexpense'
GetbyIdExpense = '/expense/getbyidexpense'
GetAllExpense = '/expense/getallexpense'
patchExpense = '/expense/patchexpense'
searchExpenses = '/expense/searchexpenses'
approvedorrejectexpense = '/expense/expenserejectorapproved'

def handler(event, context):
    # log.info("Request event : {}".format(event))
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")

    elif httpMethod == getMethod and path == GetAllExpense:
        response = getAllExpenses(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                                  event['queryStringParameters']['offset'])

    elif httpMethod == getMethod and path == GetbyIdExpense:
        response = getExpense(
            conn, event['queryStringParameters']['expenseid'], event['queryStringParameters']['sub'])

    elif httpMethod == postMethod and path == searchExpenses:
        response = getsearchedExpenses(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                                       event['queryStringParameters']['offset'], event['body'])

    elif httpMethod == postMethod and path == addDriverExpense:
        response = onexpense(conn, event['body'])
    
    elif httpMethod == patchMethod and path == approvedorrejectexpense:
        response = AcceptOrRejectTheExpenseByDispatcher(conn,
                           event['queryStringParameters']['sub'], event['body'])

    elif httpMethod == postMethod and path == deleteExpense:
        response = deleteExpensesFromRecord(conn, event['body'])

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
        conn = pymysql.connect(user=name,
                               password=password,
                               host=rds_host,
                               database=db_name,
                               cursorclass=pymysql.cursors.DictCursor)
        print("SUCCESS: Successfully connected to MySQL instance.")
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    return conn


def deleteExpensesFromRecord(connection, bodyDetails):
    print("CHECKDELETEEXPENSE")
    cursor = connection.cursor()
    try:
        bodyParam = json.loads(bodyDetails)
        expenseid = bodyParam['expenseid']
        status = bodyParam['status']
        sub = bodyParam['sub']
        if cursor.rowcount != 0:
            deleteExpense = "update dispatch_dev.expense SET status = %s where expenseId = %s and sub = %s"
            cursor.execute(deleteExpense, (status, expenseid, sub))
            loadsResponse = {
                "status": True
            }
            connection.commit()  # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching expense details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching expense details for sub : {}'
                             .format(sub)})


def onexpense(connection, bodyDetails):
    body = json.loads(bodyDetails)
    cursor = connection.cursor()
    expenses = body['expenses']  # Assuming 'expenses' is the key in the JSON body containing an array of expenses
    insertedExpenseIds = []  # List to store inserted expense IDs
    
    try:
        for expense in expenses:
            values = (
                body['driverFk'],
                expense['expensetype'],
                datetime.datetime.now(),
                datetime.datetime.now(),
                expense['cost'],
                expense['status'],
                expense['sub'],
                expense['type'],
            )
            cursor.execute(insertexpense, values)
            insertedExpenseIds.append(cursor.lastrowid)  # Append the inserted expense ID to the list
        
        print("Dataxxx", insertedExpenseIds)
    except pymysql.Error as ex:
        connection.rollback()
        connection.close()
        return buildResponse(500, {'Message': 'Exception occurred while saving expenses', 'Error': str(ex)})
    else:
        connection.commit()
        
        # Fetch all the inserted expenses
        cursor.execute("SELECT * FROM dispatch_dev.expense WHERE expenseId IN ({})".format(','.join(map(str, insertedExpenseIds))))
        results = []
        for row in cursor.fetchall():
            results.append(formatCreatedExpenseResponse(row))
        
        connection.close()
        return buildResponse(200, results)



def AcceptOrRejectTheExpenseByDispatcher(connection, sub, bodyDetails):
    cursor = connection.cursor()
    try:
       bodyParam = json.loads(bodyDetails)
       expenseIds = bodyParam['expenseIds']
       if cursor.rowcount != 0:
            if(bodyParam['choice'] == 'Accept'):
                for expenseId in expenseIds:
                    updateLoadStatus = "update dispatch_dev.expense SET status = 'ACCEPTED' where expenseId = %s and sub = %s"
                    cursor.execute(updateLoadStatus, (expenseId, sub))
                loadsResponse = {"Message":"Expense Accepted"}
            elif(bodyParam['choice'] == 'Reject'):
                for expenseId in expenseIds:
                    updateLoadStatus = "update dispatch_dev.expense SET status = 'REJECTED' where expenseId = %s and sub = %s"
                    cursor.execute(updateLoadStatus, (expenseId, sub))
                loadsResponse = {"Message":"Expense Rejected"}
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all expense details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all expense details for sub : {}'
                             .format(sub)})


def getExpense(connection, expenseid, sub):
    cursor = connection.cursor()
    cursor.execute(getexpensebyidquery, (expenseid, sub))
    try:
        if cursor.rowcount != 0:
            expnse = formatExpenseResponse(cursor.fetchone())
            return buildResponse(200, expnse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'ExpnseId: {} not found for sub: {}'.format(expenseid, sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while fetching expense details for expenseId : {expenseid} , Error: {ex}'})


def getAllExpenses(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getExpenseBySub, (sub, int(
        limit), (int(limit) * int(offset))))
    expenses = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                expenses.append(formatExpenseResponse(row))
            driversResponse = {
                "totalDbCount": getExpenseCount(connection, sub),
                "expenses": expenses
            }
            return buildResponse(200, driversResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No expense record found for sub : {}'.format(sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': 'Exception occurred while fetching all expense details for sub : {}'
                             .format(sub)})


def getsearchedExpenses(connection, sub, limit, offset, bodyDetails):
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    start_dete = body['start_date']
    end_dete = body['end_date']
    cursor.execute(getSearchedExpenseBySub, (sub, start_dete,
                   end_dete, int(limit), int(offset)))
    expenses = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                expenses.append(formatExpenseResponse(row))
            driversResponse = {
                "totalDbCount": getExpenseCount(connection, sub),
                "expenses": expenses
            }
            return buildResponse(200, driversResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No expense record found for sub : {}'.format(sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': 'Exception occurred while fetching all expense details for sub : {}'
                             .format(sub)})


def getExpenseCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getExpenseCountBySub, sub)
    return cursor.fetchone()['count']


def formatExpenseResponse(row):
    print("formatExpenseResponse = {}".format(row))
    response = {'expenseId': row['expenseId'],
                'driverFk': row['driverFk'],
                'driver_name': str(row['first_name']) + ' ' + str(row['last_name']),
                'expensetype': str(row['expensetype']),
                'ts_created': str(row['ts_created']),
                'ts_updated': str(row['ts_updated']),
                'cost': float(row['cost']),
                'status': str(row['status']),
                'sub': str(row['sub']),
                }
    # response[‘event’] = event
    return response


def formatCreatedExpenseResponse(row):
    print("formatCreatedExpenseResponse = {}".format(row))
    response = {'expenseId': row['expenseId'],
                'driverFk': str(row['driverFk']),
                'expensetype': str(row['expensetype']),
                'ts_created': str(row['ts_created']),
                'ts_updated': str(row['ts_updated']),
                'cost': str(row['cost']),
                'status': str(row['status']),
                'sub': str(row['sub']),
                'type': str(row['type']),
                }
    # response[‘event’] = event
    return response


def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])


def buildResponse(statusCode, body=None):
    print(body)
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
