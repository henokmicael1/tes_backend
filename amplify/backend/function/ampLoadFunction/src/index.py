from ast import Pass
import datetime
import json
import boto3
import pymysql
from query import *
from generatepdf import *
from driverNotifications import *
from load import Load
import re
from pydantic import ValidationError
import os
from fpdf import FPDF
import requests
import boto3
import os
from fpdf import FPDF, HTMLMixin
import boto3
from string import Template
from jinja2 import Template
import io
import PyPDF2
import uuid
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
healthPath = '/load/health'
# searchLoadPath = '/load/search'
loadTableSearch = '/load/tableViewSearch'
driverActiveLoadPath = '/load/getalldriverloads'
driverStartAndEndPath = '/load/startandendtrip'
acceptOrRejectTheLoad = '/load/acceptorrejectload'
AcceptOrRejectTheLoadBOL = '/load/acceptorrejectloadbol'
uploadBOL = '/load/uploadbol'
assignLoadToDriveronPortal = '/load/assignload'
getPayments = '/load/getpaymentsofload'
getAllLoadsDriver = '/load/getdriverloads'
getallinvoices = '/load/invoice/getallinvoices'
getallinvoicesbyidandsub = '/load/invoice/getinvoice'
# historyPath = '/load/history'

factoringTheLoads = '/load/invoice/factoringinvoice'
saveDeviceTokensForSendingNotifications = '/load/device/savedevicefcmtokens'
getDriverNotifcations = '/load/driver/drivernotifications'
sendInvoiceToEmail = '/load/sendmail'
uploadDocumentsApi = '/load/uploaddoc'
removeFactoringFromLoads = '/load/invoice/removefactoredinvoice'
updateTheLoadStatusFromTheList = '/load/loadstatusupdate'

def handler(event, context):
    # log.info("Request event : {}".format(event))
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    print(path)

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")
    elif httpMethod == postMethod and path == loadTableSearch:
        response = loadsTableViewSearch(conn, event['queryStringParameters']['sub'],
                                         event['queryStringParameters']['limit'],
                                         event['queryStringParameters']['offset'], event['body'])
    elif httpMethod == getMethod and path == getallinvoices:
        response = getAllLoadsGeneratedInvoices(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['filter_search'], event['queryStringParameters']['limit'],
                            event['queryStringParameters']['offset'])

    elif httpMethod == getMethod and path == getallinvoicesbyidandsub:
        response = getinvoicebysubanditsid(conn, event['queryStringParameters']['invoiceId'], event['queryStringParameters']['sub'])
    

    elif httpMethod == getMethod and 'loadId' in event['queryStringParameters']:
        response = getLoad(conn, event['queryStringParameters']['loadId'],
                           event['queryStringParameters']['sub'])
    elif httpMethod == getMethod and 'limit' in event['queryStringParameters']:
        response = getLoads(conn, event['queryStringParameters']['sub'], event['queryStringParameters']['limit'],
                            event['queryStringParameters']['offset'])

    
    elif httpMethod == getMethod and path == driverActiveLoadPath:
        response = driverActiveLoad(conn, event['queryStringParameters']['driverid'], event['queryStringParameters']['sub'], event['queryStringParameters']['status'])

    elif httpMethod == getMethod and path == getAllLoadsDriver:
        response = getPerticularDriversLoads(conn, event['queryStringParameters']['driverid'], event['queryStringParameters']['sub'], event['queryStringParameters']['load_number'])

    elif httpMethod == getMethod and path == getDriverNotifcations:
        response = getNotifications(conn, event['queryStringParameters']['driverid'], event['queryStringParameters']['sub'])

    elif httpMethod == patchMethod and path == driverStartAndEndPath:
        response = driverStartOrEndTrip(conn, event['queryStringParameters']['loadId'],
                           event['queryStringParameters']['sub'], event['body'])

    elif httpMethod == patchMethod and path == acceptOrRejectTheLoad:
        response = AcceptOrRejectTheLoad(conn, event['queryStringParameters']['loadId'],
                           event['queryStringParameters']['sub'], event['body'])

    elif httpMethod == patchMethod and path == AcceptOrRejectTheLoadBOL:
        response = AcceptOrRejectTheLoadBOLByDispatcher(conn, event['queryStringParameters']['loadId'],
                           event['queryStringParameters']['sub'], event['body'])

    elif httpMethod == patchMethod and path == uploadBOL:
        response = uploadBOLbyDispatcher(conn, event['queryStringParameters']['loadId'],
                           event['queryStringParameters']['sub'], event['body'])


    elif httpMethod == patchMethod and path == assignLoadToDriveronPortal:
        response = assignLoadToDriver(conn, event['queryStringParameters']['loadId'],
                           event['queryStringParameters']['sub'], event['body'])

    elif httpMethod == postMethod and path == getPayments:
        response = getLoadsPayments(conn, event['queryStringParameters']['limit'],
                            event['queryStringParameters']['offset'], event['body'])

    elif httpMethod == postMethod and path == factoringTheLoads:
        response = factoringTheLoadInvoice(conn, event['body'])
        
    elif httpMethod == postMethod and path == saveDeviceTokensForSendingNotifications:
        response = saveDeviceTokens(conn, event['body'])

    elif httpMethod == postMethod and path == sendInvoiceToEmail:
        response = sendInvoices(conn, event['body'])

    elif httpMethod == postMethod and path == uploadDocumentsApi:
        response = uploadFiles(conn, event['body'])

    elif httpMethod == postMethod and path == removeFactoringFromLoads:
        response = removeFactored(conn, event['body'])

    elif httpMethod == postMethod and path == updateTheLoadStatusFromTheList:
        response = updateTheLoadStatusList(conn, event['body'])

    elif httpMethod == postMethod:
        try:
            load = Load(**json.loads(event['body']))
            response = onboardLoad(conn, event['body'])
        except ValidationError as ex:
            # log.error(ex)
            print("Validation error , error = {}".format(ex))
            response = buildResponse(400, json.loads(ex.json()))
    elif httpMethod == patchMethod:
        response = updateLoad(conn, event['body'])
    elif httpMethod == deleteMethod:
        response = deleteLoad(conn, event['queryStringParameters']['loadId'],
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


def getLoad(connection, loadId, sub):
    cursor = connection.cursor()
    cursor.execute(getLoadByIdAndSub, (loadId, sub))
    try:
        if cursor.rowcount != 0:
            load = formatLoadResponse(cursor.fetchone())
            print(load)
            
            # truck = formatTruckResponse(cursor.fetchone())
            print("Driver 1 = {} and sub = {}".format(load['driver1Fk'], load['sub']))
            
            load['drivers'] = []
            load['trailer'] = None
            load['dispatcher'] = None
            # load['broker'] = None            
            
            if load['driver1Fk'] is not None:
                cursor.execute(getDriverByIdAndSub, (load['driver1Fk'], load['sub']))
                load['drivers'].append(formatDriverResponse(cursor.fetchone(), "primary"))
                print("driver1Fk".format(load['driver1Fk']))
            
            print("driver2Fk {}".format(load['driver2Fk']))
            if load['driver2Fk'] is not None:
                cursor.execute(getDriverByIdAndSub, (load['driver2Fk'], load['sub']))
                load['drivers'].append(formatDriverResponse(cursor.fetchone(), "secondary"))
                print("driver2Fk".format(load['driver2Fk']))
            
            # load['drivers'] = drivers
            
            
            if load['dispatcherFk'] is not None:
                cursor.execute(getDispatcherByIdAndSub, (load['dispatcherFk'], load['sub']))
                load['dispatcher'] = formatDispatcherResponse(cursor.fetchone())
                load['dispatcherId'] = load['dispatcherFk']
                print("dispatcherFk".format(load['dispatcherFk']))
                
            # if load['brokerFk'] is not None:
            #     cursor.execute(getBrokerByIdAndSub, (load['brokerFk'], load['sub']))
            #     load['broker'] = formatBrokerResponse(cursor.fetchone())
            #     print("brokerFk".format(load['brokerFk']))

            if load['trailerFk'] is not None:
                cursor.execute(getTrailerById, (load['trailerFk'], load['sub']))
                load['trailer'] = formatTrailerResponse(cursor.fetchone())
                load['trailorId'] = load['trailerFk']
            del load['driver1Fk']
            del load['driver2Fk']
            del load['trailerFk']
            del load['dispatcherFk']
            # del load['brokerFk']

            return buildResponse(200, load)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'LoadId: {} not found for sub: {}'.format(loadId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while fetching load details for loadId:{} and sub : {}".format(loadId,
                                                                                               sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching load details for '
                                              'loadId: {} and sub:{}'.format(loadId, sub), 'Error': ex})


def getLoads(connection, sub, limit, offset):
    cursor = connection.cursor()
    cursor.execute(getLoadBySub, (sub, int(limit),  (int(limit) * int(offset))))
    loads = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                loads.append(formatLoadResponse(row))
            loadsResponse = {
                "totalDbCount": getLoadsCount(connection, sub),
                "loads": loads
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No load record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})


def getPerticularDriversLoads(connection, driverid, sub, load_number=None):
    cursor = connection.cursor()
    if load_number:
        cursor.execute("SELECT * FROM dispatch_dev.load WHERE driver1_fk = %s AND sub = %s AND status != 'DELETED' AND load_number LIKE %s ORDER BY load_id", (driverid, sub, f'%{load_number}%'))
    else:
        cursor.execute(getLoadByDriverIdAndSub, (driverid, sub))
    loads = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                loads.append(formatLoadResponse(row))
            loadsResponse = {
                "loads": loads
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No load record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})

def getNotifications(connection, driverid, sub):
    print("TESTING__NOTI__GET__API")
    cursor = connection.cursor()
    cursor.execute(getNotificationDriverIdAndSub, (driverid, sub))
    loads = []  # Initialize loads as an empty list
    payments = []
    try:
        for row in cursor:
            notification = formatNotificationResponse(row)
            loads.append(notification)

        # Fetch payments with status 'DRAFT' for the given driverid and sub
        payment_query = "SELECT * FROM dispatch_dev.payments WHERE status = 'DRAFT' AND driver1_fk = %s AND sub = %s"
        payment_cursor = connection.cursor()  # Create a new cursor for payment query
        payment_cursor.execute(payment_query, (driverid, sub))
        payment_rows = payment_cursor.fetchall()
        # print("payment_rows", payment_rows)
        for payment_row in payment_rows:
            payment = {
                "paymentId": payment_row['payment_id'],
                "payment_start_date": payment_row['payment_start_date'],
                "payment_end_date": payment_row['payment_end_date'],
                "status": payment_row['status'],
                "amount": float(payment_row['driver_net_settelment']),
                "paymentUrl": payment_row['payment_url'],
                'message': 'Hey there, You have a new route settlement details',
                'notificationtype': 'settlement'
            }
            payments.append(payment)
        
        # Get the count of loads and payments
        loads_count = len(loads)
        payments_count = len(payments)
        total_count = loads_count + payments_count
        notifications_response = {
            "loads": loads,
            "payments": payments,
            "total_count": total_count
        }
        return buildResponse(200, notifications_response)

    except Exception as ex:
        print("Exception occurred while fetching all load and payments details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load and payments details for sub : {}'
                             .format(sub)})



# This endpoint might need to remove in future.
def getLoadsPayments(connection, limit, offset, bodyDetails):
    bodyParam = json.loads(bodyDetails)
    bolUrl = bodyParam['sub']
    start_date = bodyParam['start_date']
    end_date = bodyParam['end_date']
    cursor = connection.cursor()
    if start_date and end_date:
        cursor.execute(getLoadBySubPaymentdatewise, (bolUrl,start_date,end_date, int(limit),  (int(limit) * int(offset))))
    else:
        cursor.execute(getLoadBySubPayment, (bolUrl, int(limit),  (int(limit) * int(offset))))
    loads = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                loads.append(formatPaymentResponse(row))
            for data in loads:
                linkedDispatcherId = data['dispatcherFk']
                linkedDispatcherSub = data['sub']
                loadRate = data['rate']
                if linkedDispatcherId is not None:
                    cursor.execute(getDispatcherPaymentsByIdAndSub, (linkedDispatcherId, linkedDispatcherSub))
                    dispatchers = []
                    for dispatcher_row in cursor:
                        commission_percentage = dispatcher_row['commission_percentage']
                        monthly_salary = dispatcher_row['monthly_salary']
                        if commission_percentage == None and monthly_salary == None:
                            dispatcher_row['commission_percentage'] = '0'
                            dispatcher_row['monthly_salary'] = '0' 
                        else:
                            dispatcher_row['commission_percentage'] = commission_percentage
                            dispatcher_row['monthly_salary'] = monthly_salary
                            if commission_percentage != 0:
                                commission_percentage_float = float(commission_percentage)
                                loadRateCommissionForAdmin = loadRate / commission_percentage_float
                                data['loadRateCommissionForAdmin'] = loadRateCommissionForAdmin
                        dispatchers.append(formatPaymentDispatcherResponse(dispatcher_row))
                    data['dispatchers'] = dispatchers
                else:
                    data['dispatchers'] = []
            loadsResponse = {
                "totalDbCount": getLoadsCount(connection, bolUrl),
                "loads": loads
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No load record found for sub : {}'.format(bolUrl)})
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(bolUrl, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(bolUrl)})

def formatPaymentResponse(row):
    print("formatPaymentResponse = {}".format(row))
    print("formatPaymentResponse>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> = {}".format(row))
    response = {'loadId': row['load_id'], 
                'loadNumber': str(row['load_number']), 
                'estDriveTime': str(row['est_drive_time']),
                'rate': float(row['rate']), 
                'detention': float(row['detention']),
                'bolPath': str(row['bol_path']), 
                'truckFk': row['truck_fk'], 
                'dispatcherFk': row['dispatcher_fk'],
                'brokerName': row['broker_name'], 
                'trailerFk': row['trailer_fk'], 
                'driver1Fk': row['driver1_fk'], 
                'driver2Fk': row['driver2_fk'],
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']), 
                'status': str(row['status']), 
                'sub': str(row['sub'])
                }
    # response[‘event’] = event
    return response

# Driver start and end date.
def driverStartOrEndTrip(connection, loadId, sub, bodyDetails):
    cursor = connection.cursor()
    cursor.execute(getLoadByIdAndSub, (loadId, sub))
    try:
       bodyParam = json.loads(bodyDetails)
       bolUrl = bodyParam['bol_path']
       if cursor.rowcount != 0:
            load_data = cursor.fetchone()
            driverId = load_data['driver1_fk']
            print("CHECKING DRIVER ID", driverId)
            if bodyParam['trip'] == 'started':
                # Check if the driver has any other load with status 'IN TRANSIT'
                checkStartedLoadQuery = "SELECT * FROM dispatch_dev.load WHERE driver1_fk = %s AND status = 'IN TRANSIT' AND sub = %s"
                cursor.execute(checkStartedLoadQuery, (driverId, sub))
                if cursor.rowcount != 0:
                    return buildResponse(400, {'Message': 'You already have another load in progress. You cannot start a different load.'})
                else:
                    updateLoadStatus = "update dispatch_dev.load SET status = 'IN TRANSIT', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                    cursor.execute(updateLoadStatus, (loadId, sub))
                    loadsResponse = {"Message": "Trip started successfully"}
                    # return buildResponse(200, loadsResponse)
            elif(bodyParam['trip'] == 'completed') and bolUrl=='':
                updateLoadStatus = "update dispatch_dev.load SET status = 'BOL PENDING', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateLoadStatus, (loadId, sub))
                loadsResponse = {"Message":"Trip completed successfully without Bill of loading"}
            elif(bodyParam['trip'] == 'completed'):
                updateLoadStatus = "update dispatch_dev.load SET status = 'READY FOR REVIEW', bol_path = %s, ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateLoadStatus, (bolUrl, loadId, sub))

                
                def generate_pdf(image_url):
                    print("GENERATED_INVOICE_RESPONSE_TEST1")
                    response = requests.get(image_url)
                    image_data = response.content
                    # Create the PDF object
                    pdf = FPDF()
                    pdf.add_page()
                    filename, extension = os.path.splitext(image_url)
                    tempImg = f"/tmp/image{loadId}{extension}"
                    print("temp", tempImg)
                    with open(tempImg, 'wb') as f:
                        f.write(image_data)
                    # FIT THE IMAGE TO SCREEN.
                    with open(tempImg, 'rb') as f:
                        img_data = f.read()
                        img_width = pdf.w - 2*pdf.l_margin
                        img_height = pdf.h - 2*pdf.t_margin
                        pdf.image(tempImg, x=pdf.l_margin, y=pdf.t_margin, w=img_width, h=img_height)
                    # pdf.image(tempImg)
                    
                    # Save the PDF file to disk
                    file_name = os.path.basename(filename) + ".pdf"
                    print(file_name)
                    file_path = "/tmp/" + file_name
                    print(file_path)
                    pdf.output(file_path)
                    return file_path, file_name




                def upload_to_s3(bucket_name, file_path, aws_access_key_id, aws_secret_access_key):
                    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
                    object_name = os.path.basename(file_path)
                    s3_key = 'public/' + object_name
                    with open(file_path, "rb") as f:
                        s3.upload_fileobj(f, bucket_name, s3_key)
                    presigned_url = s3.generate_presigned_url(
                        ClientMethod="get_object",
                        Params={"Bucket": bucket_name, "Key": s3_key},
                        ExpiresIn=0,  
                    )
                    return presigned_url

                image_url = bolUrl
                pdf_file_path, pdf_file_name = generate_pdf(image_url)
                bucket_name = "prashantbuckettest"
                aws_access_key_id = "AKIAQPUIZQ3OQ5OTUOVG"
                aws_secret_access_key = "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"
                presigned_url = upload_to_s3(bucket_name, pdf_file_path, aws_access_key_id, aws_secret_access_key)
                pdf_urlx = presigned_url.split('.pdf')[0] + '.pdf'
                print(pdf_urlx)

                print("PDF file name:", pdf_file_name)
                print("S3 URL:", presigned_url)
                print("S3 URL:", pdf_urlx)
                os.remove(pdf_file_path)
                updateLoadUrl = "update dispatch_dev.load SET bol_path = %s where load_id = %s and sub = %s"
                cursor.execute(updateLoadUrl, (pdf_urlx, loadId, sub))

                loadsResponse = {"Message":"Trip completed successfully with Bol"}
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})

# Driver accept or reject the Load.
def AcceptOrRejectTheLoad(connection, loadId, sub, bodyDetails):
    cursor = connection.cursor()
    cursor.execute(getLoadByIdAndSub, (loadId, sub))
    try:
       bodyParam = json.loads(bodyDetails)
       if cursor.rowcount != 0:
            if(bodyParam['choice'] == 'Accept'):
                updateLoadStatus = "update dispatch_dev.load SET status = 'ACCEPTED', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateLoadStatus, (loadId, sub))
                loadsResponse = {"Message":"Load Accepted"}
            elif(bodyParam['choice'] == 'Reject'):
                updateLoadStatus = "update dispatch_dev.load SET status = 'REJECTED', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateLoadStatus, (loadId, sub))
                loadsResponse = {"Message":"Load Rejected"}
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})


# Dispatcher will accept or reject the BOL.
def AcceptOrRejectTheLoadBOLByDispatcher(connection, loadId, sub, bodyDetails):
    cursor = connection.cursor()
    cursor.execute(getLoadByIdAndSub, (loadId, sub))
    try:
       bodyParam = json.loads(bodyDetails)
       if cursor.rowcount != 0:
            if(bodyParam['choice'] == 'Accept'):
                # updateLoadStatus = "update dispatch_dev.load SET status = 'READY FOR INVOICE' where load_id = %s and sub = %s"
                invoiceRes = generateInvoicePaymentsLoads(connection, loadId, sub)
                print("INVOICERESPONSE", invoiceRes)
                return invoiceRes
                # updateLoadStatus = "update dispatch_dev.load SET status = 'INVOICED', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                # cursor.execute(updateLoadStatus, (loadId, sub))
                # loadsResponse = {"Message":"BOL Accepted"}
            elif(bodyParam['choice'] == 'Reject'):
                updateLoadStatus = "update dispatch_dev.load SET status = 'BOL PENDING', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateLoadStatus, (loadId, sub))
                loadsResponse = {"Message":"BOL Rejected"}
                connection.commit() # commit the changes to the database
                return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})


def uploadBOLbyDispatcher(connection, loadId, sub, bodyDetails):
    print("testinguploadbolapis", uploadBOLbyDispatcher)
    cursor = connection.cursor()
    cursor.execute(getLoadByIdAndSub, (loadId, sub))
    try:
       bodyParam = json.loads(bodyDetails)
       bolUrl = bodyParam['bolPath']
       print("bolUrlbolUrlbolUrlbolUrl", bolUrl)
       if cursor.rowcount != 0:
            if(bolUrl):
                # updateLoadStatus = "update dispatch_dev.load SET status = 'INVOICED', bol_path = %s, ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                # cursor.execute(updateLoadStatus, (bolUrl, loadId, sub))
                # loadsResponse = {"Message":"BOL Uploaded"}
                invoiceRes = generateInvoicePaymentsLoads(connection, loadId, sub)
                print("INVOICERESPONSE", invoiceRes)
                return invoiceRes
            # connection.commit() # commit the changes to the database
            # return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})

def assignLoadToDriver(connection, loadId, sub, bodyDetails):
    print("assignLoadToDriver")
    cursor = connection.cursor()
    cursor.execute(getLoadByIdAndSub, (loadId, sub))
    try:
       bodyParam = json.loads(bodyDetails)
       truck = bodyParam['truckFk']
       driver1 = bodyParam['driver1Fk']
       driver2 = bodyParam['driver2Fk']
       trailor = bodyParam['trailerFk']

       if cursor.rowcount != 0:
            assignedLoadToDriver = "update dispatch_dev.load SET truck_fk = %s, driver1_fk = %s, driver2_fk = %s, trailer_fk = %s, status = 'ASSIGNED', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
            cursor.execute(assignedLoadToDriver, (truck, driver1, driver2, trailor, loadId, sub))
            loadsResponse = {"Message":"Load assigned to driver"}

            # Trigger Notifications
            cursor.execute("SELECT * FROM dispatch_dev.load WHERE load_id = %s AND sub = %s", (loadId, sub,))
            row = cursor.fetchone()
            if row:
                loadNumber = row['load_number']
            else:
                loadNumber = ''
            cursor.execute("SELECT * FROM dispatch_dev.user_device_tokens WHERE user_id = %s AND sub = %s", (driver1, sub,))
            row = cursor.fetchone()
            print("ARGUMENTS__ROW", row)
            if row:
                deviceToken = row['device_token']
            else:
                deviceToken = ''
            print("TRIGGERING NOTIFICATIONS")
            message = {}
            message['loadId'] = loadId
            message['getMessagetitle'] = 'New Load has been assigned to you'
            message['getMessagebody'] = loadNumber
            message['userToken'] = deviceToken
            send_notifications(message)
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})




def generateInvoicePaymentsLoads(connection, loadId, sub):
    try:
        cursor = connection.cursor()
        insertedLoadDetails = getLoad(connection, loadId, sub)
        objToString = json.dumps(insertedLoadDetails)
        response_dict = json.loads(objToString)['body']
        generateInvoiceLoad = response_dict
        data_dict = json.loads(generateInvoiceLoad)
    except pymysql.Error as ex:
        connection.rollback()
        connection.close()
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving invoice', 'Error': ex})
    else:
        sub = data_dict['sub']
        dispatcherId = data_dict['dispatcherId']
        cursor.execute("SELECT * FROM dispatch_dev.factor WHERE sub = %s", (sub,))
        row = cursor.fetchone()
        if row:
            data_dict['companyname'] = row['name']
            data_dict['companyaddress'] = row['address1']
            if not data_dict['companyaddress']:
                response = {
                    'Message': 'Profile address is not found for this user',
                    'Error': 'ProfileError'
                }
                return buildResponse(400, response)
        else:
            response = {
                'Message': 'Profile is not found for this user',
                'Error': 'ProfileError'
            }
            return buildResponse(400, response)

        cursor.execute("SELECT * FROM dispatch_dev.dispatcher WHERE dispatcher_id = %s", (dispatcherId,))
        row = cursor.fetchone()
        if row:
            data_dict['dispatcherEmail'] = row['email']
            data_dict['dispatcherPhone'] = row['phone_number']
        else:
            data_dict['dispatcherEmail'] = ''
            data_dict['dispatcherPhone'] = ''

        cursor.execute("SELECT * FROM dispatch_dev.company WHERE sub = %s", (sub,))
        row = cursor.fetchone()
        if row:
            data_dict['profileaddress'] = row['address1']
            data_dict['profilefaxnumber'] = row['fax']
            data_dict['profilecompanyname'] = row['name']
            if not data_dict['profileaddress']:
                response = {
                    'Message': 'Profile address is not found for this user',
                    'Error': 'ProfileError'
                }
                return buildResponse(400, response)
        else:
            response = {
                'Message': 'Profile is not found for this user',
                'Error': 'ProfileError'
            }
            return buildResponse(400, response)
        
        try:
            class HTML2PDF(FPDF, HTMLMixin):
                pass

            def generate_pdf(invoice_data, filename):
                pdf = HTML2PDF()
                pdf.add_page()
                # pdf.write_html(html_summary_data)
                pdf.set_font('Arial', 'B', 14)
                # pdf.cell(0, 5, 'MODEL TRANSPORT LLC', 0, 1, 'L')
                # pdf.cell(0, 5, '9304 FOREST LN SUITE 236', 0, 1, 'L')
                # pdf.cell(0, 5, 'DALLAS, TX 75243', 0, 1, 'L')
                # for line in profileAddressxxx:
                #     pdf.cell(0, 5, line, 0, 1, 'L')
                pdf.cell(0, 5, profileaddress, 0, 1, 'L')
                pdf.ln(20)
                # Date and invoice number
                pdf.set_font('Arial', 'B', 10)
                pdf.cell(74, 10, '', 0, 0)
                pdf.cell(0, 5, 'INVOICE', 0, 1, 'R')
                pdf.set_font('Arial', '', 12)
                pdf.cell(0, 5, 'Date: ' + date_only, 0, 1, 'R')
                pdf.cell(0, 5, 'Inv# ' + my_str, 0, 1, 'R')
                pdf.ln(10)
                # Contact information
                pdf.set_font('Arial', 'B', 10)
                current_y = pdf.y
                pdf.y = current_y - 10
                pdf.cell(0, 5, 'TEL# ' + dispatcherPhone, 0, 1)
                pdf.cell(0, 5, 'FAX#', 0, 1)
                pdf.cell(0, 5, 'Email: MODEL ' + dispatcherEmail, 0, 1)
                pdf.ln(10)
                # Bill to
                pdf.set_font('Arial', 'B', 14)
                pdf.cell(0, 5, 'BILL TO:', 0, 1)
                pdf.set_font('Arial', '', 12)
                pdf.cell(0, 5, companyname, 0, 1)
                pdf.cell(0, 5, companyaddress, 0, 1)
                pdf.ln(20)
                # Table headers
                pdf.set_fill_color(221, 221, 221)
                pdf.set_font('Arial', 'B', 10)
                pdf.cell(32, 10, 'SHIPMENT DATE', 1, 0, 'C', True)
                pdf.cell(32, 10, 'LOAD NO.', 1, 0, 'C', True)
                pdf.cell(32, 10, 'TRUCK#', 1, 0, 'C', True)
                pdf.cell(32, 10, 'TRAILER#', 1, 0, 'C', True)
                pdf.cell(32, 10, 'P O NO.', 1, 0, 'C', True)
                pdf.cell(32, 10, 'TERMS', 1, 1, 'C', True)
                # Table data
                pdf.set_fill_color(255, 255, 255)
                pdf.set_font('Arial', '', 8)
                pdf.cell(32, 10, date_only, 1, 0, 'C', True)
                pdf.cell(32, 10, loadNumber, 1, 0, 'C', True)
                pdf.cell(32, 10, truckFkstr, 1, 0, 'C', True)
                pdf.cell(32, 10, trailorIdFkstr, 1, 0, 'C', True)
                pdf.cell(32, 10, '-', 1, 0, 'C', True)
                pdf.cell(32, 10, 'NET 15 DAYS', 1, 0, 'C', True)
                pdf.ln(20)
                w = 48
                h = 12
                pdf.set_font('Arial', 'B', 10)
                pdf.set_fill_color(221, 221, 221)
                col_names = ["QUANTITY", "DESCRIPTION", "UNIT PRICE", "TOTAL"]
                for col_name in col_names:
                    pdf.cell(w, h, col_name, border=1, align='C', fill=True)
                pdf.ln()
                pdf.set_font('Arial', '', 8)
                pdf.set_text_color(0, 0, 0)
                pdf.cell(w, 10, "100", border=1, align='C')
                pdf.cell(w, 10, results, border=1, align='C')
                pdf.cell(w, 10, '$'+ stringRate, border=1, align='C')
                pdf.cell(w, 10, '$'+ stringRate, border=1, align='C')
                pdf.ln(10)
                pdf.set_font('Arial', 'B', 12)
                
                pdf.ln(20)

                pdf.set_text_color(216, 35, 11)
                pdf.set_font('Arial', 'B', 16)
                pdf.rect(10, 190, 190, 40, 'D')
                pdf.set_fill_color(221, 221, 221)
                pdf.rect(10, 190, 190, 40, 'F')
                pdf.cell(0, 5, 'NOTICE OF ASSIGNMENT', 0, 1, 'C')
                pdf.set_font('Arial', '', 12)
                pdf.cell(0, 5, 'THIS INVOICE HAS BEEN ASSIGNED TO,', 0, 1, 'C')
                pdf.cell(0, 5, 'AND MUST BE PAID DIRECTLY TO:', 0, 1, 'C')
                pdf.cell(0, 5, companyname, 0, 1, 'C')
                for line in addressLines:
                    pdf.cell(0, 5, line, 0, 1, 'C')
                pdf.ln(20)
                
                pdf.set_text_color(0, 0, 0)
                pdf.set_font('Arial', 'B', 10)
                pdf.cell(10)
                pdf.cell(120, 5, 'LINE HAUL', 1, 0, 'R', True)
                pdf.cell(40, 5, '-', 1, 1)
                pdf.cell(10)
                pdf.cell(120, 5, 'STOP OFF', 1, 0, 'R', True)
                pdf.cell(40, 5, '-', 1, 1)
                pdf.cell(10)
                pdf.cell(120, 5, 'FSC', 1, 0, 'R', True)
                pdf.cell(40, 5, '-', 1, 1)
                pdf.cell(10)
                pdf.cell(120, 5, 'UNLOADING', 1, 0, 'R', True)
                pdf.cell(40, 5, '-', 1, 1)
                pdf.cell(10)
                pdf.cell(120, 5, 'TOTAL DUE', 1, 0, 'R', True)
                pdf.cell(40, 5, '$'+ stringRate, 1, 1)
                pdf.ln(10)
                file_path = f"/tmp/invoice_{filename}.pdf"
                pdf.output(file_path)
                return file_path
                
            def merge_pdfs(file_paths):
                pdf_merger = PyPDF2.PdfMerger()
                for path in file_paths:
                    with open(path, 'rb') as f:
                        pdf_merger.append(PyPDF2.PdfReader(f))
                merged_pdf = io.BytesIO()
                pdf_merger.write(merged_pdf)
                return merged_pdf.getvalue()

            # Define function to upload file to S3 and generate pre-signed URL
            def upload_to_s3(bucket_name, file_path, aws_access_key_id, aws_secret_access_key):
                s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
                object_name = f"public/{uuid.uuid4()}.pdf"
                print("object_namex", object_name)
                with io.BytesIO(file_path) as f:
                    s3.upload_fileobj(f, bucket_name, object_name)
                presigned_url = s3.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": bucket_name, "Key": object_name},
                    ExpiresIn=None,  
                )
                return presigned_url

            # Call the functions to generate the PDF and upload it to S3
            invoice_data = data_dict
            bolpath = data_dict["bolPath"]
            rcPath = data_dict["rcPath"]
            rate = data_dict["rate"]
            stringRate = str(rate)
            # invoiceId = data_dict['invoiceId']
            loadId = data_dict['loadId']
            my_str = str(loadId)
            timestamp = data_dict['tsUpdated']
            # timestamp = result['lastUpdated']
            date_only = timestamp[:10]
            loadNumber = data_dict['loadNumber']
            truckFk = data_dict['truckFk']
            truckFkstr = str(truckFk)
            trailorId = data_dict['trailorId']
            trailorIdFkstr = str(trailorId)
            companyname = data_dict['companyname']
            companyaddress = data_dict['companyaddress']
            dispatcherEmail = data_dict['dispatcherEmail']
            dispatcherPhone = data_dict['dispatcherPhone']
            subid = data_dict['sub']
            print("companyaddress", companyaddress)
            if companyaddress is not None:
                addressLines = companyaddress.split(', ')
            else:
                addressLines = []

            profileaddress = data_dict['profileaddress']
            # profileAddressxxx = profileaddress.split(",")
            # print("Checkprofilesplitaddress", profileAddressxxx)
            address = data_dict["address"]
            city_state = []
            for stop in address:
                city_state.append(stop['city'] + ', ' + stop['state'])
            results = ' TO '.join(city_state)
            print("Testing:-",results)

            # Summary.
            with open('index.html') as f:
                template_string = f.read()
            template = Template(template_string)
            data_dict = template.render(invoice_data)
            html_summary_data = invoice_data
            print("Testing:-1")


            html_rc_data = rcPath
            html_bol_data = bolpath
            pdf_file_path1 = generate_pdf(html_summary_data, "summary")
            pdf_file_path2 = f"/tmp/rc_data_{loadId}.pdf"
            response = requests.get(html_rc_data)
            with open(pdf_file_path2, "wb") as f:
                f.write(response.content)


            pdf_file_path3 = f"/tmp/bol_data_{loadId}.pdf"
            response = requests.get(html_bol_data)
            with open(pdf_file_path3, "wb") as f:
                f.write(response.content)
            # response = requests.get(html_rc_data)
            print("InvoiceId:", loadId)
            merged_pdf_bytes = merge_pdfs([pdf_file_path1, pdf_file_path2, pdf_file_path3])
            merged_pdf_path = f"/tmp/merged_{loadId}.pdf"
            with open(merged_pdf_path, "wb") as f:
                f.write(merged_pdf_bytes)
            
            bucket_name = "prashantbuckettest"
            aws_access_key_id = "AKIAQPUIZQ3OQ5OTUOVG"
            aws_secret_access_key = "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"
            presigned_url = upload_to_s3(bucket_name, merged_pdf_bytes, aws_access_key_id, aws_secret_access_key)
            pdf_urlx = presigned_url.split('.pdf')[0] + '.pdf'
            print(presigned_url)
            os.remove(merged_pdf_path)
            os.remove(pdf_file_path1)
            os.remove(pdf_file_path2)
            os.remove(pdf_file_path3)
            print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",pdf_urlx)
            updateInvoiceUrl = "update dispatch_dev.load SET status = 'INVOICED', invoice_url = %s, ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
            cursor.execute(updateInvoiceUrl, (pdf_urlx, loadId, subid))
            connection.commit()
            print("OPERATION DONE")
            loadsResponseNew = {"Message":"BOL Accepted"}
            return buildResponse(200, loadsResponseNew)
        except Exception as ex:
            connection.rollback()
            return buildResponse(500,{'Message': 'Exception occurred while generating the invoiveZZ', 'Error': ex})
        finally:
            connection.close()
        



        # generate_invoice_pdf(result)

        
        # connection.close()

def formatCompanyResponse(row):
    # print("formatCreatedCompanyResponse = {}".format(row))
    response = {'company_id': row[0], 
                'commission': str(row[1]),
                'insurancerate': str(row[2]),
                'factoringinfo': str(row[3]), 
                'companyname': str(row[4]), 
                'companyaddress': str(row[5]), 
                'ts_created': str(row[6]), 
                'ts_updated': str(row[7]), 
                'status': str(row[8]), 
                'sub': str(row[9]), 
                }
    # response[‘event’] = event
    return response

def getAllLoadsGeneratedInvoices(connection, sub, filter_search, limit, offset):
    cursor = connection.cursor()
    if filter_search == 'Null':
        print("IF")
        cursor.execute(getAllInvoicesWithSub, (sub, int(limit),  (int(limit) * int(offset))))
    else:
        print("ELSE")
        cursor.execute(getAllInvoicesWithSubSearch, (sub, filter_search, int(limit),  (int(limit) * int(offset))))
    invoices = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                invoices.append(formatInvoiceResponse(row))
            loadsResponse = {
                "totalDbCount": len(invoices),
                "loads": invoices
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No invoice record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all invoice details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all invoice details for sub : {}'
                             .format(sub)})


def getinvoicebysubanditsid(connection,invoiceId,sub):
    cursor = connection.cursor()
    cursor.execute(getAllInvoicesWithSubandId,(invoiceId,sub))
    try:
        if cursor.rowcount != 0:
            expnse = formatInvoiceResponse(cursor.fetchone())
            return buildResponse(200, expnse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'invoice: {} not found for sub: {}'.format(invoiceId, sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while fetching invoice details for invoiceId : {invoiceId} , Error: {ex}'})


def factoringTheLoadInvoice(connection, bodyDetails):
    cursor = connection.cursor()
    try:
       invoiceList = []
       allUrls = []
       bodyParam = json.loads(bodyDetails)
       invoiceIds = bodyParam['invoiceIds']
       sub = bodyParam['sub']
       if cursor.rowcount != 0:
            for invoiceId in invoiceIds:
                updateInvoiceStatus = "update dispatch_dev.load SET status = 'FACTORED', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateInvoiceStatus, (invoiceId, sub))

                getSelectedInvoice = "select * from dispatch_dev.load where load_id = %s and sub = %s"
                cursor.execute(getSelectedInvoice, (invoiceId, sub))
                for row in cursor:
                    invoiceList.append(formatLoadResponse(row))
            for invoices in invoiceList:
                print("invoices", invoices)
                invoiceUrls = invoices['invoice_url']
                print("invoiceUrls", invoiceUrls)
                allUrls.append(invoiceUrls)
            pdf_url = mergeAllInvoicePdf(allUrls)
            loadsResponse = {
                "mergedPdfUrl": pdf_url
            }
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching invoice details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching invoice details for sub : {}'
                             .format(sub)})


def removeFactored(connection, bodyDetails):
    print("REMOVEFACTORED")
    cursor = connection.cursor()
    try:
       bodyParam = json.loads(bodyDetails)
       invoiceIds = bodyParam['invoiceIds']
       sub = bodyParam['sub']
       if cursor.rowcount != 0:
            for invoiceId in invoiceIds:
                updateInvoiceStatus = "update dispatch_dev.load SET status = 'INVOICED', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
                cursor.execute(updateInvoiceStatus, (invoiceId, sub))
            loadsResponse = {
                "status": True
            }
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching invoice details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching invoice details for sub : {}'
                             .format(sub)})

def updateTheLoadStatusList(connection, bodyDetails):
    print("CHECK")
    cursor = connection.cursor()
    try:
       bodyParam = json.loads(bodyDetails)
       load_id = bodyParam['load_id']
       status = bodyParam['status']
       sub = bodyParam['sub']
       if cursor.rowcount != 0:
            updateLoadStatus = "update dispatch_dev.load SET status = %s, ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
            cursor.execute(updateLoadStatus, (status, load_id, sub))
            if(status == 'ASSIGNED'):
                notificationCodeLoadUpdate(cursor, load_id, sub)
            loadsResponse = {
                "status": True
            }
            connection.commit() # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while fetching invoice details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching invoice details for sub : {}'
                             .format(sub)})


def notificationCodeLoadUpdate(cursor, insertedLoadId, sub):
    print("TESTINGNOTI")
    cursor.execute("SELECT * FROM dispatch_dev.load WHERE load_id = %s AND sub = %s", (insertedLoadId, sub,))
    row = cursor.fetchone()
    # status = row['status']
    driver1_fk = row['driver1_fk']
    # if status == 'ASSIGNED':
    if row:
        loadNumber = row['load_number']
    else:
        loadNumber = ''
    
    cursor.execute("SELECT * FROM dispatch_dev.user_device_tokens WHERE user_id = %s AND sub = %s", (driver1_fk, sub,))
    row = cursor.fetchone()
    print("ARGUMENTS__ROW>>>>2", row)
    if row:
        deviceToken = row['device_token']
        print("ARGUMENTS__ROW>>>>deviceToken", deviceToken)
    else:
        deviceToken = ''
    print("TRIGGERING NOTIFICATIONS")
    message = {}
    message['loadId'] = insertedLoadId
    message['getMessagetitle'] = 'New Load has been assigned to you'
    message['getMessagebody'] = loadNumber
    message['userToken'] = deviceToken
    send_notifications(message)
                             

def mergeAllInvoicePdf(allUrls):
    print("TESTING<><><><><><>", allUrls)
    import uuid
    import PyPDF2
    import io
    def merge_pdfs(file_paths):
        pdf_merger = PyPDF2.PdfMerger()
        for path in file_paths:
            with open(path, 'rb') as f:
                pdf_merger.append(PyPDF2.PdfReader(f))
        merged_pdf = io.BytesIO()
        pdf_merger.write(merged_pdf)
        return merged_pdf.getvalue()
    
    def upload_to_s3(bucket_name, file_path, aws_access_key_id, aws_secret_access_key):
        s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
        object_name = f"public/{uuid.uuid4()}.pdf"
        print("object_namex", object_name)
        with io.BytesIO(file_path) as f: 
            s3.upload_fileobj(f, bucket_name, object_name)
        presigned_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=None,
        )
        return presigned_url

    pdf_file_paths = []
    for i, url in enumerate(allUrls):
        # file_path = f"/tmp/invoice_{filename}.pdf"
        pdf_file_path = f"/tmp/invoice_{i+1}.pdf"
        response = requests.get(url)
        with open(pdf_file_path, "wb") as f:
            f.write(response.content)
        pdf_file_paths.append(pdf_file_path)

    # Merge the PDF files
    merged_pdf_bytes = merge_pdfs(pdf_file_paths)
    bucket_name = "prashantbuckettest"
    aws_access_key_id = "AKIAQPUIZQ3OQ5OTUOVG"
    aws_secret_access_key = "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"
    presigned_url = upload_to_s3(
        bucket_name, merged_pdf_bytes, aws_access_key_id, aws_secret_access_key)
    pdf_urlx = presigned_url.split('.pdf')[0] + '.pdf'
    for file_path in pdf_file_paths:
        os.remove(file_path)
    return pdf_urlx
    print(presigned_url)
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", pdf_urlx)
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<1>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    pass
    

def getDriverLoadsCount(connection, driverid, sub, status):
    cursor = connection.cursor()
    cursor.execute(getDriverLoadHistoryCountBySubandDriverid, (driverid, sub, status))
    return cursor.fetchone()[0]




def driverActiveLoad(connection, driverid, sub, status):
    cursor = connection.cursor()
    if status == '':
        print("IF")
        cursor.execute(getAllLoadsDataWithStatus, (driverid, sub))
    else:
        print("ELSE")
        cursor.execute(getDriverLoadHistory, (driverid, sub, status))
    totalActiveloads = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                load = formatLoadResponse(row)
                totalActiveloads.append(load)
            loadsResponse = {
                "totalDbCount": len(totalActiveloads),
                "driverId": driverid,
                "sub": sub,
                "activeLoads": totalActiveloads
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No load record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all load details for sub : {}'
                             .format(sub)})

def getDriverLoadsCount(connection, driverid, sub, status):
    cursor = connection.cursor()
    cursor.execute(getDriverLoadHistoryCountBySubandDriverid, (driverid, sub, status))
    return cursor.fetchone()['count']




def getLoadsCount(connection, sub):
    cursor = connection.cursor()
    cursor.execute(getLoadCountBySub, sub)
    return cursor.fetchone()['count']


def updateLoad(connection, bodyDetails):
    cursor = connection.cursor()
    if 'loadId' not in bodyDetails or 'sub' not in bodyDetails:
        return buildResponse(400, {'Message': 'loadId and sub should be specified'})
    body = json.loads(bodyDetails)
    updateQueryParameters = buildUpdateQueryParameters(body)
    print("updateQueryParameters")
    print(updateQueryParameters)
    try:
        cursor.execute(updateQueryParameters['query'], updateQueryParameters['values'])
        connection.commit()

        if 'truckFk' in body:
            if body['truckFk'] is None:
                print("IF__REMOVING__THE__TRUCK")
                removeAssociatedEntities(cursor, connection, body['loadId'], body['sub'])
        print("ADDING_THE__TRUCK")
        cursor.execute(getLoadByIdAndSub, (body['loadId'], body['sub']))
        if cursor.rowcount != 0:
            response = {
                'operation': 'patch',
                'message': 'success',
                'load': formatLoadResponse(cursor.fetchone())
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {
                'Message': 'LoadId: {} not found for sub: {}'.format(body['loadId'], body['sub'])})
    except Exception as ex:
        print("Exception occurred while updating load with loadId:{} and sub : {} , Error : {}".format(
            body['loadId'], body['sub'], ex))
        return buildResponse(500, {'Message': 'Exception occurred  while updating load with '
                                              'loadId: {} and sub:{}'.format(body['loadId'],
                                                                             body['sub'])})

def removeAssociatedEntities(cursor, connection, loadId, sub):
    try:
        print("TESTING", loadId)
        cursor.execute("UPDATE dispatch_dev.load SET status = 'CREATED', driver1_fk = NULL, driver2_fk = NULL, trailer_fk = NULL WHERE load_id = %s AND sub = %s",
                       (loadId, sub))
        connection.commit()
        print("Associated entities removed for LoadId: {} and sub: {}".format(loadId, sub))
    except Exception as ex:
        print("Exception occurred while removing associated entities for LoadId: {} and sub: {}, Error: {}".format(
            loadId, sub, ex))


def buildUpdateQueryParameters(body):
    print(body)
    keyValueList = []
    baseQuery = 'update dispatch_dev.load set '
    values = ()
    loadUpdateAttributes = ['loadNumber', 
                            'estDriveTime', 
                            'rate', 
                            'loadMiles', 
                            'loadMiles', 
                            'address', 
                            'rcPath',
                            'lumper', 
                            'detention', 
                            'tonu', 
                            'bolPath', 
                            'dispatcherFk', 
                            'brokerName', 
                            'truckFk',
                            'trailerFk', 
                            'driver1Fk', 
                            'driver2Fk', 
                            'tsCreated', 
                            'tsUpdated', 
                            'status', 
                            'sub']

    for fieldName in loadUpdateAttributes:
        print(fieldName)
        print(fieldName in body)
        if fieldName in body and fieldName != 'loadId' and fieldName != 'sub':
            value = body[fieldName]
            if fieldName in ['lumper', 'detention', 'tonu'] and value is None:
                value = 0.00
            keyValueList.append(camel2snake(fieldName) + '= %s')
            if fieldName == 'address' or fieldName == 'documents':
                values = values + (json.dumps(body[fieldName]),)
            else:
                values = values + (value,)
    if len(keyValueList) > 0:
        keyValueList.append('ts_updated = %s')
        values = values + (datetime.datetime.now(),)
    values = values + (body['loadId'], body['sub'])
    print(','.join(keyValueList))
    print(values)
    return {
        "query": baseQuery + ','.join(keyValueList) + ' where load_id = %s and sub = %s',
        "values": values
    }



def loadsTableViewSearch(connection, sub, limit, offset, bodyDetails):
    cursor = connection.cursor()
    body = json.loads(bodyDetails)
    searchCriteria = body['searchCriteria']
    print(searchCriteria)
    values = (
        '%' + str(searchCriteria['loadNumber']) + '%',
        '%' + searchCriteria['status'] + '%',
        '%' + searchCriteria['primaryDriverName'] + '%',
        '%' + searchCriteria['dispatcherName'] + '%',
        '%' + searchCriteria['truckPlateNumber'] + '%',
        '%' + searchCriteria['trailerPlateNumber'] + '%',
        sub,
        int(limit),
        (int(limit) * int(offset)))
    print(values)
    cursor.execute(getLoadsBySubView, values)
    loads = []
    count = 0
    try:
        print(cursor.rowcount)
        if cursor.rowcount != 0:
            for row in cursor:
                formattedLoad = formatLoadViewResponse(row)
                formattedLoad['actions'] = getActionsForStatus(row['status'])
                loads.append(formattedLoad)
                count = row['count']
            loadsResponse = {
                "totalDbCount": count,
                "loads": loads
            }
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No load record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all load details for sub: {}".format(sub))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all loads details for sub : {}'
                             .format(sub), 'Error': ex})

# This status logic is handled and showing the status accordingly to the operation.
def getActionsForStatus(status):
    if status == 'CREATED':
        return []
    elif status == 'ASSIGNED':
        return ['ACCEPTED', 'IN TRANSIT', 'BOL PENDING', 'REJECTED']
    elif status == 'ACCEPTED':
        return ['IN TRANSIT', 'BOL PENDING']
    elif status == 'REJECTED':
        return []
    elif status == 'IN TRANSIT':
        return ['BOL PENDING']
    elif status == 'BOL PENDING':
        return []
    elif status == 'READY FOR REVIEW':
        return []
    else:
        return []

def deleteLoad(connection, loadId, sub):
    print(loadId)
    print(sub)
    cursor = connection.cursor()
    try:
        cursor.execute(deleteLoadByIdAndSub, ('DELETED', loadId, sub))
        connection.commit()
        if cursor.rowcount != 0:
            response = {
                'operation': 'delete',
                'message': 'success',
                'affectedRowCount': cursor.rowcount
            }
            return buildResponse(200, response)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'LoadId: {} not found for sub: {}'.format(loadId, sub)})
    except Exception as ex:
        print(
            "Exception occurred while deleting load with loadId:{} and sub : {},  Error : {}".format(
                loadId, sub,
                ex))
        return buildResponse(500, {'Message': 'Exception occurred  while deleting load with '
                                              'loadId: {} and sub:{}'.format(loadId, sub)})


def onboardLoad(connection, bodyDetails):
    body = json.loads(bodyDetails)
    print('onboardLoad : body = {}'.format(body))
    print('onboardLoad : address = {}'.format(json.dumps(body['address'])))
    cursor = connection.cursor()
    try:
        if(body['truckFk'] == None):
            body['status'] = 'CREATED'
        else:
            body['status'] = 'ASSIGNED'
        values=(body['loadNumber'], 
                body['estDriveTime'], 
                body['rate'], 
                body['loadMiles'],
                json.dumps(body['address']), 
                body['rcPath'], 
                body['lumper'],
                body['detention'],
                body['tonu'], 
                body['bolPath'], 
                body['truckFk'], 
                body['dispatcherFk'],
                body['brokerName'], 
                body['trailerFk'], 
                body['driver1Fk'], 
                body['driver2Fk'],
                datetime.datetime.now(), 
                datetime.datetime.now(), 
                body['status'],
                body['sub'],
                body['weight'],
                
                )
        cursor.execute(insertLoad, values)
        insertedLoadId = connection.insert_id()
        print("Inserted load ID : {}".format(insertedLoadId))
        

        # Trigger Notifications
        cursor.execute("SELECT * FROM dispatch_dev.load WHERE load_id = %s AND sub = %s", (insertedLoadId, body['sub'],))
        row = cursor.fetchone()
        status = row['status']
        if status == 'ASSIGNED':
            if row:
                loadNumber = row['load_number']
            else:
                loadNumber = ''
            
            cursor.execute("SELECT * FROM dispatch_dev.user_device_tokens WHERE user_id = %s AND sub = %s", (body['driver1Fk'], body['sub'],))
            row = cursor.fetchone()
            print("ARGUMENTS__ROW>>>>2", row)
            if row:
                deviceToken = row['device_token']
            else:
                deviceToken = ''
            print("TRIGGERING NOTIFICATIONS")
            message = {}
            message['loadId'] = insertedLoadId
            message['getMessagetitle'] = 'New Load has been assigned to you'
            message['getMessagebody'] = loadNumber
            message['userToken'] = deviceToken
            send_notifications(message)
    except pymysql.Error as ex:
        print(ex)
        connection.rollback()
        connection.close()
        print('Exception occurred while saving load')
        return buildResponse(500,
                             {'Message': 'Exception occurred while saving load', 'Error': ex})
    else:
        connection.commit()
        insertedLoadDetails = getLoad(connection, insertedLoadId, body['sub'])
        connection.close()

    return insertedLoadDetails


def formatLoadResponse(row):
    print("formatLoadResponse = {}".format(row))
    response = {'loadId': row['load_id'], 
                'loadNumber': str(row['load_number']), 
                'estDriveTime': str(row['est_drive_time']),
                'rate': str(row['rate']), 
                'loadMiles': str(row['load_miles']), 
                'address': json.loads(row['address']),
                'rcPath': str(row['rc_path']), 
                'lumper': float(row['lumper']), 
                'detention': float(row['detention']),
                'tonu': float(row['tonu']), 
                'bolPath': str(row['bol_path']), 
                'truckFk': row['truck_fk'], 
                'dispatcherFk': row['dispatcher_fk'],
                'brokerName': row['broker_name'], 
                'trailerFk': row['trailer_fk'], 
                'driver1Fk': row['driver1_fk'], 
                'driver2Fk': row['driver2_fk'],
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']), 
                'status': str(row['status']), 
                'sub': str(row['sub']),
                'weight': str(row['weight']),
                'invoice_url': str(row['invoice_url'])
                }
    # response[‘event’] = event
    return response


def formatNotificationResponse(row):
    response = {'loadId': row['load_id'], 
                'loadNumber': str(row['load_number']), 
                'rate': str(row['rate']), 
                'loadMiles': str(row['load_miles']), 
                'address': json.loads(row['address']),
                'rcPath': str(row['rc_path']), 
                'bolPath': str(row['bol_path']), 
                'sub': str(row['sub']),
                'status': str(row['status']),
                'tsCreated': str(row['ts_created']), 
                'message': 'Hey there, a new load is available for pickup! click here to know more',
                'notificationtype': 'load'
                }
    # response[‘event’] = event
    return response


def formatInvoiceResponse(row):
    print("formatInvoiceResponse = {}".format(row))
    response = {'invoiceId': row['load_id'], 
                'loadId': row['load_id'], 
                'loadNumber': str(row['load_number']),
                'estDriveTime': str(row['est_drive_time']),
                'rate': float(row['rate']), 
                'loadMiles': str(row['load_miles']), 
                'address': json.loads(row['address']),
                'rcPath': str(row['rc_path']), 
                'lumper': float(row['lumper']), 
                'detention': float(row['detention']),
                'tonu': float(row['tonu']),
                'bolPath': str(row['bol_path']), 
                'truckFk': row['truck_fk'],
                'dispatcherId': row['dispatcher_fk'],
                'trailorId': row['trailer_fk'],
                'driverid_1': row['driver1_fk'],
                'driverid_2': row['driver2_fk'],
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']), 
                'status': str(row['status']), 
                'sub': str(row['sub']),
                'weight': str(row['weight']),
                'invoice_url': str(row['invoice_url']),
                'payment_status': str(row['payment_status']),
                'truck_license_plate_number': str(row['truck_license_plate_number']),
                'trailer_license_plate_number': str(row['trailer_license_plate_number']),
                'driverFullName': str(row['fullname']),
                'driver_number': str(row['driver_number']),
                'dispatcherFullName': str(row['dispatcherfullname']),
                }
    # response[‘event’] = event
    return response

def formatDriverResponse(row, driverRole):
    print(row)
    print(driverRole)
    response = {'driverId': row['driver_id'], 
                'fullName': str(row['full_name']), 
                "driverRole": driverRole, 
                'sub': str(row['sub'])}
    print("formatDriverResponse response ".format(response))
    return response


def formatTrailerResponse(row):
    print("formatTrailerResponse = {}".format(row))
    response = {'trailerId': row['trailer_id'], 
                'licensePlateNumber': str(row['license_plate_number']), 
                'sub': str(row['sub'])}
    return response


def formatDispatcherResponse(row):
    print("formatDispatcherResponse = {}".format(row))
    response = {'dispatcherId': row['dispatcher_id'], 
                'fullName': str(row['full_name']), 
                'sub': str(row['sub'])}
    print("formatDispatcherResponse response ".format(response))
    return response


def formatBrokerResponse(row):
    print("formatBrokerResponse = {}".format(row))
    response = {'brokerId': row['broker_id'], 
                'name': str(row['name']), 
                'sub': str(row['sub'])}
    print("formatBrokerResponse response ".format(response))
    return response


# def formatLoadViewResponse(row):
#     print(row)
#     response = {'loadId': row['loadId'],
#                 'loadNumber':row['loadNumber'],
#                 'primaryDriverName': str(row['primaryDriverName']),
#                 'dispatcherName': str(row['dispatcherName']),
#                 'truckPlateNumber': str(row['truckPlateNumber']), 
#                 'trailerPlateNumber': str(row['trailerPlateNumber']),
#                 'address': json.loads(row['address']),
#                 'status': str(row['status'])}
#     return response

# FORMAT FOR LOAD HISTORY API'S
def formatLoadHistoryResponse(row):
    print("formatLoadResponse = {}".format(row))
    response = {
                'loadStatus': str(row['status']),
                'loadId': row['load_id'], 
                'loadNumber': str(row['load_number']), 
                'estDriveTime': str(row['est_drive_time']),
                'rate': float(row['rate']), 
                'loadMiles': float(row['load_miles']), 
                'address': json.loads(row['address']),
                'rcPath': str(row['rc_path']), 
                'lumper': float(row['lumper']), 
                'detention': float(row['detention']),
                'tonu': float(row['tonu']), 
                'bolPath': str(row['bol_path']), 
                'truckFk': row['truck_fk'], 
                'dispatcherFk': row['dispatcher_fk'],
                'brokerName': row['broker_name'], 
                'trailerFk': row['trailer_fk'], 
                'driver1Fk': row['driver1_fk'], 
                'driver2Fk': row['driver2_fk'],
                'tsCreated': str(row['ts_created']), 
                'tsUpdated': str(row['ts_updated']), 
                'status': str(row['status']), 
                'sub': str(row['sub'])
                }
    # response[‘event’] = event
    print("RESPONSE", response)
    print("formatLoadHistoryDriverResponse response ".format(response))
    return response

def formatDriverResponse(row, driverRole):
    print(row)
    print(driverRole)
    response = {'driverId': row['driver_id'], 
                'fullName': str(row['full_name']), 
                "driverRole": driverRole, 
                'sub': str(row['sub'])}
    print("formatDriverResponse response ".format(response))
    return response


def formatTrailerResponse(row):
    print("formatTrailerResponse = {}".format(row))
    response = {'trailerId': row['trailer_id'], 
                'licensePlateNumber': str(row['license_plate_number']), 
                'sub': str(row['sub'])}
    return response


def formatDispatcherResponse(row):
    print("formatDispatcherResponse = {}".format(row))
    response = {'dispatcherId': row['dispatcher_id'], 
                'fullName': str(row['full_name']), 
                'sub': str(row['sub'])}
    print("formatDispatcherResponse response ".format(response))
    return response

def formatPaymentDispatcherResponse(row):
    print("formatPaymentDispatcherResponse = {}".format(row))
    response = {'dispatcherId': row['dispatcher_id'], 
                'fullName': str(row['full_name']), 
                'dispatcher_type': str(row['dispatcher_type']), 
                'commission_percentage': str(row['commission_percentage']), 
                'monthly_salary': str(row['monthly_salary']), 
                'sub': str(row['sub'])}
    print("formatPaymentDispatcherResponse response ".format(response))
    return response


# def formatBrokerResponse(row):
#     print("formatBrokerResponse = {}".format(row))
#     response = {'brokerId': row['broker_id'], 
#                 'name': str(row['name']), 
#                 'sub': str(row['sub'])}
#     print("formatBrokerResponse response ".format(response))
#     return response


def formatLoadViewResponse(row):
    print(row)
    response = {'loadId': row['loadId'],
                'loadNumber':row['loadNumber'],
                'rate':str(row['rate']),
                'truck_fk':str(row['truck_fk']),
                'driver1_fk':str(row['driver1_fk']),
                'driver2_fk':str(row['driver2_fk']),
                'trailer_fk':str(row['trailer_fk']),
                'primaryDriverName': str(row['primaryDriverName']),
                'dispatcherName': str(row['dispatcherName']),
                'truckPlateNumber': str(row['truckPlateNumber']), 
                'trailerPlateNumber': str(row['trailerPlateNumber']),
                'address': json.loads(row['address']),
                'updated': str(row['updated']),
                'status': str(row['status']),
                'truckNumber': str(row['truckNumber']),
                'trailerNumber': str(row['trailerNumber'])
                
                
                }
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


def saveDeviceTokens(connection, bodyDetails):
    body = json.loads(bodyDetails)
    user_id = body.get('driverId')
    device_token = body.get('deviceToken')
    sub = body.get('sub')
    platform = body.get('platform')
    query = """
        SELECT * FROM dispatch_dev.user_device_tokens WHERE user_id=%s AND sub=%s
    """
    cursor = connection.cursor()
    cursor.execute(query, (user_id, sub))
    existing_token = cursor.fetchone()
    if existing_token is not None:
        driverId = existing_token['user_id']
        tokenId = existing_token['tokenId']
        if user_id == driverId:
            updatequery = "UPDATE dispatch_dev.user_device_tokens SET device_token = %s WHERE tokenId = %s AND user_id = %s AND sub = %s AND platform = %s"
            cursor.execute(updatequery, (device_token, tokenId, user_id, sub, platform))
            connection.commit()
            message = 'Device Token replaced'
            status_code = 200
        else:
            message = 'User ID does not match'
            status_code = 400
    else:
        query = """
            INSERT INTO dispatch_dev.user_device_tokens (user_id, device_token, sub, platform)
            VALUES (%s, %s, %s, %s)
        """
        try:
            cursor.execute(query, (user_id, device_token, sub, platform))
            connection.commit()
            message = 'Device Token has been successfully saved.'
            status_code = 201
        except pymysql.Error as ex:
            connection.rollback()
            message = 'Exception occurred while saving device token'
            status_code = 500
    
    cursor.close()
    connection.close()
    response_data = {
        'status': status_code < 400,
        'status_code': status_code,
        'data': {
            'message': message
        },
    }
    tokenResponse = {
                "data": response_data
    }
    return buildResponse(200, tokenResponse)
 



def sendInvoices(connection, bodyDetails):
    print("TESTING____HELLO")
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email.utils import COMMASPACE
    from email import encoders

    
    try:
        bodyParam = json.loads(bodyDetails)
        email = bodyParam['email']
        pdf_urlx = bodyParam['pdf_urlx']
        # CREDENTIALS NEED TO BE CHANGED
        sender_email = "amrut.support@barquecontech.com"
        sender_password = "Amrut@123"
        recipient_email = email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = COMMASPACE.join([recipient_email])
        msg['Subject'] = "Dispatcher Load Invoice"
        with open('emailTemplate.html', 'r') as f:
            html = f.read()
        msg.attach(MIMEText(html, 'html'))

        pdf_response = requests.get(pdf_urlx)
        invoice_send = "/tmp/invoice_send.pdf"
        with open(invoice_send, 'wb') as f:
            f.write(pdf_response.content)

        with open(invoice_send, 'rb') as f:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename="invoice.pdf")
            msg.attach(part)

        smtp_server = smtplib.SMTP('mail.barquecontech.com', 587)
        smtp_server.ehlo()
        smtp_server.starttls()
        smtp_server.login(sender_email, sender_password)
        smtp_server.sendmail(sender_email, recipient_email, msg.as_string())
        smtp_server.close()


        print("COMPLETED")
        response_data = {
        'data': {
            'message': "Email send successfully"
        },
    }
        return buildResponse(200, response_data)

    except Exception as ex:
        return buildResponse(500, {'Message': 'Error occured while sending the invoice','Error': ex})


def uploadFiles(connection, bodyDetails):
    print("test")
    import pathlib
    # print("1>>>", bodyDetails)
    # body = json.loads(bodyDetails)
    # bucket_name = "prashantbuckettest"
    # aws_access_key_id = "AKIAQPUIZQ3OQ5OTUOVG"
    # aws_secret_access_key = "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"

    try:
        # print("xxx", bodyDetails)
        data = json.loads(json.dumps(bodyDetails
        ))
        print("xxx", data)
        file = data['fileContent']
        file_name = data['fileName']
        file_extension = pathlib.Path(file_name).suffix
        content_type_dict = {
                ".png": "image/png",
                ".jpg": "image/jpg",
                ".gif": "image/gif",
                ".jpeg": "image/jpeg"
                }
        content_type = content_type_dict.get(file_extension, 'application/octet-stream')
        s3_client = boto3.client('s3', aws_access_key_id='AKIAQPUIZQ3OQ5OTUOVG', aws_secret_access_key='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')
        s3_resource = boto3.resource('s3', aws_access_key_id='AKIAQPUIZQ3OQ5OTUOVG', aws_secret_access_key='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')
        bucket_name = 'amplify-try-dev-173136-deployment'

        s3_object_key = "public/" + file_name
        s3_resource.Bucket(bucket_name).put_object(Key=s3_object_key, Body=file, ContentType=content_type)
        s3_object_url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': s3_object_key
            },
            ExpiresIn=3600  # The URL will expire in 1 hour
        ) 

        response = {
            "filename": file_name,
            "s3_object_url": s3_object_url
        }
        print("COMPLETED")
        print("COMPLETED", response)
        return buildResponse(200, response)

    except Exception as ex:
        return buildResponse(500, {'Message': 'Error occured while uploading the file', 'Error': str(ex)})

