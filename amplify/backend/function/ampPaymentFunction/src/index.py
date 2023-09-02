import json
from pickle import NONE
import boto3
import pymysql
from query import *
import json
import os
import datetime
from driverNotifications import *
import uuid
import pdfkit
from jinja2 import Template
import re
db_client = boto3.client('secretsmanager')
db_secret = db_client.get_secret_value(
    SecretId='dev/dispatch/app'
)
db_secret_dict = json.loads(db_secret['SecretString'])
print(db_secret_dict)

healthPath = '/payments/health'
getMethod = 'GET'
postMethod = 'POST'
putMethod = 'PUT'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'

getInvoicesForPaymentsSettelment = '/payments/getpaymentinvoices'
getInvoicesForPaymentsSettelmentById = '/payments/getpaymentinvoicebyid'
getInvoicesFortableSearchView = '/payments/getinvoicestablesearch'
prepareThePayments = '/payments/preparepayments'
changeStatusOfPayment= '/payments/changepaymentstatus'
getGeneratedDriverPayments = '/payments/getdriverpayments'
getPaymentByPaymentId = '/payments/getpayment'
getDriverPaymentsWithStatus = '/payments/getallpayments'

# Edit payment expense so that we can create the payment again.
patchExpense = '/payments/patchexpense'
updatePreparePayment = '/payments/updatepayment'
deletePayment = '/payments/deletepayment'
# testhtmlpdfui
generateui = '/payments/generateui'


def handler(event, context):
    print('received event:')
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']
    conn = database_connect()

    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200, "SUCCESS")

    elif httpMethod == getMethod and path == getInvoicesForPaymentsSettelment:
        if 'sub' in event['queryStringParameters']:
            sub = event['queryStringParameters']['sub']
        else:
            return buildResponse(400, 'No sub')

        if 'driverid' in event['queryStringParameters']:
            driverid = event['queryStringParameters']['driverid']
        else:
            return buildResponse(400, 'No driver id ')
        
        if 'startdate' in event['queryStringParameters']:
            startdate = event['queryStringParameters']['startdate']
        else:
            startdate = ''

        if 'enddate' in event['queryStringParameters']:
            enddate = event['queryStringParameters']['enddate']
        else:
            enddate = ''

        if 'limit' in event['queryStringParameters']:
            limit = event['queryStringParameters']['limit']
        else:
            limit = 10

        if 'offset' in event['queryStringParameters']:
            offset = event['queryStringParameters']['offset']
        else:
            offset = 0

        if 'filter_search' in event['queryStringParameters']:
            filter_search = event['queryStringParameters']['filter_search']
        else:
            filter_search = 'INVOICED'
    
        response = getAllLoadsGeneratedInvoices(conn, sub, driverid, startdate, enddate, limit,
                                                    offset,filter_search)


    elif httpMethod == getMethod and path == getGeneratedDriverPayments:
        if 'sub' in event['queryStringParameters']:
            sub = event['queryStringParameters']['sub']
        else:
            return buildResponse(400, 'No sub')

        if 'driverid' in event['queryStringParameters']:
            driverid = event['queryStringParameters']['driverid']
        else:
            return buildResponse(400, 'No driver id ')
        
        if 'startdate' in event['queryStringParameters']:
            startdate = event['queryStringParameters']['startdate']
        else:
            startdate = ''

        if 'enddate' in event['queryStringParameters']:
            enddate = event['queryStringParameters']['enddate']
        else:
            enddate = ''

        if 'limit' in event['queryStringParameters']:
            limit = event['queryStringParameters']['limit']
        else:
            limit = 10

        if 'offset' in event['queryStringParameters']:
            offset = event['queryStringParameters']['offset']
        else:
            offset = 0

        response = getAllGeneratedDriverPayments(conn, sub, driverid, startdate, enddate, limit,
                                                    offset)

    elif httpMethod == getMethod and path == getInvoicesForPaymentsSettelmentById:
        query_params = event.get('queryStringParameters', {})
        invoice_id = query_params.get('invoiceId')
        sub = query_params.get('sub')

        if not invoice_id or not sub:
            return buildResponse(400, 'Missing invoice ID or sub')

        response = getSettelementInvoiceById(conn, event['queryStringParameters']['invoiceId'], event['queryStringParameters']['sub'])

    elif httpMethod == getMethod and path == getPaymentByPaymentId:
        query_params = event.get('queryStringParameters', {})
        payment_id = query_params.get('paymentId')
        sub = query_params.get('sub')

        if not payment_id or not sub:
            return buildResponse(400, 'Missing payment ID or sub')

        response = getPaymentByItsPaymentId(conn, event['queryStringParameters']['paymentId'], event['queryStringParameters']['sub'])
    
    elif httpMethod == getMethod and path == getDriverPaymentsWithStatus:
        query_params = event.get('queryStringParameters', {})
        driverid = query_params.get('driverid')
        sub = query_params.get('sub')
        if not driverid or not sub:
            return buildResponse(400, 'Missing driverid ID or sub or status')
        response = driverActivePayments(conn, event['queryStringParameters']['driverid'], event['queryStringParameters']['sub'], event['queryStringParameters']['status'])


    elif httpMethod == patchMethod and path == patchExpense:
        response = updateTheExpense(conn, event['body'])


    elif httpMethod == postMethod and path == prepareThePayments:
        response = prepareTheSettelementPayment(conn, event['body'])

    elif httpMethod == postMethod and path == updatePreparePayment:
        response = updateTheSettelementPayment(conn, event['body'])

    elif httpMethod == postMethod and path == changeStatusOfPayment:
        response = changeStatusOfPaymentByDriverAndAdmin(conn, event['body'])

    elif httpMethod == deleteMethod and path == deletePayment:
        response = DeletePaymentWithExpenses(conn, event['body'])

    elif httpMethod == postMethod and path == generateui:
        response = generatePaymentsTemplate(conn, event['body'])

    else:
        return buildResponse(400, 'Error with request')
    return response


def getAllLoadsGeneratedInvoices(connection, sub, driverid, startdate, enddate, limit, offset,filter_search):
    print("Loading", filter_search)
    cursor = connection.cursor()

    if driverid and filter_search and startdate and enddate:
        print("IF")
        cursor.execute(getAllInvoicesWithDriverStatusAndDateRange, (sub, driverid,
                       filter_search, startdate, enddate, int(limit),  (int(limit) * int(offset))))

    elif driverid and filter_search and startdate == '' and enddate == '':
        print("ELIF2")
        cursor.execute(getAllInvoicesWithDriverandStatus, (sub, driverid,
                       filter_search, int(limit),  (int(limit) * int(offset))))
    else:
        print("ELSE")
        return buildResponse(404, 'Path Not Found')
        # print(f"filter search: {filter_search} limit: {limit}  offset: {offset} ")
        
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


def getAllGeneratedDriverPayments(connection, sub, driverid, startdate, enddate, limit, offset):
    cursor = connection.cursor()
    if driverid and startdate and enddate:
        print("IF1")
        cursor.execute(getPaymentWithDriverIdAndDateRange, (sub, driverid, startdate, enddate, int(limit),  (int(limit) * int(offset))))

    elif driverid and startdate == '' and enddate == '':
        print("ELIF2")
        cursor.execute(getPaymentWithDriverId, (sub, driverid, int(limit),  (int(limit) * int(offset))))
    
    elif driverid == '' and startdate == '' and enddate == '':
        print("ELIF3")
        cursor.execute(getAllPaymentsWithSub,
                       (sub, int(limit),  (int(limit) * int(offset))))
    else:
        print("ELSE")
        return buildResponse(404, 'Path Not Found')
    payments = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                payments.append(formatSettlmentResponse(row))
            loadsResponse = {
                "totalDbCount": len(payments),
                "payments": payments
            }
            print(payments)
            return buildResponse(200, loadsResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No payments record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all payments details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all payments details for sub : {}'.format(sub)})


def driverActivePayments(connection, driverid, sub, status):
    cursor = connection.cursor()
    if status == '':
        print("IF")
        cursor.execute(getAllPayments, (driverid, sub))
    else:
        print("ELSE")
        cursor.execute(getPaymentWithStatusFilter, (driverid, sub, status))
    totalActivePayments = []
    try:
        if cursor.rowcount != 0:
            for row in cursor:
                payments = forPaymentDashboardMobileResponse(row)
                totalActivePayments.append(payments)
            paymentResponse = {
                "totalDbCount": len(totalActivePayments),
                "driverId": driverid,
                "sub": sub,
                "activePayments": totalActivePayments
            }
            return buildResponse(200, paymentResponse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'No payment record found for sub : {}'.format(sub)})
    except Exception as ex:
        print("Exception occurred while fetching all payments details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching all payments details for sub : {}'
                             .format(sub)})


def getPaymentByItsPaymentId(connection, paymentId, sub):
    cursor = connection.cursor()
    cursor.execute(getPaymentById, (paymentId, sub))
    try:
        if cursor.rowcount != 0:
            payment = formatSinglePaymentResponse(cursor.fetchone())
            return buildResponse(200, payment)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'payment: {} not found for sub: {}'.format(paymentId, sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while fetching paymentId details for invoiceId : {paymentId} , Error: {ex}'})

def fetchExpensesByDriver(connection, cursor, driverid, sub):
    try:
        cursor.execute("SELECT * FROM dispatch_dev.expense WHERE driverFk = %s and sub = %s AND status != 'DELETED'", (driverid,sub))
        rows = cursor.fetchall()
        expenses = []
        for row in rows:
            expense = formatCreatedExpenseResponse(row)
            expenses.append(expense)
        print("EXPENSES>>>>>>>>>>>>>>>>>>>>>>>>", expenses)
        return expenses
    except pymysql.Error as ex:
        raise Exception(f"Failed to fetch expenses for driver {driverid}: {str(ex)}")

def updateTheSettelementPayment(connection, bodyDetails):
    driverExpenseTotals = []
    loadDetails = []
    invoiceList = []
    emptyObj = {}
    totalRate = 0
    cursor = connection.cursor()
    try:
        bodyParam = json.loads(bodyDetails)
        paymentId = bodyParam['paymentid']
        expenseId = bodyParam['expenseid']
        driverId = bodyParam['driverid']
        sub = bodyParam['sub']
        
        # Get Payments
        cursor.execute("SELECT * FROM dispatch_dev.payments WHERE payment_id = %s and sub = %s", (paymentId, sub))
        row = cursor.fetchone()
        
        if not row:
            error_message = 'Payment not found in the database.'
            return buildResponse(404, {'ErrorMessage': error_message})

        # Fetch the company details for commission calculation.
        cursor.execute("SELECT * FROM dispatch_dev.company WHERE sub = %s", (sub,))
        company_row = cursor.fetchone()
        if company_row:
            companyPhoneNumber = str(company_row[2])
            commissionAmount = float(company_row[6])
            companyAddress = str(company_row[8])
            companyEmail = str(company_row[17])
        else:
            companyPhoneNumber = ''
            commissionAmount = 0.0
            companyEmail = ''
            companyAddress = ''

        # Payment found, proceed with further processing
        payment_id = row[0]
        load_id = json.loads(row[1])
        gross_amount = float(row[2])
        income_adjustment = float(row[3])
        adjustedgrossamount = float(row[4])
        gross_payable = float(row[5])
        less_insurance = float(row[6])
        less_transaction = float(row[7])
        # less_other = float(row[8])
        # income_over_expenses = float(row[9])
        driver_share = float(row[10])
        # add_driver_refunds = float(row[11])
        less_driver_advances = float(row[12])
        # driver_net_settelment = float(row[13])
        status = str(row[14])
        # paysub = str(row[15])
        # payment_url = str(row[16])
        driver1_fk = row[19]
        payment_start_date = str(row[21])
        payment_end_date = str(row[22])

        # Fetch the related invoices.
        for invoiceId in load_id:
            cursor.execute(getAllInvoicesWithSubandId, (invoiceId, sub))
            for row in cursor:
                invoiceList.append(formatInvoiceResponse(row))

        for loadinoice in invoiceList:
            loadInvoiceId = loadinoice['loadId']
            rate = loadinoice['rate']
            loadNumber = loadinoice['loadNumber']
            address = loadinoice['address']
            driverNumber = loadinoice['driverNumber']
            truckNumber = loadinoice['trucknumber']
            trailerNumber = loadinoice['trailernumber']
            driverName = loadinoice['driverFullName']
            loadStatus = loadinoice['status']
            # Check the loadStatus to determine if it's delivered or not
            if loadStatus in ['CREATED', 'ASSIGNED', 'IN TRANSIT', 'ACCEPTED']:
                deliveredOrNot = "NOT DELIVERED"
            else:
                deliveredOrNot = "DELIVERED"
            totalRate += rate
            deduct = rate * commissionAmount / 100
            deductamtforEachLoad = rate - deduct
            loadDetail = {
                'loadId': loadInvoiceId,
                'loadNumber': loadNumber,
                'address': address,
                'rate': rate,
                'deductedRate': deductamtforEachLoad,
                'deliveredOrNot':deliveredOrNot
            }
            loadDetails.append(loadDetail)

        # Fetch Expenses
        expenseIds_str = ', '.join(map(str, expenseId))
        expenseSql = "SELECT * FROM dispatch_dev.expense WHERE expenseId IN ({}) AND sub = %s AND status != 'DELETED' AND driverFk = %s".format(expenseIds_str)
        cursor.execute(expenseSql, (sub, driverId))
        expenses = cursor.fetchall()

        allExpenses = [formatCreatedExpenseResponse(expense) for expense in expenses]
        refund_expenses = []
        deduct_expenses = []
        for expenses in allExpenses:
            if expenses['type'] == 'REFUND':
                refund_expenses.append(expenses)
            elif expenses['type'] == 'DEDUCT':
                deduct_expenses.append(expenses)


        # Calculate driver expense totals
        for expense in deduct_expenses:
            expensetype = expense['expensetype']
            cost = expense['cost']
            if any(d['type'] == expensetype for d in driverExpenseTotals):
                # Update existing entry
                for d in driverExpenseTotals:
                    if d['type'] == expensetype:
                        d['total'] += cost
                        d['count'] += 1
            else:
                driverExpenseTotals.append({'type': expensetype, 'total': cost, 'count': 1})
        
        # Total refund expenses.
        refundExpenseTotal = sum(expense['cost'] for expense in refund_expenses)

        insuranceExpensedriverdeduction = less_insurance       
        for totalExpense in driverExpenseTotals:
            insuranceExpensedriverdeduction += totalExpense['total']   
        print("GROSS__PAYABLE__UPDATE>>>>>>>>>>>>>>>>>>>>", gross_payable)
        driverIncomeOverExpense = gross_payable - insuranceExpensedriverdeduction                           #Updated Deduction of all expenses and insurance
        print("UPDATED_DRIVER__NEW__INCOME__OVER__EXPENSE>>>>>>>>>>>>>>>>>>>>>>>>", driverIncomeOverExpense)
        incomeOverExpenseNew = driverIncomeOverExpense                                                      #Updated Income over expenses and insurance
        addDriverRefundsNew = refundExpenseTotal                                                            #Updated driver refunds
        driverNetSettlement = incomeOverExpenseNew + addDriverRefundsNew                                    #Updated driver net settlement.
        print("UPDATED_DRIVER__NEW__SETTLEMENTS>>>>>>>>>>>>>>>>>>>>>>>>", driverNetSettlement)


        emptyObj['status'] = status
        emptyObj['loadCount'] = len(load_id)
        emptyObj['loadDetails'] = loadDetails
        emptyObj['phoneNumber'] = companyPhoneNumber
        emptyObj['email'] = companyEmail
        emptyObj['currentDate'] = datetime.datetime.now().strftime("%m/%d/%Y")
        emptyObj['startDate'] = payment_start_date
        emptyObj['endDate'] = payment_end_date
        emptyObj['driverExpenses'] = deduct_expenses
        emptyObj['refundExpenses'] = refund_expenses
        emptyObj['driverName'] = driverName
        emptyObj['driverid'] = driver1_fk
        emptyObj['driverNumber'] = driverNumber
        emptyObj['truckNumber'] = truckNumber
        emptyObj['trailerNumber'] = trailerNumber
        emptyObj['subId'] = sub
        emptyObj['companyAddress'] = companyAddress

        # emptyObj['loadInvoiceId'] = json.dumps(load_id)
        emptyObj['grossAmount'] = gross_amount
        emptyObj['incomeAdjustment'] = income_adjustment
        emptyObj['adjustedGrossAmount'] = adjustedgrossamount
        emptyObj['grossPayable'] = gross_payable
        emptyObj['lessInsurance'] = less_insurance
        emptyObj['lessTransaction'] = less_transaction
        emptyObj['incomeOverExpenses'] = incomeOverExpenseNew
        emptyObj['driverShare'] = driver_share
        emptyObj['addDriverRefunds'] = addDriverRefundsNew
        emptyObj['lessDriverAdvances'] = less_driver_advances
        emptyObj['driverNetSettlement'] = driverNetSettlement
        emptyObj['ts_created'] = datetime.datetime.now()
        emptyObj['ts_updated'] = datetime.datetime.now()
        emptyObj['driverExpenseTotals'] = driverExpenseTotals


        relatedExpenseIdUpdate = json.dumps(expenseId)
        # Updated payment PDF
        emptyObj['payment_id'] = payment_id
        paymentUrl = generatePaymentsTemplate(emptyObj, driverId)
        paymentGeneratedUrl = json.loads(paymentUrl['body'])['generatedUrl']
        print("paymentGeneratedUrl", paymentGeneratedUrl)

         # Update the values in the payments table
        update_query = """
        UPDATE dispatch_dev.payments 
        SET income_over_expenses = %s, 
            add_driver_refunds = %s, 
            driver_net_settelment = %s,
            status = 'DRAFT',
            payment_url = %s,
            ts_created = CURRENT_TIMESTAMP, 
            ts_updated = CURRENT_TIMESTAMP,
            expense_deduction = %s,
            related_expensids = %s
        WHERE payment_id = %s AND driver1_fk = %s AND sub = %s;
        """
        cursor.execute(update_query, (driverIncomeOverExpense, addDriverRefundsNew, driverNetSettlement, paymentGeneratedUrl, insuranceExpensedriverdeduction, relatedExpenseIdUpdate, paymentId, driverId, sub))


        # Update the status to DRAFT again when the payment is generated.
        update_load_query = "UPDATE dispatch_dev.load SET payment_status = 'DRAFT', ts_updated = CURRENT_TIMESTAMP WHERE load_id IN %s AND driver1_fk = %s AND sub = %s"
        cursor.execute(update_load_query, (tuple(load_id), driverId, sub))

        # Update the status to DRAFT again when the payment is generated in the "dispatch_dev.expense" table
        update_expense_query = "UPDATE dispatch_dev.expense SET status = 'DRAFT', ts_updated = CURRENT_TIMESTAMP WHERE expenseId IN %s AND driverFk = %s AND sub = %s"
        cursor.execute(update_expense_query, (tuple(expenseId), driverId, sub))


        connection.commit()

        print("PAYMENT__UPDATED__SUCCESSFULLY")
        updateSettlementPayment = {
            "paymentSuccess": "Payment updated successfully"
        }

        cursor.execute("SELECT * FROM dispatch_dev.user_device_tokens WHERE user_id = %s AND sub = %s", (driverId, sub,))
        row = cursor.fetchone()
        if row:
            deviceToken = row[2]
        else:
            deviceToken = ''
        print("TRIGGERING NOTIFICATIONS")
        message = {}
        message['paymentId'] = payment_id
        message['getMessagetitle'] = 'Congratulations! Payment Created for Driver'
        message['getMessagebody'] = 'We are excited to inform you that a payment has been successfully created in your account. please check attached pdf'
        message['userToken'] = deviceToken
        send_notifications(message)

        connection.commit()  # commit the changes to the database
        return buildResponse(200, updateSettlementPayment)

    except Exception as ex:
        print("Exception occurred while updating payment details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching payment details for sub: {}'
                             .format(sub)})


def prepareTheSettelementPayment(connection, bodyDetails):
    print("CHECKSETTLMENTTAB")
    cursor = connection.cursor()
    try:
        invoiceList = []
        bodyParam = json.loads(bodyDetails)
        invoiceIds = bodyParam['invoiceIds']
        expenses = bodyParam['expenses']
        refundExpenses = bodyParam['refundexpenses']
        driverid = bodyParam['driverid']
        startDate = bodyParam['startdate']
        endDate = bodyParam['enddate']
        if startDate and endDate:
            startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d").strftime("%m/%d/%Y")
            endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d").strftime("%m/%d/%Y")
        else:
            startDate = ''
            endDate = ''
        sub = bodyParam['sub']
        if cursor.rowcount != 0:
            for invoiceId in invoiceIds:
                cursor.execute(getAllInvoicesWithSubandId, (invoiceId, sub))
                for row in cursor:
                    invoiceList.append(formatInvoiceResponse(row))

            preparePaymentInvoices(connection, cursor, invoiceList,expenses, refundExpenses, driverid, sub, startDate, endDate)
            loadsResponse = {
                "paymentSuccess": "Payment generated for selected driver and loads"
            }
            connection.commit()  # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while saving payment details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while fetching payment details for sub : {}'
                             .format(sub)})



def preparePaymentInvoices(connection, cursor, invoiceList, expenses, refundExpenses, driverid, sub, startDate, endDate):
    print("OPERATION_STARTED")
    print("driverid", driverid)
    print("INVOICE_LIST", invoiceList)
    print("INVOICE_LIST", sub)
    insertedExpenseIds = [] 
    try:
        connection.begin()
        # Execute the expense insertion query within a transaction
        with connection.cursor() as expense_cursor:
            for expense in expenses:
                values = (
                    driverid,
                    expense['expensetype'],
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                    expense['cost'],
                    expense['status'],
                    expense['sub'],
                    'DEDUCT',
                )
                expense_cursor.execute(insertexpense, values)
                insertedExpenseIds.append(expense_cursor.lastrowid)
        # Execute the expense insertion query for refund expenses within a transaction
        with connection.cursor() as refund_cursor:
            for expense in refundExpenses:
                values = (
                    driverid,
                    expense['expensetype'],
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                    expense['cost'],
                    expense['status'],
                    expense['sub'],
                    'REFUND',
                )
                refund_cursor.execute(insertexpense, values)
                insertedExpenseIds.append(refund_cursor.lastrowid)
        
        # connection.commit()
        print("INSERTEDEXPENSEIDS", insertedExpenseIds)
        # driverExpenses = fetchExpensesByDriver(connection, cursor, driverid, sub)
        insertPaymentId = insertPayment(cursor, invoiceList, expenses, refundExpenses, driverid, sub, startDate, endDate, insertedExpenseIds)
        
        if insertPaymentId is not None:
            print("OPERATION_COMPLETED", insertPaymentId)
            connection.commit()  # Commit transaction
            cursor.close()
            return buildResponse(200, {'Message': 'Payments saved successfully', 'PaymentId': insertPaymentId})
        else:
            print("OPERATION_CANCELLED")
            connection.rollback()
            cursor.close()
            return buildResponse(500, {'Message': 'Failed to insert payments', 'Error': 'Expense insertion failed'})

    except pymysql.Error as ex:
        connection.rollback()
        cursor.close()
        return buildResponse(500, {'Message': 'Exception occurred while saving expenses', 'Error': str(ex)})
    except KeyError as ex:
        connection.rollback()
        cursor.close()
        return buildResponse(400, {'Message': 'Missing required fields', 'Error': str(ex)})



def insertPayment(cursor, invoiceList, driverExpenses, refundExpenses, driverid,  sub, startDate, endDate, insertedExpenseIds):
    allLoadIds = []
    loadDetails = []
    emptyObj = {}
    driverExpenseTotals = []
    totalRate = 0
    print("CHECKEXPENSES")
    try:
        # Fetch the company details for commission calculation.
        cursor.execute("SELECT * FROM dispatch_dev.company WHERE sub = %s", (sub,))
        row = cursor.fetchone()
        if row:
            companyPhoneNumber = str(row[2])
            insuranceAmount = float(row[5])
            commissionAmount = float(row[6])
            companyAddress = str(row[8])
            companyEmail = str(row[17])
        else:
            companyPhoneNumber = ''
            companyEmail = ''
            insuranceAmount = 0.0
            commissionAmount = 0.0
            companyAddress = ''

        
        # Fetch the previous payments for the same driver in the same month
        previous_payments = {}
        previous_payments_query = "SELECT driver1_fk, payment_start_date, payment_end_date FROM dispatch_dev.payments WHERE driver1_fk = %s AND payment_end_date >= %s"
        cursor.execute(previous_payments_query, (driverid, startDate))
        rows = cursor.fetchall()
        for row in rows:
            driver_fk, payment_start_date, payment_end_date = row
            if driver_fk not in previous_payments:
                previous_payments[driver_fk] = []
            previous_payments[driver_fk].append((payment_start_date, payment_end_date))

        # Check if the insurance has been deducted for the given payment period
        insuranceDeducted = any(prev_start_date <= startDate <= prev_end_date for prev_start_date, prev_end_date in previous_payments.get(driverid, []))
        print("CHECKINSURANCEDEDUCTEDORNOT", insuranceDeducted)


        for invoices in invoiceList:
            loadInvoiceId = invoices['loadId']
            rate = invoices['rate']
            loadNumber = invoices['loadNumber']
            address = invoices['address']
            # driverid_2 = invoices['driverid_2']
            driverNumber = invoices['driverNumber']
            truckNumber = invoices['trucknumber']
            trailerNumber = invoices['trailernumber']
            driverName = invoices['driverFullName']
            loadStatus = invoices['status']
             # Check the loadStatus to determine if it's delivered or not
            if loadStatus in ['CREATED', 'ASSIGNED', 'IN TRANSIT', 'ACCEPTED']:
                deliveredOrNot = "NOT DELIVERED"
            else:
                deliveredOrNot = "DELIVERED"
            totalRate += rate

            deduct = rate * commissionAmount / 100
            deductamtforEachLoad = rate - deduct

            allLoadIds.append(loadInvoiceId)
            loadDetail = {
                'loadId': loadInvoiceId,
                'loadNumber': loadNumber,
                'address': address,
                'rate': rate,
                'deductedRate': deductamtforEachLoad,
                'deliveredOrNot':deliveredOrNot
            }
            loadDetails.append(loadDetail)
        
        # Calculate driver expense totals
        for expense in driverExpenses:
            expensetype = expense['expensetype']
            cost = expense['cost']
            if any(d['type'] == expensetype for d in driverExpenseTotals):
                # Update existing entry
                for d in driverExpenseTotals:
                    if d['type'] == expensetype:
                        d['total'] += cost
                        d['count'] += 1
            else:
                driverExpenseTotals.append({'type': expensetype, 'total': cost, 'count': 1})
        
        # Total refund expenses.
        refundExpenseTotal = sum(expense['cost'] for expense in refundExpenses)
        
       
        
        companyDeduction = totalRate * commissionAmount / 100 #Company Percentage calculation
        print("companyDeduction>>>>>>>>>", companyDeduction)
        amountAfterCompanyDeduction = totalRate - companyDeduction

        print("DRIVER__EXPENSE__TOTAL>>>>>>>>>>>>>>>>>>>>", driverExpenseTotals)  
        if insuranceDeducted == True:
            insuranceExpensedriverdeduction = 0.00       
            insuranceAmount = 0.00       
            for totalExpense in driverExpenseTotals:
                insuranceExpensedriverdeduction += totalExpense['total'] 
        else:
            insuranceExpensedriverdeduction = insuranceAmount       
            insuranceAmount = insuranceAmount       
            for totalExpense in driverExpenseTotals:
                insuranceExpensedriverdeduction += totalExpense['total']        #Total of all expenses and insurance

        
        grossAmount = totalRate
        incomeAdjustment = 0.00             #static
        adjustedGrossAmount = totalRate
        grossPayable = amountAfterCompanyDeduction
        lessInsurance = insuranceAmount
        lessTransaction = 0.00              #static
        # lessOther = 500

        print("GROSS__PAYABLE__CREATE>>>>>>>>>>>>>>>>>>>>", grossPayable)
        driverIncomeOverExpense = grossPayable - insuranceExpensedriverdeduction    #Deduction of all expenses and insurance
        print("DRIVER__NEW__SETTLEMENTS>>>>>>>>>>>>>>>>>>>>>>>>", driverIncomeOverExpense)
                   
        incomeOverExpenses = driverIncomeOverExpense
        driverShare = 0.00                  #static
        addDriverRefunds = refundExpenseTotal             #If refund selected in expenses)
        lessDriverAdvances = 0.00            #static 
        driverNetSettlement = driverIncomeOverExpense + addDriverRefunds

        status = 'DRAFT'
        subId = sub


        print("LOAD__DETAILS", loadDetails)
        emptyObj['status'] = status
        emptyObj['loadCount'] = len(loadDetails)
        # emptyObj['alloadNumbers'] = allloadNumbers
        # emptyObj['allAddress'] = allAddress
        emptyObj['loadDetails'] = loadDetails
        emptyObj['phoneNumber'] = companyPhoneNumber
        emptyObj['email'] = companyEmail
        emptyObj['currentDate'] = datetime.datetime.now().strftime("%m/%d/%Y")
        emptyObj['startDate'] = startDate
        emptyObj['endDate'] = endDate
        emptyObj['driverExpenses'] = driverExpenses
        emptyObj['refundExpenses'] = refundExpenses
        emptyObj['driverName'] = driverName
        emptyObj['driverid'] = driverid
        # emptyObj['driverid_2'] = driverid_2
        emptyObj['driverNumber'] = driverNumber
        emptyObj['truckNumber'] = truckNumber
        emptyObj['trailerNumber'] = trailerNumber
        emptyObj['subId'] = subId
        emptyObj['companyAddress'] = companyAddress

        emptyObj['loadInvoiceId'] = json.dumps(allLoadIds)
        emptyObj['allExpenseId'] = json.dumps(insertedExpenseIds)
        emptyObj['grossAmount'] = grossAmount
        emptyObj['incomeAdjustment'] = incomeAdjustment
        emptyObj['adjustedGrossAmount'] = adjustedGrossAmount
        emptyObj['grossPayable'] = grossPayable
        emptyObj['lessInsurance'] = lessInsurance
        emptyObj['lessTransaction'] = lessTransaction
        # emptyObj['lessOther'] = lessOther
        emptyObj['incomeOverExpenses'] = incomeOverExpenses
        emptyObj['driverShare'] = driverShare
        emptyObj['addDriverRefunds'] = addDriverRefunds
        emptyObj['lessDriverAdvances'] = lessDriverAdvances
        emptyObj['driverNetSettlement'] = driverNetSettlement
        emptyObj['ts_created'] = datetime.datetime.now()
        emptyObj['ts_updated'] = datetime.datetime.now()
        emptyObj['driverExpenseTotals'] = driverExpenseTotals
        emptyObj['insuranceExpensedriverdeduction'] = insuranceExpensedriverdeduction

        # print("emptyObj", emptyObj)

        sql_query = '''INSERT INTO dispatch_dev.`payments` (load_id, gross_amount, income_adjustment, adjustedgrossamount, gross_payable, less_insurance, less_transaction, income_over_expenses, driver_share, add_driver_refunds, less_driver_advances, driver_net_settelment, status, sub, ts_created, ts_updated, driver1_fk, payment_start_date, payment_end_date, expense_deduction, related_expensids)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s)'''
        values = (
            emptyObj['loadInvoiceId'],
            emptyObj['grossAmount'],
            emptyObj['incomeAdjustment'],
            emptyObj['adjustedGrossAmount'],
            emptyObj['grossPayable'],
            emptyObj['lessInsurance'],
            emptyObj['lessTransaction'],
            # emptyObj['lessOther'],
            emptyObj['incomeOverExpenses'],
            emptyObj['driverShare'],
            emptyObj['addDriverRefunds'],
            emptyObj['lessDriverAdvances'],
            emptyObj['driverNetSettlement'],
            emptyObj['status'],
            emptyObj['subId'],
            emptyObj['ts_created'],
            emptyObj['ts_updated'],
            emptyObj['driverid'],
            # emptyObj['driverid_2'],
            emptyObj['startDate'],
            emptyObj['endDate'],
            emptyObj['insuranceExpensedriverdeduction'],
            emptyObj['allExpenseId']
        )
        cursor.execute(sql_query, values)
        # Generate the Payments pdf.
        payment_id = cursor.lastrowid
        print("payment_id", payment_id)
        emptyObj['payment_id'] = payment_id
        paymentUrl = generatePaymentsTemplate(emptyObj, driverid)
        paymentGeneratedUrl = json.loads(paymentUrl['body'])['generatedUrl']
        print("paymentGeneratedUrl", paymentGeneratedUrl)
        # Update the payment URL in the payment table
        update_query = "UPDATE dispatch_dev.payments SET payment_url = %s WHERE payment_id = %s and sub = %s"
        update_values = (paymentGeneratedUrl, payment_id, subId)
        cursor.execute(update_query, update_values)

        # Update the selected load status also.
        for invoiceId in allLoadIds:
            updateInvoiceStatus = "update dispatch_dev.load SET payment_status = 'DRAFT', ts_updated = CURRENT_TIMESTAMP where load_id = %s and sub = %s"
            cursor.execute(updateInvoiceStatus, (invoiceId, subId))
        
        if payment_id:
            cursor.execute("SELECT * FROM dispatch_dev.user_device_tokens WHERE user_id = %s AND sub = %s", (driverid, sub,))
            row = cursor.fetchone()
            if row:
                deviceToken = row[2]
            else:
                deviceToken = ''
            print("TRIGGERING NOTIFICATIONS")
            message = {}
            message['paymentId'] = payment_id
            message['getMessagetitle'] = 'Congratulations! Payment Created for Driver'
            message['getMessagebody'] = 'We are excited to inform you that a payment has been successfully created in your account. please check attached pdf'
            message['userToken'] = deviceToken
            send_notifications(message)

        return payment_id
    
    except pymysql.Error as ex:
        raise Exception(f"Failed to insert payment: {str(ex)}")


def getSettelementInvoiceById(connection, invoiceId, sub):
    cursor = connection.cursor()
    cursor.execute(getAllInvoicesWithSubandId, (invoiceId, sub))
    try:
        if cursor.rowcount != 0:
            expnse = formatInvoiceResponse(cursor.fetchone())
            return buildResponse(200, expnse)
        elif cursor.rowcount == 0:
            return buildResponse(200, {'Message': 'invoice: {} not found for sub: {}'.format(invoiceId, sub)})
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while fetching invoice details for invoiceId : {invoiceId} , Error: {ex}'})



# Delete the payment
def DeletePaymentWithExpenses(connection, bodyDetails):
    print("TESTING_DELETE_ENDPOINT")
    bodyParam = json.loads(bodyDetails)
    paymentid = bodyParam['paymentid']
    driverid = bodyParam['driverid']
    sub = bodyParam['sub']
    cursor = connection.cursor()
    try:
        cursor.execute(getPaymentByDriverId, (paymentid, driverid, sub))
        row = cursor.fetchone()
        if row:
            loadIds = json.loads(row[1])
            expenseIds = json.loads(row[24])
        else:
            loadIds = []
        
        if cursor.rowcount != 0:
            updatePaymentStatus = "UPDATE dispatch_dev.payments SET status = 'DELETED', ts_updated = CURRENT_TIMESTAMP WHERE payment_id = %s AND driver1_fk = %s AND sub = %s"
            cursor.execute(updatePaymentStatus, (paymentid, driverid, sub))
            loadsResponse = {"Message": "Payment deleted Successfully"}

            # Update the status to UNPAID of loads WHEN payment is deleted.
            for loadId in loadIds:
                updateLoadStatus = "UPDATE dispatch_dev.load SET payment_status = 'UNPAID', ts_updated = CURRENT_TIMESTAMP WHERE load_id = %s AND sub = %s"
                cursor.execute(updateLoadStatus, (loadId, sub))

            # Update the status of the EXPENSES to DELETED WHEN payment is deleted.
            # driverExpenses = fetchExpensesByDriver(connection, cursor, driverid, sub)
            print("TESTING__DATA", expenseIds)
            for expid in expenseIds:
                # expenseId = exp['expenseId']
                updateExpenseStatus = "UPDATE dispatch_dev.expense SET status = 'DELETED', ts_updated = CURRENT_TIMESTAMP WHERE expenseId = %s AND driverFk = %s AND sub = %s"
                cursor.execute(updateExpenseStatus, (expid, driverid, sub))

            connection.commit()  # commit the changes to the database
            return buildResponse(200, loadsResponse)
    except Exception as ex:
        print("Exception occurred while updating load and payment details for sub: {}, Error: {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while updating load and payment details for sub: {}'.format(sub)})



def changeStatusOfPaymentByDriverAndAdmin(connection, bodyDetails):
    print("TESTING_ENDPOINT")
    bodyParam = json.loads(bodyDetails)
    payment_id = bodyParam['paymentid']
    driver_id = bodyParam['driverid']
    status = bodyParam['status']
    sub = bodyParam['sub']
    try:
        cursor = connection.cursor()
        cursor.execute(getPaymentByDriverId, (payment_id, driver_id, sub))
        row = cursor.fetchone()
        if row:
            load_ids = json.loads(row[1])
            expenseIds = json.loads(row[24])
        else:
            load_ids = []

        if cursor.rowcount != 0 and (status in ['ACCEPTED', 'REJECTED', 'PAID']):
            update_status_response = None
            if status == 'ACCEPTED':
                # updatePayImageInsidePdf()
                updateImageInsidePdf(connection, payment_id, expenseIds, driver_id, sub)
                update_status_response = {"Message": "Payment Accepted"}
            elif status == 'REJECTED':
                update_status_response = {"Message": "Payment Rejected"}
            
            elif status == 'PAID':
                update_status_response = {"Message": "Payment Paid"}

            if update_payment_and_load_status(connection, status, payment_id, driver_id, sub, load_ids, expenseIds):
                return buildResponse(200, update_status_response)

    except Exception as ex:
        print("Exception occurred while updating load and payment details for sub: {}, Error: {}".format(sub, ex))

    return buildResponse(500, {'Message': 'Exception occurred while updating load and payment details for sub: {}'.format(sub)})


def update_payment_and_load_status(connection, status, payment_id, driver_id, sub, load_ids, expenseIds):
    try:
        cursor = connection.cursor()
        update_payment_query = "UPDATE dispatch_dev.payments SET status = %s, ts_updated = CURRENT_TIMESTAMP WHERE payment_id = %s AND driver1_fk = %s AND sub = %s"
        cursor.execute(update_payment_query, (status, payment_id, driver_id, sub))

        update_load_query = "UPDATE dispatch_dev.load SET payment_status = %s, ts_updated = CURRENT_TIMESTAMP WHERE load_id IN %s AND sub = %s"
        cursor.execute(update_load_query, (status, tuple(load_ids), sub))

        update_load_query = "UPDATE dispatch_dev.expense SET status = %s, ts_updated = CURRENT_TIMESTAMP WHERE expenseId IN %s AND sub = %s"
        cursor.execute(update_load_query, (status, tuple(expenseIds), sub))

        connection.commit()
        return True
    except Exception as ex:
        print("Exception occurred while updating load and payment details for sub: {}, Error: {}".format(sub, ex))
        return False


# Update the expense and prepare the payment again.
def updateTheExpense(conn, body):
    try:
        data = json.loads(body)
        expense_id = data.get('expenseId')
        driver_id = data.get('driverId')
        sub = data.get('sub')
        
        if expense_id and driver_id and sub:
            with conn.cursor() as cursor:
                # Check if the expense with the provided expenseId, driverId, and sub exists
                sql = "SELECT * FROM expense WHERE expenseId = %s AND driverFk = %s AND sub = %s"
                cursor.execute(sql, (expense_id, driver_id, sub))
                result = cursor.fetchone()
                if result:
                    # Update the expense based on the provided data
                    update_fields = []
                    params = []
                    for key, value in data.items():
                        if key not in ['expenseId', 'driverId', 'sub', 'tsCreated']:
                            snake_key = camel2snake(key)
                            update_fields.append(f"{snake_key} = %s")
                            params.append(value)
                    if update_fields:
                        params.extend([expense_id, driver_id, sub])
                        sql = f"UPDATE expense SET {', '.join(update_fields)}, ts_updated = CURRENT_TIMESTAMP WHERE expenseId = %s AND driverFk = %s AND sub = %s"
                        cursor.execute(sql, params)
                        conn.commit()
                        response = buildResponse(200, "Expense updated successfully")
                    else:
                        response = buildResponse(400, "No fields to update")
                else:
                    response = buildResponse(404, "Expense not found")
        else:
            response = buildResponse(400, "Missing 'expenseId', 'driverId', or 'sub' field in request body")
    except (pymysql.Error, json.JSONDecodeError) as e:
        response = buildResponse(500, str(e))
    return response



def updateImageInsidePdf(connection, payment_id, expenseIds, driver_id, sub):
    emptyObj = {}
    invoiceList = []
    loadDetails = []
    driverExpenseTotals = []
    totalRate = 0
    try:
        # Get Payments
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM dispatch_dev.payments WHERE payment_id = %s and sub = %s", (payment_id, sub))
        row = cursor.fetchone()
        
        if not row:
            error_message = 'Payment not found in the database.'
            return buildResponse(404, {'ErrorMessage': error_message})

        # Fetch the company details for commission calculation.
        cursor.execute("SELECT * FROM dispatch_dev.company WHERE sub = %s", (sub,))
        company_row = cursor.fetchone()
        if company_row:
            companyPhoneNumber = str(company_row[2])
            commissionAmount = float(company_row[6])
            companyAddress = str(company_row[8])
            companyEmail = str(company_row[17])
        else:
            companyPhoneNumber = ''
            commissionAmount = 0.0
            companyEmail = ''
            companyAddress = ''

        # Payment found, proceed with further processing
        payment_id = row[0]
        load_id = json.loads(row[1])
        gross_amount = float(row[2])
        income_adjustment = float(row[3])
        adjustedgrossamount = float(row[4])
        gross_payable = float(row[5])
        less_insurance = float(row[6])
        less_transaction = float(row[7])
        # less_other = float(row[8])
        # income_over_expenses = float(row[9])
        driver_share = float(row[10])
        # add_driver_refunds = float(row[11])
        less_driver_advances = float(row[12])
        # driver_net_settelment = float(row[13])
        status = str(row[14])
        # paysub = str(row[15])
        # payment_url = str(row[16])
        driver1_fk = row[19]
        payment_start_date = str(row[21])
        payment_end_date = str(row[22])

        # Fetch the related invoices.
        for invoiceId in load_id:
            cursor.execute(getAllInvoicesWithSubandId, (invoiceId, sub))
            for row in cursor:
                invoiceList.append(formatInvoiceResponse(row))

        for loadinoice in invoiceList:
            loadInvoiceId = loadinoice['loadId']
            rate = loadinoice['rate']
            loadNumber = loadinoice['loadNumber']
            address = loadinoice['address']
            driverNumber = loadinoice['driverNumber']
            truckNumber = loadinoice['trucknumber']
            trailerNumber = loadinoice['trailernumber']
            driverName = loadinoice['driverFullName']
            loadStatus = loadinoice['status']
            # Check the loadStatus to determine if it's delivered or not
            if loadStatus in ['CREATED', 'ASSIGNED', 'IN TRANSIT', 'ACCEPTED']:
                deliveredOrNot = "NOT DELIVERED"
            else:
                deliveredOrNot = "DELIVERED"
            totalRate += rate
            deduct = rate * commissionAmount / 100
            deductamtforEachLoad = rate - deduct
            loadDetail = {
                'loadId': loadInvoiceId,
                'loadNumber': loadNumber,
                'address': address,
                'rate': rate,
                'deductedRate': deductamtforEachLoad,
                'deliveredOrNot': deliveredOrNot,
            }
            loadDetails.append(loadDetail)

        # Fetch Expenses
        expenseIds_str = ', '.join(map(str, expenseIds))
        expenseSql = "SELECT * FROM dispatch_dev.expense WHERE expenseId IN ({}) AND sub = %s AND status != 'DELETED' AND driverFk = %s".format(expenseIds_str)
        cursor.execute(expenseSql, (sub, driver_id))
        expenses = cursor.fetchall()

        allExpenses = [formatCreatedExpenseResponse(expense) for expense in expenses]
        refund_expenses = []
        deduct_expenses = []
        for expenses in allExpenses:
            if expenses['type'] == 'REFUND':
                refund_expenses.append(expenses)
            elif expenses['type'] == 'DEDUCT':
                deduct_expenses.append(expenses)


        # Calculate driver expense totals
        for expense in deduct_expenses:
            expensetype = expense['expensetype']
            cost = expense['cost']
            if any(d['type'] == expensetype for d in driverExpenseTotals):
                # Update existing entry
                for d in driverExpenseTotals:
                    if d['type'] == expensetype:
                        d['total'] += cost
                        d['count'] += 1
            else:
                driverExpenseTotals.append({'type': expensetype, 'total': cost, 'count': 1})
        
        # Total refund expenses.
        refundExpenseTotal = sum(expense['cost'] for expense in refund_expenses)

        insuranceExpensedriverdeduction = less_insurance       
        for totalExpense in driverExpenseTotals:
            insuranceExpensedriverdeduction += totalExpense['total']   
        driverIncomeOverExpense = gross_payable - insuranceExpensedriverdeduction                           
        incomeOverExpenseNew = driverIncomeOverExpense                                                      
        addDriverRefundsNew = refundExpenseTotal                                                            
        driverNetSettlement = incomeOverExpenseNew + addDriverRefundsNew 

                            
        emptyObj['status'] = status
        emptyObj['loadCount'] = len(load_id)
        emptyObj['loadDetails'] = loadDetails
        emptyObj['phoneNumber'] = companyPhoneNumber
        emptyObj['email'] = companyEmail
        emptyObj['currentDate'] = datetime.datetime.now().strftime("%m/%d/%Y")
        emptyObj['startDate'] = payment_start_date
        emptyObj['endDate'] = payment_end_date
        emptyObj['driverExpenses'] = deduct_expenses
        emptyObj['refundExpenses'] = refund_expenses
        emptyObj['driverName'] = driverName
        emptyObj['driverid'] = driver1_fk
        emptyObj['driverNumber'] = driverNumber
        emptyObj['truckNumber'] = truckNumber
        emptyObj['trailerNumber'] = trailerNumber
        emptyObj['subId'] = sub
        emptyObj['companyAddress'] = companyAddress

        # emptyObj['loadInvoiceId'] = json.dumps(load_id)
        emptyObj['grossAmount'] = gross_amount
        emptyObj['incomeAdjustment'] = income_adjustment
        emptyObj['adjustedGrossAmount'] = adjustedgrossamount
        emptyObj['grossPayable'] = gross_payable
        emptyObj['lessInsurance'] = less_insurance
        emptyObj['lessTransaction'] = less_transaction
        emptyObj['incomeOverExpenses'] = incomeOverExpenseNew
        emptyObj['driverShare'] = driver_share
        emptyObj['addDriverRefunds'] = addDriverRefundsNew
        emptyObj['lessDriverAdvances'] = less_driver_advances
        emptyObj['driverNetSettlement'] = driverNetSettlement
        emptyObj['ts_created'] = datetime.datetime.now()
        emptyObj['ts_updated'] = datetime.datetime.now()
        emptyObj['driverExpenseTotals'] = driverExpenseTotals
        emptyObj['statuschange'] = 'ACCEPTED'

        # Updated payment PDF
        emptyObj['payment_id'] = payment_id
        paymentUrl = generatePaymentsTemplate(emptyObj, driver_id)
        paymentGeneratedUrl = json.loads(paymentUrl['body'])['generatedUrl']
        print("paymentGeneratedUrl", paymentGeneratedUrl)

         # Update the values in the payments table
        update_query = """
        UPDATE dispatch_dev.payments 
        SET payment_url = %s,
            ts_updated = CURRENT_TIMESTAMP
        WHERE payment_id = %s AND driver1_fk = %s AND sub = %s;
        """
        cursor.execute(update_query, (paymentGeneratedUrl, payment_id, driver_id, sub))
        connection.commit()
        print("PAYMENT_ACCEPTED")
        connection.commit() 
    except Exception as ex:
        print("Exception occurred while approving the payment details for sub: {} , Error : {}".format(sub, ex))
        return buildResponse(500, {'Message': 'Exception occurred while approving payment details for sub: {}'
                             .format(sub)})

def updatePayImageInsidePdf():
    # S3 bucket and file details
    s3_bucket = 'amplify-try-dev-173136-deployment'
    aws_access_key_id = 'AKIAQPUIZQ3OQ5OTUOVG'
    aws_secret_access_key = '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'
    s3_file_key = 'public/payments/Payments_driver_1032_2023-07-24.pdf'
    # https://amplify-try-dev-173136-deployment.s3.amazonaws.com/public/payments/Payments_driver_1032_2023-07-24.pdf
    # Image URLs
    old_image_url = 'https://amplify-try-dev-173136-deployment.s3.amazonaws.com/public/images/DRAFT.png'
    new_image_url = 'https://amplify-try-dev-173136-deployment.s3.amazonaws.com/public/images/APPROVED.png'

    try:
        # Download the PDF file from S3
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=s3_bucket, Key=s3_file_key)
        pdf_data = response['Body'].read()

        print('PDF image URL has been updated and uploaded to S3.1')
 
        # Save the downloaded PDF data to a temporary file
        temp_pdf_path = '/tmp/temp.pdf'
        with open(temp_pdf_path, 'wb') as temp_pdf_file:
            temp_pdf_file.write(pdf_data)
        print('PDF image URL has been updated and uploaded to S3.2')
 
        # Configure pdfkit with the path to wkhtmltopdf executable
        wkhtmltopdf_path = './bin/wkhtmltopdf'  # Replace with the actual path
        configuration = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)
 
        print('PDF image URL has been updated and uploaded to S3.3')

        # Convert the PDF to HTML using pdfkit
        temp_html_path = '/tmp/temp.html'
        print('PDF image URL has been updated and uploaded to S3.4')
        html_content = pdfkit.from_string(pdf_data.decode('latin-1'), False, configuration=configuration, options={'quiet': ''})
        with open(temp_html_path, 'wb') as html_file:
            html_file.write(html_content)
        print('PDF image URL has been updated and uploaded to S3.5')

        # Read the HTML content
        with open(temp_html_path, 'rb') as html_file:
            html_content = html_file.read()

        # Replace the image URL in the HTML content
        html_content = html_content.replace(old_image_url.encode(), new_image_url.encode())

        # Write the modified HTML content back to the file
        with open(temp_html_path, 'wb') as html_file:
            html_file.write(html_content)

        print('PDF image URL has been updated and uploaded to S3.6')

        # Convert the modified HTML back to PDF using pdfkit
        updated_pdf_data = pdfkit.from_file(temp_html_path, False, configuration=configuration)
        print('PDF image URL has been updated and uploaded to S3.7')

        # Upload the updated PDF file back to S3
        presigned_url = upload_to_s3(s3_bucket, temp_pdf_path, aws_access_key_id, aws_secret_access_key, "12")
        print('Uploaded PDF URL:', presigned_url)

        # Clean up temporary files
        # ...

        print('PDF image URL has been updated and uploaded to S3.8')

    except Exception as e:
        print('An error occurred during PDF processing:', str(e))


def upload_to_s3(bucket_name, file_path, aws_access_key_id, aws_secret_access_key, driverid):
    print("INSIDE__UPLOAD", driverid)
    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    # object_name = f"public/payments/{uuid.uuid4()}.pdf"
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    object_name = f"public/payments/Payments_driver_{driverid}_{current_date}.pdf"
    print("object_name:", object_name)
    with open(file_path, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, object_name)
    presigned_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket_name, "Key": object_name},
        ExpiresIn=None,
    )
    return presigned_url

# Set your AWS credentials


def generatePaymentsTemplate(paymentdata, driverid):
    print("Testing")
    path_wkhtmltopdf = './bin/wkhtmltopdf'

    bucket_name = "prashantbuckettest"
    aws_access_key_id = "AKIAQPUIZQ3OQ5OTUOVG"
    aws_secret_access_key = "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"

    try:
        print("OPERATION STARTED")
        # html_content = "<html><body style='background-color: #f2f2f2;'><h1 style='color: blue;'>Hello, World!</h1></body></html>"
        with open('payments.html', 'r') as file:
            html_content = file.read()
        template = Template(html_content)
        rendered_html = template.render(paymentdata=paymentdata)
        options = {
            'page-size': 'A4',
            'margin-top': '0mm',
            'margin-right': '0mm',
            'margin-bottom': '0mm',
            'margin-left': '0mm',
        }

        # Configure pdfkit to use the wkhtmltopdf binary file
        config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)

        pdfkit.from_string(rendered_html, f'/tmp/output_{driverid}.pdf',
                           options=options, configuration=config)
        print("OPERATION COMPLETED")
        presigned_url = upload_to_s3(
            bucket_name, f'/tmp/output_{driverid}.pdf', aws_access_key_id, aws_secret_access_key, driverid)
        pdf_urlx = presigned_url.split('.pdf')[0] + '.pdf'
        loadsResponse = {
            "generatedUrl": pdf_urlx
        }
        os.remove(f'/tmp/output_{driverid}.pdf')
        return buildResponse(200, loadsResponse)
    except Exception as ex:
        return buildResponse(500, {'Message': f'Exception occurred while generating the pdf, Error: {ex}'})


def formatInvoiceResponse(row):
    response = {'invoiceId': row[0], 
                'loadId': row[0], 
                'loadNumber': str(row[1]),
                'estDriveTime': str(row[2]),
                'rate': float(row[3]), 
                'loadMiles': str(row[4]), 
                'address': json.loads(row[5]),
                'rcPath': str(row[6]), 
                'lumper': float(row[7]), 
                'detention': float(row[8]),
                'tonu': float(row[9]),
                'bolPath': str(row[10]), 
                'truckFk': row[11],
                'dispatcherId': row[12],
                'trailorId': row[13],
                'driverid_1': row[14],
                'driverid_2': row[15],
                'tsCreated': str(row[16]), 
                'tsUpdated': str(row[17]), 
                'status': str(row[18]), 
                'sub': str(row[19]),
                'weight': str(row[22]),
                'invoice_url': str(row[23]),
                'payment_status': str(row[24]),
                'truck_license_plate_number': str(row[25]),
                'trailer_license_plate_number': str(row[26]),
                'driverFullName': str(row[27]),
                'dispatcherFullName': str(row[28]),
                'driverNumber': str(row[29]),
                'trucknumber': str(row[30]),
                'trailernumber': str(row[31]),
                }
    return response

def formatSinglePaymentResponse(row):
    print(row)
    response = {
                'payment_id': row[0],
                'loadId': json.loads(row[1]), 
                'driverId': row[2],
                'deductedExpense': float(row[3]), 
                'paymentUrl': str(row[4]), 
                'status': str(row[5]), 
                'grossAmount': float(row[6]), 
                'netSettlment': float(row[7]), 
                'driverRefund': float(row[8]), 
                'payment_start_date': str(row[9]), 
                'payment_end_date': str(row[10]), 
                'allAssociatedLoads': json.loads(row[11]), 
                'allSpecificExpenses': json.loads(row[12]), 
                'driverFullname': str(row[13]),
                'driverNumber': row[14]
                }
    return response


def forPaymentDashboardMobileResponse(row):
    response = {
                'payment_id': row[0],
                'driverNetSettlment': float(row[13]), 
                'status': str(row[14]), 
                'paymentStartDate': str(row[21]), 
                'paymentEndDate': str(row[22]),  
                }
    return response


def formatExpenseResponse(row):
    response = {'expenseId': row[0], 
                'driverFk': row[1],
                'expensetype': str(row[2]), 
                'date': str(row[3]), 
                'cost': float(row[4]),
                }
    return response


def formatCreatedExpenseResponse(row):
    print("formatCreatedExpenseResponse = {}".format(row))
    response = {
        'expenseId': row[0],
        'driverFk': row[1],
        'expensetype': str(row[2]),
        'ts_created': str(row[3]),
        'cost': float(row[4]),
        'status': str(row[5]),
        'sub': str(row[6]),
        'ts_updated': str(row[7]),
        'type': str(row[8]),
    }
    return response


def formatSettlmentResponse(row):
    response = {
                'paymentId': row[0], 
                'loadId': json.loads(row[1]), 
                'driverNetAmount': float(row[13]), 
                'status': str(row[14]), 
                'sub': str(row[15]),
                'paymentUrl': str(row[16]), 
                'tsCreated': str(row[17]), 
                'tsUpdated': str(row[18]), 
                'driverid_1': row[19],
                # 'paymentstartdate': str(row[21]),
                # 'paymentenddate': str(row[22]),
                'driverFullName': str(row[25]),
                'driverNumber': row[26],
                }
    return response


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


def camel2snake(name):
    return re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name)
