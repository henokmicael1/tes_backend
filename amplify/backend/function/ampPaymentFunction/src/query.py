
# GET BY ID SETTELEMENT INVOICES
getAllInvoicesWithSubandId = """SELECT dispatch_dev.load.*, truck.license_plate_number as truck_license_plate_number, trailer.license_plate_number as trailer_license_plate_number, driver.full_name as fullname, dispatcher.full_name as dispatcherfullname, driver.driver_number as drivernumber, truck.truck_number as trucknumber, trailer.trailer_number as trailernumber FROM dispatch_dev.load
LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id
LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
WHERE dispatch_dev.load.load_id = %s AND dispatch_dev.load.sub = %s AND dispatch_dev.load.status='INVOICED'"""


# FILTER WITH DRIVER AND STATUS
getAllInvoicesWithDriverandStatus = """SELECT dispatch_dev.load.*, truck.license_plate_number as truck_license_plate_number, trailer.license_plate_number as trailer_license_plate_number, driver.full_name as fullname, dispatcher.full_name as dispatcherfullname, driver.driver_number as drivernumber, truck.truck_number as trucknumber, trailer.trailer_number as trailernumber FROM dispatch_dev.load
LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id
LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
WHERE dispatch_dev.load.sub = %s AND dispatch_dev.load.driver1_fk = %s AND dispatch_dev.load.status LIKE %s AND dispatch_dev.load.status !='FACTORED' AND dispatch_dev.load.payment_status NOT IN ('DRAFT', 'ACCEPTED', 'PAID') AND dispatch_dev.load.status != 'DELETED' ORDER BY JSON_EXTRACT(dispatch_dev.load.address, '$[0].stopDate') DESC LIMIT %s OFFSET %s"""

# # FILTER WITH DRIVER, STATUS, DATE RANGE
# getAllInvoicesWithDriverStatusAndDateRange = """SELECT dispatch_dev.load.*, truck.license_plate_number as truck_license_plate_number, trailer.license_plate_number as trailer_license_plate_number, driver.full_name as fullname, dispatcher.full_name as dispatcherfullname FROM dispatch_dev.load
# LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk= truck.truck_id
# LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id
# LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
# LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
# WHERE dispatch_dev.load.sub = %s AND dispatch_dev.load.driver1_fk = %s AND dispatch_dev.load.status LIKE %s AND dispatch_dev.load.status !='FACTORED' AND dispatch_dev.load.ts_created between %s and %s ORDER BY dispatch_dev.load.ts_updated DESC LIMIT %s OFFSET %s"""

# FILTER WITH DRIVER, STATUS, DATE RANGE
getAllInvoicesWithDriverStatusAndDateRange = """SELECT dispatch_dev.load.*, truck.license_plate_number AS truck_license_plate_number, trailer.license_plate_number AS trailer_license_plate_number, driver.full_name AS fullname, dispatcher.full_name AS dispatcherfullname, driver.driver_number as drivernumber, truck.truck_number as trucknumber, trailer.trailer_number as trailernumber
FROM dispatch_dev.load
LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id
LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
WHERE dispatch_dev.load.sub = %s
  AND dispatch_dev.load.driver1_fk = %s
  AND dispatch_dev.load.status LIKE %s
  AND dispatch_dev.load.status != 'FACTORED'
  AND dispatch_dev.load.payment_status NOT IN ('DRAFT', 'ACCEPTED', 'PAID') AND dispatch_dev.load.status != 'DELETED'
  AND JSON_EXTRACT(dispatch_dev.load.address, '$[0].locationType') = 'Pickup'
  AND JSON_EXTRACT(dispatch_dev.load.address, '$[0].stopDate') >= %s
  AND JSON_EXTRACT(dispatch_dev.load.address, '$[0].stopDate') <= %s
ORDER BY JSON_EXTRACT(dispatch_dev.load.address, '$[0].stopDate') DESC LIMIT %s OFFSET %s"""


# Get all driver expenses
getAllDriverExpensesByDriverId = """select expense.*, driver.first_name, driver.last_name from dispatch_dev.expense LEFT JOIN dispatch_dev.driver ON expense.driverFk = driver.driver_id where expense.driverFk = %s and expense.sub = %s and expense.status != 'DELETED'"""

insertexpense = "INSERT INTO dispatch_dev.`expense` (driverFk, expensetype, ts_created, ts_updated, cost, status, sub, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

getPaymentByDriverId = "select * from dispatch_dev.payments where payment_id = %s and driver1_fk = %s and sub = %s"


getPaymentWithDriverId = """SELECT dispatch_dev.payments.*, driver.full_name AS fullname, driver.driver_number AS drivernumber
FROM dispatch_dev.payments
LEFT JOIN dispatch_dev.driver ON dispatch_dev.payments.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.load ON FIND_IN_SET(dispatch_dev.load.load_id, dispatch_dev.payments.load_id)
WHERE dispatch_dev.payments.sub = %s AND dispatch_dev.payments.driver1_fk = %s
AND dispatch_dev.payments.status <> 'DELETED'
ORDER BY dispatch_dev.payments.ts_created DESC
LIMIT %s OFFSET %s
"""


getPaymentWithDriverIdAndDateRange = """SELECT dispatch_dev.payments.*, driver.full_name AS fullname, driver.driver_number AS drivernumber
FROM dispatch_dev.payments
LEFT JOIN dispatch_dev.driver ON dispatch_dev.payments.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.load ON FIND_IN_SET(dispatch_dev.load.load_id, dispatch_dev.payments.load_id)
WHERE dispatch_dev.payments.sub = %s AND dispatch_dev.payments.driver1_fk = %s
AND dispatch_dev.payments.ts_created >= %s
AND dispatch_dev.payments.ts_created <= DATE_ADD(%s, INTERVAL 1 DAY)
AND dispatch_dev.payments.status <> 'DELETED'
ORDER BY dispatch_dev.payments.ts_created DESC
LIMIT %s OFFSET %s
"""


getAllPaymentsWithSub = """SELECT dispatch_dev.payments.*, driver.full_name AS fullname, driver.driver_number AS drivernumber
FROM dispatch_dev.payments
LEFT JOIN dispatch_dev.driver ON dispatch_dev.payments.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.load ON FIND_IN_SET(dispatch_dev.load.load_id, dispatch_dev.payments.load_id)
WHERE dispatch_dev.payments.sub = %s
AND dispatch_dev.payments.status <> 'DELETED'
ORDER BY dispatch_dev.payments.ts_created DESC
LIMIT %s OFFSET %s
"""


getPaymentById = """
SELECT
  dispatch_dev.payments.payment_id,
  dispatch_dev.payments.load_id,
  dispatch_dev.payments.driver1_fk,
  dispatch_dev.payments.expense_deduction,
  dispatch_dev.payments.payment_url,
  dispatch_dev.payments.status,
  dispatch_dev.payments.gross_payable,
  dispatch_dev.payments.driver_net_settelment,
  dispatch_dev.payments.add_driver_refunds,
  dispatch_dev.payments.payment_start_date,
  dispatch_dev.payments.payment_end_date,
  COALESCE(
    (
      SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
          'loadId', dispatch_dev.load.load_id,
          'load_number', dispatch_dev.load.load_number,
          'invoice_url', dispatch_dev.load.invoice_url,
          'rate', dispatch_dev.load.rate,
          'truck', dispatch_dev.load.truck_fk,
          'dispatcher_fk', dispatch_dev.load.dispatcher_fk,
          'trailer_fk', dispatch_dev.load.trailer_fk,
          'address', dispatch_dev.load.address,
          'status', dispatch_dev.load.status,
          'truck_number', truck.truck_number,
          'license_plate_number', truck.license_plate_number,
          'trailer_number', trailer.trailer_number,  
          'trailer_license_plate', trailer.license_plate_number,
          'dispatcher_name', dispatcher.full_name,
          'driver_full_name', driver.full_name,
          'driver_number', driver.driver_number,
          'load_miles', dispatch_dev.load.load_miles
        )
      )
      FROM dispatch_dev.load
      LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
      LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id 
      LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
      WHERE JSON_CONTAINS(dispatch_dev.payments.load_id, CAST(dispatch_dev.load.load_id AS JSON))
    ),
    JSON_ARRAY()
  ) AS allLoads,
  COALESCE(
    (
      SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
          'expenseId', expense.expenseId,
          'driverFk', expense.driverFk,
          'expenseAmount', expense.cost,
          'expenseType', expense.expensetype,
          'type', expense.type,
          'ts_created', expense.ts_created,
          'status', expense.status,
          'driverName', driver.full_name
        )
      )
      FROM dispatch_dev.expense
      WHERE expense.driverFk = dispatch_dev.payments.driver1_fk
      AND expense.status <> 'DELETED' 
      AND JSON_CONTAINS(CAST(dispatch_dev.payments.related_expensids AS JSON), CAST(expense.expenseId AS JSON))
    ),
    JSON_ARRAY()
  ) AS associatedExpenses,
  driver.full_name AS fullname,
  driver.driver_number AS drivernumber
FROM
  dispatch_dev.payments
LEFT JOIN
  dispatch_dev.driver ON dispatch_dev.payments.driver1_fk = driver.driver_id
WHERE
  dispatch_dev.payments.payment_id = %s
  AND dispatch_dev.payments.sub = %s;

"""
getAllPayments = """SELECT * FROM dispatch_dev.payments where driver1_fk = %s and sub = %s ORDER BY ts_created DESC;"""

getPaymentWithStatusFilter = """SELECT *
FROM dispatch_dev.payments
WHERE driver1_fk = %s AND sub = %s
  AND FIND_IN_SET(status, %s)
ORDER BY ts_created DESC;"""