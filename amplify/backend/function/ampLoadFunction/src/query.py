getLoadByIdAndSub = "select * from dispatch_dev.load  where load_id = %s and sub = %s  and status != 'DELETED'"

getLoadBySub = """select * from dispatch_dev.load where sub = %s and status != 'DELETED' order by load_id limit %s offset %s """

getLoadByDriverIdAndSub = """select * from dispatch_dev.load where driver1_fk = %s and sub = %s and status != 'DELETED' order by load_id """



getLoadCountBySub = "select count(*) as count from dispatch_dev.load where status != 'DELETED' and  sub = %s"

deleteLoadByIdAndSub = "update dispatch_dev.load set `status` = %s where load_id = %s and sub = %s"

# updateLoadByIdAndSub = "update dispatch_dev.load set ts_updated = %s, %s where driver_id = %s and sub = %s"

insertLoad = """INSERT INTO dispatch_dev.`load` ( load_number, est_drive_time, rate, load_miles, address, rc_path, 
lumper, detention, tonu, bol_path,truck_fk,dispatcher_fk, broker_name,trailer_fk,driver1_fk, driver2_fk,ts_created,
ts_updated,`status`,sub, weight) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

# getDriverLoadHistory = "select * from dispatch_dev.load where driver1_fk = %s and sub = %s and status = %s order by load_id"

getDriverLoadHistory = "select * from dispatch_dev.load where driver1_fk = %s and sub = %s and find_in_set(status,%s) order by load_id"
getDriverLoadHistoryCountBySubandDriverid = "select count(*) from dispatch_dev.load where driver1_fk = %s and sub = %s and find_in_set(status,%s)"


getDriverByIdAndSub = "select driver_id, full_name,sub from dispatch_dev.driver where driver_id = %s and sub = %s"
getTrailerById = """select trailer_id,license_plate_number, sub from dispatch_dev.trailer  where trailer_id = %s and sub = %s """
getDispatcherByIdAndSub = "select dispatcher_id, full_name,sub from dispatch_dev.dispatcher where dispatcher_id = %s and sub = %s"
getDispatcherPaymentsByIdAndSub = "select dispatcher_id, full_name,sub, dispatcher_type, commission_percentage, monthly_salary from dispatch_dev.dispatcher where dispatcher_id = %s and sub = %s"
# getBrokerByIdAndSub = "select broker_id, name,sub from dispatch_dev.broker where broker_id = %s and sub = %s"



getLoadsBySubView = """
SELECT ll.load_id AS loadId, ll.load_number AS loadNumber, ll.address AS address, ll.status AS status, ll.rate AS rate, ll.truck_fk AS truck_fk, ll.driver1_fk AS driver1_fk, ll.driver2_fk AS driver2_fk, ll.trailer_fk AS trailer_fk, ll.ts_updated AS updated, 
driver1.full_name AS primaryDriverName,dispatcher.full_name AS dispatcherName,truck.license_plate_number AS truckPlateNumber, truck.truck_number AS truckNumber, trailer.license_plate_number AS trailerPlateNumber, trailer.trailer_number AS trailerNumber,count(*) OVER() 
AS count FROM dispatch_dev.load ll 
LEFT OUTER JOIN dispatch_dev.driver driver1 ON ll.driver1_fk = driver1.driver_id 
LEFT OUTER JOIN dispatch_dev.truck truck ON ll.truck_fk = truck.truck_id 
LEFT OUTER JOIN dispatch_dev.trailer trailer ON ll.trailer_fk = trailer.trailer_id
LEFT JOIN dispatch_dev.dispatcher dispatcher ON ll.dispatcher_fk = dispatcher.dispatcher_id 
WHERE ll.load_number LIKE %s 
AND ll.status LIKE %s 
AND (driver1.full_name LIKE %s OR driver1.full_name IS NULL) 
AND dispatcher.full_name LIKE %s 
AND (truck.license_plate_number LIKE %s OR truck.license_plate_number IS NULL)
AND (trailer.license_plate_number LIKE %s OR trailer.license_plate_number IS NULL)
AND ll.sub = %s 
AND ll.status != 'DELETED'
AND ll.status != 'FACTORED'
ORDER BY JSON_EXTRACT(ll.address, '$[0].stopDate') DESC 
LIMIT %s 
OFFSET %s
""" 

getDriverLoadHistory = "select * from dispatch_dev.load where driver1_fk = %s and sub = %s and find_in_set(status,%s) order by ts_created DESC"
getDriverLoadHistoryCountBySubandDriverid = "select count(*) as count from dispatch_dev.load where driver1_fk = %s and sub = %s and find_in_set(status,%s)"


getAllInvoicesWithSub = """SELECT dispatch_dev.load.*, truck.license_plate_number as truck_license_plate_number, trailer.license_plate_number as trailer_license_plate_number, driver.full_name as fullname, driver.driver_number as driver_number, dispatcher.full_name as dispatcherfullname FROM dispatch_dev.load
LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id
LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
WHERE dispatch_dev.load.status = 'INVOICED' and dispatch_dev.load.sub = %s ORDER BY JSON_EXTRACT(dispatch_dev.load.address, '$[0].stopDate') DESC LIMIT %s OFFSET %s"""

getAllInvoicesWithSubSearch = """SELECT dispatch_dev.load.*, truck.license_plate_number as truck_license_plate_number, trailer.license_plate_number as trailer_license_plate_number, driver.full_name as fullname, driver.driver_number as driver_number, dispatcher.full_name as dispatcherfullname FROM dispatch_dev.load
LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id
LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
WHERE dispatch_dev.load.sub = %s AND dispatch_dev.load.status = %s ORDER BY JSON_EXTRACT(dispatch_dev.load.address, '$[0].stopDate') DESC LIMIT %s OFFSET %s"""

getAllInvoicesWithSubandId = """SELECT dispatch_dev.load.*, truck.license_plate_number as truck_license_plate_number, trailer.license_plate_number as trailer_license_plate_number, driver.full_name as fullname, driver.driver_number as driver_number, dispatcher.full_name as dispatcherfullname FROM dispatch_dev.load
LEFT JOIN dispatch_dev.truck ON dispatch_dev.load.truck_fk = truck.truck_id
LEFT JOIN dispatch_dev.trailer ON dispatch_dev.load.trailer_fk = trailer.trailer_id 
LEFT JOIN dispatch_dev.driver ON dispatch_dev.load.driver1_fk = driver.driver_id
LEFT JOIN dispatch_dev.dispatcher ON dispatch_dev.load.dispatcher_fk = dispatcher.dispatcher_id
where dispatch_dev.load.load_id = %s and dispatch_dev.load.sub = %s"""




getNotificationDriverIdAndSub = """select * from dispatch_dev.load where driver1_fk = %s and sub = %s and status = 'ASSIGNED' order by ts_updated DESC """


# New Mobile Query

getAllLoadsDataWithStatus="""SELECT * FROM dispatch_dev.load WHERE driver1_fk = %s AND sub = %s AND status IN ('ACCEPTED','READY FOR REVIEW','BOL PENDING','COMPLETED', 'IN TRANSIT','INVOICED') AND status != 'DELETED' ORDER BY ts_created DESC"""




# This two query might need to remove in future versions.
getLoadBySubPaymentdatewise = """select * from dispatch_dev.load where sub = %s and status = 'INVOICED' and ts_created between %s and %s order by load_id limit %s offset %s """
getLoadBySubPayment = """select * from dispatch_dev.load where sub = %s and status = 'INVOICED' order by load_id limit %s offset %s """

