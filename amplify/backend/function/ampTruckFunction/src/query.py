getTruckById = "select * from dispatch_dev.truck  where truck_id = %s and sub = %s and status = 'ACTIVE'"
getTrucksBySub = """select * from dispatch_dev.truck where  sub = %s and status = 'ACTIVE'
ORDER BY ts_created DESC limit %s offset %s """

# getTrucksBySubView = """select truck.truck_id as truckId,truck.license_plate_number as truckPlateNumber, truck.truck_number as truckNumber,
# driver1.full_name as primaryDriverName,trailer.license_plate_number as trailerPlateNumber, trailer.trailer_number as trailerNumber, truck.solo,count(*) OVER() 
# AS count from dispatch_dev.truck truck left join dispatch_dev.driver driver1 on truck.driver1_fk = driver1.driver_id 
# left join dispatch_dev.trailer trailer on truck.trailer_fk = trailer.trailer_id where truck.truck_number like %s and 
# truck.license_plate_number like %s and driver1.full_name like %s and trailer.license_plate_number like  %s and 
# truck.status = 'ACTIVE' and truck.solo like %s and truck.sub = %s order by truck.ts_created DESC limit %s offset %s """

getTrucksBySubView = """SELECT truck.truck_id AS truckId, truck.license_plate_number AS truckPlateNumber, truck.truck_number AS truckNumber,
driver1.full_name AS primaryDriverName, trailer.license_plate_number AS trailerPlateNumber, trailer.trailer_number AS trailerNumber, truck.solo, count(*) OVER() AS count
FROM dispatch_dev.truck truck
LEFT JOIN dispatch_dev.driver driver1 ON truck.driver1_fk = driver1.driver_id
LEFT JOIN dispatch_dev.trailer trailer ON truck.trailer_fk = trailer.trailer_id
WHERE truck.truck_number LIKE %s
  AND truck.license_plate_number LIKE %s
  AND driver1.full_name LIKE %s
  AND (trailer.license_plate_number LIKE %s OR trailer.license_plate_number IS NULL)
  AND truck.status = 'ACTIVE'
  AND truck.solo LIKE %s
  AND truck.sub = %s
ORDER BY truck.ts_created DESC
LIMIT %s OFFSET %s
"""




getTruckCountBySub = "select count(*) as count from dispatch_dev.truck where sub = %s and status = 'ACTIVE'"
deleteTruckByIdAndSub = "update dispatch_dev.truck set `status` = %s where truck_id = %s and sub = %s"

insertTruck = """INSERT INTO dispatch_dev.truck ( truck_reg_exp, license_plate_number, fed_annual_inspection_exp, 
state_annual_inspection_exp, driver1_fk, driver2_fk, trailer_fk, vehicle_fk, ts_created, ts_updated , sub, 
solo,insure_start_dt,insure_end_dt,vin,documents) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s ); """

getDriverByIdAndSub = "select driver_id, full_name,sub from dispatch_dev.driver where driver_id = %s and sub = %s"

getTrailerById = """select trailer_id,license_plate_number, sub from dispatch_dev.trailer  where trailer_id = %s and 
sub = %s """

getVehicleById = "SELECT * FROM dispatch_dev.vehicle_type where vehicle_id = %s"
