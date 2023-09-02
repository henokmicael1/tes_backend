getTrailerById = "select * from dispatch_dev.trailer  where trailer_id = %s and sub = %s and status = 'ACTIVE'"
getTrailersBySub = """select * from dispatch_dev.trailer where  sub = %s and status = 'ACTIVE'
order by ts_created DESC limit %s offset %s """
getTrailerCountBySub = "select count(*) as count from dispatch_dev.trailer where sub = %s and status = 'ACTIVE'"
deleteTrailerByIdAndSub = "update dispatch_dev.trailer set `status` = %s where trailer_id = %s and sub = %s"

insertTrailer = """INSERT INTO dispatch_dev.trailer ( trailer_reg_exp, license_plate_number, fed_annual_inspection_exp, 
state_annual_inspection_exp, vehicle_fk, ts_created, ts_updated , sub, 
trailer_type,insure_start_dt,insure_end_dt,vin,documents) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s); """

getTrailerPlateNumAndIdBySub = """select trailer_id,license_plate_number from dispatch_dev.trailer where  sub = %s and status = 'ACTIVE'
order by trailer_id limit %s offset %s"""

getVehicleById = "SELECT * FROM dispatch_dev.vehicle_type where vehicle_id = %s"
