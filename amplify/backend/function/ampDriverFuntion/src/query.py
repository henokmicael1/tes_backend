getDriverByIdAndSub = """
SELECT
  dispatch_dev.driver.*,
  dispatch_dev.truck.truck_id AS truck_id,
  dispatch_dev.truck.truck_reg_exp AS truck_reg_exp,
  dispatch_dev.truck.license_plate_number AS license_plate_number,
  dispatch_dev.truck.fed_annual_inspection_exp AS fed_annual_inspection_exp,
  dispatch_dev.truck.state_annual_inspection_exp AS state_annual_inspection_exp,
  dispatch_dev.truck.trailer_fk AS trailer_fk,
  dispatch_dev.truck.solo AS solo,
  dispatch_dev.truck.insure_start_dt AS insure_start_dt,
  dispatch_dev.truck.insure_end_dt AS insure_end_dt,
  dispatch_dev.truck.vin AS vin,
  vehicle_type_truck.make AS truck_vehicle_make,
  vehicle_type_truck.model AS truck_vehicle_model,
  vehicle_type_truck.year AS truck_vehicle_year
FROM dispatch_dev.driver
LEFT JOIN dispatch_dev.truck ON dispatch_dev.driver.driver_id = dispatch_dev.truck.driver1_fk
LEFT JOIN dispatch_dev.vehicle_type AS vehicle_type_truck ON dispatch_dev.truck.vehicle_fk = vehicle_type_truck.vehicle_id
WHERE dispatch_dev.driver.driver_id = %s AND dispatch_dev.driver.sub = %s;

"""


getTrailerforEachTruck = """SELECT
  trailer.trailer_id AS trailer_id,
  trailer.license_plate_number AS trailer_license_plate_number,
  trailer.fed_annual_inspection_exp AS trailer_fed_annual_inspection_exp,
  trailer.vin AS trailer_vin,
  trailer.state_annual_inspection_exp AS trailer_state_annual_inspection_exp,
  trailer.trailer_reg_exp AS trailer_reg_exp,
  trailer.insure_start_dt AS trailer_insure_start_dt,
  trailer.insure_end_dt AS trailer_insure_end_dt,
  trailer.trailer_type AS trailer_type,
  vehicle_type_trailer.make AS trailer_vehicle_make,
  vehicle_type_trailer.model AS trailer_vehicle_model,
  vehicle_type_trailer.year AS trailer_vehicle_year
FROM dispatch_dev.trailer
LEFT JOIN dispatch_dev.vehicle_type AS vehicle_type_trailer ON dispatch_dev.trailer.vehicle_fk = vehicle_type_trailer.vehicle_id
WHERE trailer.trailer_id = %s AND trailer.sub = %s;""" 


getDriverBySub = """select * from dispatch_dev.driver where sub = %s and status <> 'DELETED' 
order by ts_created DESC limit %s offset %s """


getDriverCountBySub = "select count(*) as count from dispatch_dev.driver where sub = %s and status <> 'DELETED'"

deleteDriverByIdAndSub = "update dispatch_dev.driver set `status` = %s where driver_id = %s and sub = %s"

updateDriverByIdAndSub = "update dispatch_dev.driver set ts_updated = %s, %s where driver_id = %s and sub = %s"

insertDriver = """INSERT INTO dispatch_dev.driver( first_name, full_name, last_name, phone_number, email, 
birth_date, cdl_license_number, cdl_license_exp_date, cdl_state, ein_number, med_card_exp, mvr_exp, drug_clear_exp , 
address1, address2, state, city, country, zip_code, ts_created, ts_updated, documents,
sub) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ); """
