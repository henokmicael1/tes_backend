getAllStates = "SELECT distinct state_code,state_name FROM dispatch_dev.us_states;"
getCitiesByState = """select distinct city from dispatch_dev.us_cities city 
join dispatch_dev.us_states state on state.ID = city.ID_STATE where state.STATE_CODE = %s"""
getVehicleModelByMake = """select distinct model from dispatch_dev.vehicle_type where make = %s order by model 
limit %s offset %s """
getAllVehicleMake = "select distinct make from dispatch_dev.vehicle_type order by make"
getVehicleModelByMake = "select distinct model from dispatch_dev.vehicle_type where make = %s order by model"
getVehicleYearByMakeAndModel = """select distinct vehicle_id,year from dispatch_dev.vehicle_type where make =  %s
and model = %s order by year """
getVehicleById = "select * from dispatch_dev.vehicle_type where vehicle_id = %s"
getAllPricePlans = "SELECT * FROM dispatch_dev.pricing_plans"

insertContactDetails = """INSERT INTO dispatch_dev.contacts (fullname, email, phone_number, 
describe_issue, driver1_fk, sub, ts_created, ts_updated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s); """
