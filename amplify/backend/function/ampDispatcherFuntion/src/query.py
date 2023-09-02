getDispatcherByIdAndSub = """select * from dispatch_dev.dispatcher where dispatcher_id = %s and sub = %s 
and status = 'ACTIVE' """
getDispatcherBySub = """select * from dispatch_dev.dispatcher where sub = %s and status = 'ACTIVE' 
order by ts_created DESC limit %s offset %s """
getDispatcherCountBySub = "select count(*) from dispatch_dev.dispatcher where sub = %s and status = 'ACTIVE'"
# deleteDriverByIdAndSub = "delete from dispatch_dev.driver where driver_id = %s and sub = %s"
deleteDispatcherByIdAndSub = "update dispatch_dev.dispatcher set `status` = %s where dispatcher_id = %s and sub = %s"
updateDispatcherByIdAndSub = """update dispatch_dev.dispatcher set ts_updated = %s, %s where dispatcher_id = %s and 
sub = %s """


# getDriverDetailByIdAndSub = """select * from dispatch_dev.driver driver join dispatch_dev.employee employee join
# dispatch_dev.address address on driver.employee_fk = employee.employee_id and employee.address_fk =
# address.address_id where driver.driver_id = %s and driver.sub = %s"""
#
# getDriversDetailBySub = """select * from dispatch_dev.driver driver join dispatch_dev.employee employee join
# dispatch_dev.address address on driver.employee_fk = employee.employee_id and employee.address_fk =
# address.address_id where driver.sub = %s order by driver.driver_id limit %s offset %s """


# insertAddress = """INSERT INTO dispatch_dev.address (address_line_1,address_line_2,state, city, country, zip_code,
# ts_created, ts_updated, status, sub) VALUES ( %s, %s, %s, %s, %s, %s,%s,%s, %s, %s ); """
#
# insertEmployee = """INSERT INTO dispatch_dev.employee (first_name, middle_name, last_name,phone_number, email,
# birth_date, address_fk, ts_created, ts_updated,employee_type, `status`, sub)
# VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """

insertDispatcher = """INSERT INTO dispatch_dev.dispatcher (first_name, last_name, phone_number, email, 
birth_date, role, license_number, license_exp_date, address1, address2, state, city, country, zip_code, ts_created , 
ts_updated, dispatcher_type, commission_percentage, monthly_salary, documents, sub, full_name) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s) """

