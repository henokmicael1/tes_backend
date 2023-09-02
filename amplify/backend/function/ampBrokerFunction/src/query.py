getBrokerByIdAndSub = "select * from dispatch_dev.broker  where broker_id = %s and sub = %s"
getBrokerBySub = "select * from dispatch_dev.broker where sub = %s order by broker_id limit %s offset %s "
getBrokerCountBySub = "select count(*) from dispatch_dev.broker where sub = %s"
# deleteBrokerByIdAndSub = "delete from dispatch_dev.broker where broker_id = %s and sub = %s"
deleteBrokerByIdAndSub = "update dispatch_dev.broker set `status` = %s where broker_id = %s and sub = %s"
updateBrokerByIdAndSub = "update dispatch_dev.broker set ts_updated = %s, %s where broker_id = %s and sub = %s"


# getBrokerDetailByIdAndSub = """select * from dispatch_dev.broker broker join dispatch_dev.employee employee join
# dispatch_dev.address address on broker.employee_fk = employee.employee_id and employee.address_fk =
# address.address_id where broker.broker_id = %s and broker.sub = %s"""
#
# getBrokersDetailBySub = """select * from dispatch_dev.broker broker join dispatch_dev.employee employee join
# dispatch_dev.address address on broker.employee_fk = employee.employee_id and employee.address_fk =
# address.address_id where broker.sub = %s order by broker.broker_id limit %s offset %s """


# insertAddress = """INSERT INTO dispatch_dev.address (address_line_1,address_line_2,state, city, country, zip_code,
# ts_created, ts_updated, status, sub) VALUES ( %s, %s, %s, %s, %s, %s,%s,%s, %s, %s ); """
#
# insertEmployee = """INSERT INTO dispatch_dev.employee (first_name, middle_name, last_name,phone_number, email,
# birth_date, address_fk, ts_created, ts_updated,employee_type, `status`, sub)
# VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """

insertBroker = """INSERT INTO dispatch_dev.broker( name, phone_number, email,address1, address2, 
state, city, country, zip_code, ts_created, ts_updated, 
`status`, sub) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); """
