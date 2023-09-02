insertAccountant = """INSERT INTO dispatch_dev.accountantdetails (first_name, last_name, phone_number, email, 
birth_date, role, license_number, license_exp_date, address1, address2, state, city, country, zip_code, ts_created, 
ts_updated, documents, sub, full_name, status, accountant_type, commission_percentage, monthly_salary) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s) """


getAccountantsBySub = """select * from dispatch_dev.accountantdetails where sub = %s and status = 'ACTIVE' order by ts_created DESC limit %s offset %s """
getAccountantByIdandSub = """select * from dispatch_dev.accountantdetails where accountantId = %s and sub = %s """
deleteAccountantByIdAndSub = "update dispatch_dev.accountantdetails set `status` = %s where accountantId = %s and sub = %s"


getAccountantCountBySub = "select count(*) from dispatch_dev.accountantdetails where sub = %s and status = 'ACTIVE'"