insertCompanyDetails = """INSERT INTO dispatch_dev.companydetails (commission, insurancerate, factoringinfo, companyname, companyaddress, ts_created, ts_updated, status, sub, telephone, fax) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
getCompanyByIdandSub = """select * from dispatch_dev.companydetails where companyId = %s and sub = %s """
getCompanyBySub = """select * from dispatch_dev.companydetails where sub = %s """

