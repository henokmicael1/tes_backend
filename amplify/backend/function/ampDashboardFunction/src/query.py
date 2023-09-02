# Count with Dispatcher ID AND SUB.
getLoadCountBySub = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and status = 'CREATED'"
getAssignedLoadLoadCountBySub = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and status = 'ASSIGNED'"
getInTransistLoadLoadCountBySub = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and status = 'IN TRANSIT'"
invoicesLoadCountBySub = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and status = 'INVOICED'"
completedLoadCountBySub = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and status = 'COMPLETED'"
totalCountWithSubAndId = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"

# Count with only SUB.
getLoadCountBySubOnlySub = "select count(*) from dispatch_dev.load where sub = %s and status = 'CREATED'"
getAssignedLoadCountByOnlySub = "select count(*) from dispatch_dev.load where sub = %s and status = 'ASSIGNED'"
getInTransistLoadCountByOnlySub = "select count(*) from dispatch_dev.load where sub = %s and status = 'IN TRANSIT'"
getInvoicesLoadCountByOnlySub = "select count(*) from dispatch_dev.load where sub = %s and status = 'INVOICED'"
getCompletedLoadCountByOnlySub = "select count(*) from dispatch_dev.load where sub = %s and status = 'COMPLETED'"
totalCountWithSub = "select count(*) FROM dispatch_dev.load where sub = %s and status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"
totalPaidPayment = """SELECT SUM(driver_net_settelment) AS sum_value FROM dispatch_dev.payments WHERE sub = %s AND status = 'PAID'"""


# Count with year and sub
getLoadCountBySubOnlySubAndYear = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND status = 'CREATED'"
getAssignedLoadCountByOnlySubAndYear = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND status = 'ASSIGNED'"
getInTransistLoadCountByOnlySubAndYear = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND status = 'IN TRANSIT'"
getInvoicesLoadCountByOnlySubAndYear = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND status = 'INVOICED'"
getCompletedLoadCountByOnlySubAndYear = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND status = 'COMPLETED'"
totalCountWithSubAndYear = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"
totalPaidPaymentWithYear = """SELECT SUM(driver_net_settelment) AS sum_value FROM dispatch_dev.payments WHERE sub = %s AND YEAR(ts_created) = %s AND status = 'PAID'"""


# Count with year sub and date range
getLoadCountBySubOnlySubAndYearAndDate = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'CREATED'"
getAssignedLoadCountByOnlySubAndYearAndDate = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'ASSIGNED'"
getInTransistLoadCountByOnlySubAndYearAndDate = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'IN TRANSIT'"
getInvoicesLoadCountByOnlySubAndYearAndDate = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'INVOICED'"
getCompletedLoadCountByOnlySubAndYearAndDate = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'COMPLETED'"
totalCountWithSubAndYearAndDate = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"
totalPaidPaymentWithYearAndDaterRange = "SELECT SUM(driver_net_settelment) AS sum_value FROM dispatch_dev.payments WHERE sub = %s AND YEAR(ts_created) = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'PAID'"

# Count with sub and date range only
getLoadCountBySubandDateRange = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'CREATED'"
getAssignedLoadCountBySubandDateRange = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'ASSIGNED'"
getInTransistLoadCountBySubandDateRange = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'IN TRANSIT'"
getInvoicesLoadCountBySubandDateRange = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'INVOICED'"
getCompletedLoadCountBySubandDateRange = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'COMPLETED'"
totalCountWithSubAndDaterange = "SELECT COUNT(*) FROM dispatch_dev.load WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"
totalPaidPaymentWithYearAndDate = "SELECT SUM(driver_net_settelment) AS sum_value FROM dispatch_dev.payments WHERE sub = %s AND DATE(ts_created) BETWEEN %s AND %s AND status = 'PAID'"


# WITH DISPATCHER IDS.


# Count with year dispatcherid and sub.
getLoadCountBySubDispatcherIdWithYear = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and status = 'CREATED'"
getAssignedLoadCountByDispatcherIdWithYear = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and status = 'ASSIGNED'"
getInTransistLoadCountDispatcherIdWithYear = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and status = 'IN TRANSIT'"
invoicesLoadCountByDispatcherIdWithYear = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and status = 'INVOICED'"
completedLoadCountByDispatcherIdWithYear = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and status = 'COMPLETED'"
totalCountWithSubAndDispatcherIdWithYear = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"

# Count with year, date and sub.
getLoadCountBySubDispatcherIdWithYearAndDate = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'CREATED'"
getAssignedLoadCountByDispatcherIdWithYearAndDate = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'ASSIGNED'"
getInTransistLoadCountDispatcherIdWithYearAndDate = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'IN TRANSIT'"
invoicesLoadCountByDispatcherIdWithYearAndDate = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'INVOICED'"
completedLoadCountByDispatcherIdWithYearAndDate = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'COMPLETED'"
totalCountWithSubAndDispatcherIdWithYearAndDate = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and YEAR(ts_created) = %s and DATE(ts_created) BETWEEN %s AND %s and status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"


# Count with date and sub only.
getLoadCountBySubDispatcherIdWithDateRange = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'CREATED'"
getAssignedLoadCountByDispatcherIdWithDateRange = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'ASSIGNED'"
getInTransistLoadCountDispatcherIdWithDateRange = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'IN TRANSIT'"
invoicesLoadCountByDispatcherIdWithDateRange = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'INVOICED'"
completedLoadCountByDispatcherIdWithDateRange = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and DATE(ts_created) BETWEEN %s AND %s and status = 'COMPLETED'"
totalCountWithSubAndDispatcherIdWithDateRange = "select count(*) from dispatch_dev.load where dispatcher_fk = %s and sub = %s and DATE(ts_created) BETWEEN %s AND %s and status IN ('CREATED','ASSIGNED','IN TRANSIT','INVOICED','COMPLETED', 'READY FOR REVIEW')"



# getTotalLoadByDate = "select * from dispatch_dev.load where dispatcher_fk = %s and ts_created BETWEEN %s AND %s and sub = %s"
getTotalLoadsCountByDateRange= "select count(*) from dispatch_dev.load where dispatcher_fk = %s and ts_created BETWEEN %s AND %s and sub = %s"
getTotalLoadsCountByDateRangeAndOnlySub= "select count(*) from dispatch_dev.load where ts_created BETWEEN %s AND %s and sub = %s"


# getTotalLoadsCountByMonthandYear = "SELECT MONTHNAME(ts_created) MONTH, COUNT(*) COUNT FROM dispatch_dev.load WHERE dispatcher_fk = %s and sub = %s and status != 'DELETED' and YEAR(ts_created) = %s GROUP BY YEAR(ts_created) , MONTH(ts_created) ORDER BY MONTH(ts_created)"
getTotalLoadsCountByMonthandYear = """WITH months(MONTHS) AS
(
SELECT 'January' AS
MONTH
UNION SELECT 'February' AS
MONTH
UNION SELECT 'March' AS
MONTH
UNION SELECT 'April' AS
MONTH
UNION SELECT 'May' AS
MONTH
UNION SELECT 'June' AS
MONTH
UNION SELECT 'July' AS
MONTH
UNION SELECT 'August' AS
MONTH
UNION SELECT 'September' AS
MONTH
UNION SELECT 'October' AS
MONTH
UNION SELECT 'November' AS
MONTH
UNION SELECT 'December' AS
MONTH
)
Select MONTHS, Null as dispathcer_fk, Null as sub, 0 as COUNT from months 
UNION SELECT MONTHNAME(ts_created) MONTH, dispatcher_fk, sub, COUNT(*) as COUNT FROM dispatch_dev.load WHERE 
dispatcher_fk = %s and sub = %s and status != 'DELETED' and YEAR(ts_created) = %s
GROUP BY YEAR(ts_created), MONTH(ts_created);

"""
getTotalLoadsCountByMonthandYearWithSub = """WITH months(MONTHS) AS
(
SELECT 'January' AS
MONTH
UNION SELECT 'February' AS
MONTH
UNION SELECT 'March' AS
MONTH
UNION SELECT 'April' AS
MONTH
UNION SELECT 'May' AS
MONTH
UNION SELECT 'June' AS
MONTH
UNION SELECT 'July' AS
MONTH
UNION SELECT 'August' AS
MONTH
UNION SELECT 'September' AS
MONTH
UNION SELECT 'October' AS
MONTH
UNION SELECT 'November' AS
MONTH
UNION SELECT 'December' AS
MONTH
)
Select MONTHS, Null as dispathcer_fk, Null as sub, 0 as COUNT from months 
UNION SELECT MONTHNAME(ts_created) MONTH, dispatcher_fk, sub, COUNT(*) as COUNT FROM dispatch_dev.load WHERE sub = %s and status != 'DELETED' and YEAR(ts_created) = %s
GROUP BY YEAR(ts_created), MONTH(ts_created);

"""

insertprofile = "INSERT INTO dispatch_dev.`profiles` (profilename, companyname, ts_created, ts_updated, dot, mc, telephonenumber,faxnumber, address, status, sub, profileurl, userid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
getProfileWithSubId = "select * from dispatch_dev.profiles where sub = %s and status != 'DELETED'"

getProfileByIdandSub = """select * from dispatch_dev.profiles where profileid = %s and sub = %s """

getDispatcherProfileWithSubandId = "select * from dispatch_dev.profiles where  sub = %s and status != 'DELETED'"