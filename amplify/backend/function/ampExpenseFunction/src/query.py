
# Expense
insertexpense = "INSERT INTO dispatch_dev.`expense` (driverFk, expensetype, ts_created, ts_updated, cost, status, sub, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
getExpenseById = "SELECT expense.* , driver.first_name, driver.last_name FROM dispatch_dev.expense LEFT JOIN dispatch_dev.driver ON expense.driverFk = driver.driver_id WHERE expense.expenseId = %s"


getExpenseBySub = """select expense.*, driver.first_name, driver.last_name from dispatch_dev.expense LEFT JOIN dispatch_dev.driver ON expense.driverFk = driver.driver_id where expense.sub = %s and expense.status != 'DELETED' ORDER BY expense.ts_created DESC limit %s offset %s """
getExpenseCountBySub = "select count(*) as count from dispatch_dev.expense where sub = %s and status != 'DELETED'"
getexpensebyidquery = "SELECT expense.* , driver.first_name, driver.last_name FROM dispatch_dev.expense LEFT JOIN dispatch_dev.driver ON expense.driverFk = driver.driver_id WHERE expense.expenseId = %s and expense.sub = %s and expense.status != 'DELETED'"

getSearchedExpenseBySub = """select expense.*, driver.first_name, driver.last_name from dispatch_dev.expense LEFT JOIN
dispatch_dev.driver ON expense.driverFk = driver.driver_id where expense.sub = %s and
ts_created between %s and %s and expense.status != 'DELETED' order by expenseId limit %s offset %s """
