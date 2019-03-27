import sys
import datetime as dt
import pandas as pd
import re
from sqlalchemy import create_engine

now = dt.datetime.today()
now_str = ""
for item in now.timetuple()[:6]:
    now_str += "0" * (2 - len(str(item))) + str(item)

file_path = "C:/Users/Kael Yu/Desktop/CRM"
file_accounts = "Accounts.xlsx"
accounts = pd.read_excel(file_path + "/" + file_accounts, sheetname="121.40.18.150")

user_acc = accounts["Account"]
user_name = accounts["Company"]
user_dep = accounts["Department"].fillna("")
user_salesman = accounts["Salesman"]
users = {acc: (name + dep, salesman) for acc, name, dep, salesman in zip(user_acc, user_name, user_dep, user_salesman)}
users["jr_test_citicsf"] = users["jr_test_citicf"]

ip_jr = "116.226.249.35"
ip_me = "116.237.48.207"
engine = create_engine(
    "mysql+pymysql://jr_admin_0:4ba28408335e6f63eb62cc95ab5123fe6f136720@121.40.18.150:3306/access_log",
    connect_args={"charset": "utf8"})
conn = engine.connect()

patt_ip = "\d*\.\d*\.\d*\.\d*"
patt_acc = "\D*\d*@"
sql_log = "SELECT localname, log_time FROM access_log WHERE localname NOT LIKE '%%{0}' AND localname NOT LIKE '%%{1}' AND localname LIKE 'jr_test_%%' ORDER BY log_time DESC, localname ASC".format(
    ip_jr, ip_me)
log = pd.read_sql(sql_log, conn)
log["acc"] = log["localname"].apply(lambda x: re.search(patt_acc, x).group()[:-1])

log["Acc_cn"] = log["acc"].apply(lambda x: users[x][0])
log["Salesman"] = log["acc"].apply(lambda x: users[x][1])
log = log.sort_values(by=["Salesman", "acc", "log_time"], ascending=[False, True, False])
log.index = range(len(log))
log.to_csv("{0}/user_log_{1}.csv".format(file_path, now_str))
