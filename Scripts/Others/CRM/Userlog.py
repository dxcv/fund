import datetime as dt
import pandas as pd
import re
from sqlalchemy import create_engine

now = dt.datetime.now().timestamp()
now = dt.date.fromtimestamp(now)

ip_list = {"116.226.213.51", "116.226.249.35", "116.226.223.31", "116.226.210.9", "180.173.147.127", "118.242.32.38",
           "116.231.79.100"}
file_path = "C:\\Users\\Kael Yu\\Desktop\\CRM"

acc = pd.read_excel(file_path + "\\Accounts.xlsx", sheetname="121.40.18.150")
acc.fillna("", inplace=True)
acc_mapping = {acc: (acc_name + department, salesman) for acc, acc_name, department, salesman in
               zip(acc["Account"], acc["Company"], acc["Department"], acc["Salesman"])}
acc_mapping["jr_test_citicsf"] = acc_mapping["jr_test_citicf"]  #

engine = create_engine("mysql+pymysql://jr_admin_0:4ba28408335e6f63eb62cc95ab5123fe6f136720@121.40.18.150/access_log",
                       connect_args={"charset": "utf8"})
conn = engine.connect()

log = pd.read_sql("SELECT matchname, localname, log_time FROM access_log WHERE matchname NOT LIKE 'jr_admin%%'", conn)
patt_acc = "\D*\d*@"
patt_ip = "@\d*\.\d*\.\d*\.\d*"
log["matchname"] = log["matchname"].apply(lambda x: re.search(patt_acc, x).group(0)[:-1])
log["localname"] = log["localname"].apply(lambda x: re.search(patt_ip, x).group(0)[1:])
log = log.loc[log["localname"].apply(lambda x: x not in ip_list)]

log = log.loc[log.matchname.apply(lambda x: x in acc_mapping.keys())]

log["Acc_cn"] = log["matchname"].apply(lambda x: acc_mapping[x][0])
log["Salesman"] = log["matchname"].apply(lambda x: acc_mapping[x][1])

log.sort_values(by=["Salesman", "matchname", "log_time"], ascending=[False, True, False], inplace=True)
log.to_csv(file_path + "\\user_log_{0}.csv".format(now), index=False)
