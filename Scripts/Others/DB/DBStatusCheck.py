import datetime as dt
import pandas as pd
from sqlalchemy import create_engine

now = dt.datetime.now().timestamp()
now = dt.date.fromtimestamp(now)

file_path = "C:\\Users\\Kael Yu\\Desktop\\DBM"

acc = pd.read_excel(file_path+"\\Accounts_inner.xlsx", sheetname = "120.55.69.127")
acc.fillna("", inplace=True)
acc_mapping = {acc: (acc_name+department, salesman) for acc, acc_name, department, salesman in zip(acc["Account"], acc["Company"], acc["Department"], acc["Salesman"])}
acc_mapping["jr_test_citicsf"] = acc_mapping["jr_test_citicf"]  #

engine = create_engine("mysql+pymysql://jr_admin_0:4ba28408335e6f63eb62cc95ab5123fe6f136720@120.55.69.127/base", connect_args={"charset": "utf8"})
conn = engine.connect()

pl = pd.read_sql("SHOW PROCESSLIST", conn)
res = conn.execute("SHOW PROCESSLIST").fetchall()

sql = {1: "EXPLAIN EXTENDED \
SELECT SQL_NO_CACHE COUNT(1) \
FROM (SELECT fund_id FROM fund_type_mapping WHERE type_code = 60101) as ftm \
JOIN fund_weekly_return as fwr FORCE INDEX(statistic_date) \
ON ftm.fund_id = fwr.fund_id \
WHERE benchmark = 1 AND statistic_date >= '2016-01-01';"}

q = pd.read_sql(sql[1], conn)

"SELECT SQL_NO_CACHE COUNT(fwr.fund_id) \
FROM (SELECT fund_id FROM fund_type_mapping WHERE type_code = 60102 ) AS ftm \
JOIN fund_weekly_return AS fwr FORCE INDEX (statistic_date) ON ftm.fund_id = fwr.fund_id \
WHERE benchmark = 1 AND statistic_date >= '2016-01-01'";






