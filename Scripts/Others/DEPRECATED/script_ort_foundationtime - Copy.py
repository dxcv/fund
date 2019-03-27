from importlib import reload
import sys
sys.path.append("D:/Projects/PythonProject/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("/usr/local/python/3.5.2/user-defined-module")
import datetime as dt
from dateutil.relativedelta import relativedelta
import json
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect
import TimeModule
import DerivedIndicator_Fund_2 as F
import DerivedIndicator_Company as C

date_s = dt.datetime(2016,7,10)
date_e = date_s + dt.timedelta(1)
date_s_str = date_s.strftime("%Y-%m-%d %H:%M:%S")
#date_e_str = date_e.strftime("'%Y-%m-%d %H:%M:%S'")
now = dt.datetime.now()
now_str = now.strftime("'%Y-%m-%d %H:%M:%S'")

url_status = "http://120.55.69.127:8080/get_data-api/program/forward/status"
url_progress = "http://120.55.69.127:8080/get_data-api/program/forward/progress"
url_result = "http://120.55.69.127:8080/get_data-api/program/get_data/write"

token = "bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
params = sys.argv[1]
#params = "\{exec_type:PROGRAM_TASK,exec_uuid:567e482c-364f-41c4-9e88-2f7364e7bcec\}"  #exec_type: TEST
pl = {"token":str(token), "parms":str(params)}

s = requests.session()
#s.headers["Token"] = token
engine = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@127.0.0.1:3306/base?charset=utf8")
#engine = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
engine.connect()

def id_generator(ids, start=0, end=None):
    id_tupled = []
    end = min(end, len(ids)) if end is not None else len(ids)
    id_tupled = str(tuple(ids[start:end]))
    return id_tupled

sql1 = "\
    SELECT T2.org_id, T1.fund_id, MIN(T1.foundation_date) as foundation_date, T1.fund_type_strategy FROM fund_info as T1, \
    (SELECT DISTINCT org_id, fund_id \
    FROM fund_org_mapping \
    WHERE org_type_code = 1) as T2 \
    WHERE T1.fund_id = T2.fund_id \
    GROUP BY org_id, fund_type_strategy \
    ORDER BY org_id ASC, fund_id ASC"

data = pd.read_sql(sql1, engine).dropna()

data_strategys = data.groupby(["org_id"])["foundation_date"].min().apply(pd.Timestamp)
data_strategys = data_strategys.apply(lambda x: str((now-x).days))
ids_o = data_strategys.index

update_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
datas = []
for i in range(len(ids_o)):
    res_tmp = {"org_id":ids_o[i], "benchmark":"1", "index_method":"1",
               "index_range":"1", "type_code":"60001", "type_name":"全产品",
               "stype_code":"6000101", "stype_name":"全产品",
               "statistic_date":date_s_str, "update_time":update_time, "org_duration":data_strategys[ids_o[i]]
               }
    datas.append(res_tmp)

fields = ""
for field in datas[0].keys():
    fields += ",%s"%(field)
fields = fields[1:]

pl_result = pl.copy()
pl_result["result"] = str({"db_name":"base",
                           "table_name":"org_month_routine",
                           "param_fields":fields,
                           "update_fields":fields,
                           "datas": datas
                           })

r = s.post(url_result, pl_result)
print("result_api:\n%s" % r.text)

pl_status = pl.copy()
pl_status["status"] = "EXEC_SUCCESS"
r = s.post(url_status, pl_status)
print("status_api:\n%s" % r.text)



