from importlib import reload
import sys
sys.path.append("D:/Projects/PythonProject/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("D:/Projects/Python/3.5.2/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("/usr/local/python/3.5.2/user-defined-module")
import calendar as cld
import datetime as dt
import json
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect
import time
import TimeModule as T
import DerivedIndicator_Fund_2 as F

def tic(string):
    print(dt.datetime.now(), "------", string)

def merge_result(r1, r2, r3):
    result = r1[:]
    for i in range(len(result[1])):
        if result[1][i] is None:
            if r2[1][i] is not None:
                result[1][i] = r2[1][i]
                result[0][i] = r2[0][i]
            else:
                if r3[1][i] is not None:
                    result[1][i] = r3[1][i]
                    result[0][i] = r3[0][i]
                else:
                    continue
    return result

result_r = {}
components_num = {}

first_year = 2012
now = dt.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")

for year in range(first_year, now.timetuple().tm_year+1):

    if year == now.timetuple().tm_year:
        month = 9
    else:
        month = 12

    date_s = dt.datetime(year, month, 10)
    date_e = date_s + dt.timedelta(1)
    date_e_str = date_e.strftime("%Y-%m-%d")

    sql_i = "\
    SELECT fund_id, nav, statistic_date FROM fund_nv_data WHERE statistic_date < '{0}' and statistic_date >= '{1}' \
    AND fund_id IN (SELECT fund_id FROM fund_type_mapping WHERE type_code = 60104) \
    ORDER BY fund_id ASC, statistic_date DESC \
    ".format(dt.date(year+1, 1, 8), dt.date(year-1, 12, 1))

    #conn = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
    conn = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@127.0.0.1:3306/base?charset=utf8")
    conn.connect()

    tic("Getting Data")
    d = pd.read_sql(sql_i, conn).dropna()
    d.index = range(len(d))

    tic("Preprocessing...")
    d["statistic_date"] = d["statistic_date"].apply(lambda x: time.mktime(x.timetuple()))
    d_dd = d.drop_duplicates("fund_id")
    idx_slice = d_dd.index.tolist()
    idx_slice.append(len(d))
    ids = d_dd["fund_id"].tolist()

    t_std = T.timeseries_std(dt.datetime(year, month, 10), month, 12, 1, use_lastday=True)
    t_std1 = t_std[:-1]

    tic("Grouping...")
    ds = [d[idx_slice[i]:idx_slice[i+1]] for i in range(len(idx_slice)-1)]
    ts = [x["statistic_date"].tolist() for x in ds]
    navs = [x["nav"].tolist() for x in ds]

    tic("Matching...")
    matchs1 = [T.outer_match4index_f7(x, t_std1, False) for x in ts]
    #matchs2 = [T.outer_match4index_b7(x, t_std1, False) for x in ts]
    matchs2 = [T.outer_match4index_b7_2(x, t_std1) for x in ts]
    matchs3 = [T.outer_match4index_m(x, t_std, False) for x in ts]
    matchs = [merge_result(x1, x2, x3) for x1, x2, x3 in zip(matchs1, matchs2, matchs3)]

    tic("Getting Result...")
    t_matchs = [x[0] for x in matchs]
    t_matchs = [T.tr(x) for x in t_matchs]
    idx_matchs = [x[1] for x in matchs]
    nav_matchs = [[navs[i][idx] if idx is not None else None for idx in idx_matchs[i].values()] for i in range(len(idx_matchs))]


    tic("Calculating Index...")
    nvs = pd.DataFrame(nav_matchs).T.astype(float).as_matrix()
    rs = nvs[:-1] / nvs[1:] - 1
    rs[rs>30] = np.nan
    rs[rs<-1] = np.nan
    r = np.nanmean(rs, axis=1)
    r[np.isnan(r)] = 0

    result_r[year] = r
    components_num[year] = np.sum(~np.isnan(rs), axis=1)
    tic("Year:{0}, Done...".format(year))

values_r = []
values_num = []
for year in range(first_year, now.timetuple().tm_year+1):
    if len(values_r) == 0:
        values_r = result_r[year].tolist()[::-1]
        values_num = components_num[year].tolist()[::-1]
    else:
        values_r.extend(result_r[year].tolist()[::-1])
        values_num.extend(components_num[year].tolist()[::-1])

result = (np.array(values_r) + 1).cumprod() * 1000
result = result.tolist()
result.insert(0, 1000)
values_num.insert(0, 0)

tag = T.timeseries_std(dt.datetime(year, month+1, 10), T.periods_in_interval(dt.datetime(year, month+1, 10), dt.datetime(first_year, 1, 10), 12), 12)[::-1]
tag = [dt.date.fromtimestamp(x) for x in tag]

#op = pd.DataFrame(list(zip(tag, result, values_num)))
#op.columns = ["date", "index_m_yu", "components_num"]
#op.to_csv("C:\\Users\\Kael Yu\\Desktop\\fund_index_10_m.csv", index=False)


#Send Result

url_status = "http://120.55.69.127:8080/get_data-api/program/forward/status"
url_progress = "http://120.55.69.127:8080/get_data-api/program/forward/progress"
url_result = "http://120.55.69.127:8080/get_data-api/program/get_data/write"
s = requests.session()

token = "bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
params = sys.argv[1]
pl = {"token":str(token), "parms":str(params)}

datas = []
for i in range(len(tag)):
    res_tmp = {"index_id": "FI10", "index_name":"事件驱动策略私募指数", "statistic_date":str(tag[i]), "update_time":now_str,
               "typestandard_code": "601", "typestandard_name": "按投资策略分类", "type_code": "60104", "type_name": "事件驱动",
               "index_method": "1", "data_source":"0", "data_source_name":"私募云通",
               "index_value": str(result[i]), "funds_number": str(values_num[i])
               }
    datas.append(res_tmp)

fields = ""
for field in datas[0].keys():
    fields += ",%s"%(field)
fields = fields[1:]

pl_result = pl.copy()
pl_result["result"] = str({"db_name":"base",
                            "table_name":"fund_month_index",
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

