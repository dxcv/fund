import sys

sys.path.append("D:\Projects\PythonProject\ZSAssetManagementResearch\FoF Management")
import numpy as np
import pandas as pd
import datetime as dt
import DerivedIndicator_Fund as DI_F
import qcloud.ApiEncapsulation as API

# import DerivedIndicator_Company as dc
d = []
for i in range(7):
    d.append(pd.read_excel("C:/Users/Kael Yu/Desktop/TestData_20160606.xlsx", i))

ss1w = d[0]["swanav"].tolist()
ss1w_bm = d[0]["HS300"].tolist()
ss1w_f = d[0]["tbond"].tolist()
ds1w = d[0]["statistic_date"].tolist()
ds1w_m = DI_F.getLastDateOfEachMonth(ds1w, True)
ss1w_m = [d[0].loc[d[0]["statistic_date"] == date, "swanav"].tolist()[0] for date in ds1w_m]
ss1w_bm_m = [d[0].loc[d[0]["statistic_date"] == date, "HS300"].tolist()[0] for date in ds1w_m]
ss1w_f_m = [d[0].loc[d[0]["statistic_date"] == date, "tbond"].tolist()[0] for date in ds1w_m]

ss6w = d[1]["swanav"].tolist()
ss6w_bm = d[1]["HS300"].tolist()
ss6w_f = d[1]["tbond"].tolist()
ds6w = d[1]["statistic_date"].tolist()
ds6w_m = DI_F.getLastDateOfEachMonth(ds6w, True)
ss6w_m = [d[1].loc[d[1]["statistic_date"] == date, "swanav"].tolist()[0] for date in ds6w_m]
ss6w_bm_m = [d[1].loc[d[1]["statistic_date"] == date, "HS300"].tolist()[0] for date in ds6w_m]
ss6w_f_m = [d[1].loc[d[1]["statistic_date"] == date, "tbond"].tolist()[0] for date in ds6w_m]

sssw = d[3]["swanav"].tolist()
sssw_bm = d[3]["HS300"].tolist()
sssw_f = d[3]["tbond"].tolist()
dssw = d[3]["statistic_date"].tolist()
dssw_m = DI_F.getLastDateOfEachMonth(dssw, True)
sssw_m = [d[3].loc[d[3]["statistic_date"] == date, "swanav"].tolist()[0] for date in dssw_m]
sssw_bm_m = [d[3].loc[d[3]["statistic_date"] == date, "HS300"].tolist()[0] for date in dssw_m]
sssw_f_m = [d[3].loc[d[3]["statistic_date"] == date, "tbond"].tolist()[0] for date in dssw_m]

ss24w = d[5]["swanav"].tolist()
ss24w_bm = d[5]["HS300"].tolist()
ss24w_f = d[5]["tbond"].tolist()
ds24w = d[5]["statistic_date"].tolist()
ds24w_m = DI_F.getLastDateOfEachMonth(ds24w, True)
ss24w_m = [d[5].loc[d[5]["statistic_date"] == date, "swanav"].tolist()[0] for date in ds24w_m]
ss24w_bm_m = [d[5].loc[d[5]["statistic_date"] == date, "HS300"].tolist()[0] for date in ds24w_m]
ss24w_f_m = [d[5].loc[d[5]["statistic_date"] == date, "tbond"].tolist()[0] for date in ds24w_m]

ss6m = d[2]["swanav"].tolist()
ss6m_bm = d[2]["HS300"].tolist()
ss6m_f = d[2]["tbond"].tolist()
ds6m = d[2]["statistic_date"].tolist()

sssm = d[4]["swanav"].tolist()
sssm_bm = d[4]["HS300"].tolist()
sssm_f = d[4]["tbond"].tolist()
dssm = d[4]["statistic_date"].tolist()

ss24m = d[6]["swanav"].tolist()
ss24m_bm = d[6]["HS300"].tolist()
ss24m_f = d[6]["tbond"].tolist()
ds24m = d[6]["statistic_date"].tolist()

freq = [52, 52, 52, 52, 12, 12, 12]

#
swanav = np.array([ss1w, ss6w, sssw, ss24w, ss6m, sssm, ss24m])
swanav_m = np.array([ss1w_m, ss6w_m, sssw_m, ss24w_m, ss6m, sssm, ss24m])

rr = np.array([np.array(x) for x in list(map(DI_F.genReturnRateSeq, swanav))])
rr_m = np.array([np.array(x) for x in list(map(DI_F.genReturnRateSeq, swanav_m))])
# rr_a = list(map(DI.annualizeReturn,rr,[52,52,52,52,12,12,12]))

# Price of Benchmark
p_bm = np.array([ss1w_bm, ss6w_bm, sssw_bm, ss24w_bm, ss6m_bm, sssm_bm, ss24m_bm])
p_bm_m = np.array([ss1w_bm_m, ss6w_bm_m, sssw_bm_m, ss24w_bm_m, ss6m_bm, sssm_bm, ss24m_bm])

rr_bm = np.array([np.array(x) for x in list(map(DI_F.genReturnRateSeq, p_bm))])
rr_bm_m = np.array([np.array(x) for x in list(map(DI_F.genReturnRateSeq, p_bm_m))])
# rr_bm_a = list(map(DI.annualizeReturn,rr_bm,[52,52,52,52,12,12,12]))

# Price of tbond
p_f = np.array([ss1w_f, ss6w_f, sssw_f, ss24w_f, ss6m_f, sssm_f, ss24m_f])
p_f_m = np.array([ss1w_f_m, ss6w_f_m, sssw_f_m, ss24w_f_m, ss6m_f, sssm_f, ss24m_f])
rr_f = np.array([np.array(x) for x in list(map(DI_F.genReturnRateSeq, p_f))])
rr_f_m = np.array([np.array(x) for x in list(map(DI_F.genReturnRateSeq, p_bm_m))])
rr_f_a = list(map(DI_F.annualizeReturn, rr_f, [52, 52, 52, 52, 12, 12, 12]))

ds = np.array([ds1w, ds6w, dssw, ds24w, ds6m, dssm, ds24m])
ds_m = np.array([ds1w_m, ds6w_m, dssw_m, ds24w_m, ds6m, dssm, ds24m])
# interval = [1, 6, "s", 24, 6, "s", 24]
# freq_m = [12, 12, 12, 12, 12, 12, 12]

#
res1_1 = list(map(DI_F.interval_return, swanav))  #
res1_2 = list(map(DI_F.interval_return_a, rr, freq))
res1_3 = list(map(DI_F.interval_excessReturn_a, rr, rr_bm, freq))
# res1_7 = list(map(DI.interval_maxRangeOfIncrease2,
# [[DI.interval_return2(swanav[0])],rr[4], rr[5], rr[6], rr[4], rr[5], rr[6]]))
##
res1_7 = list(map(DI_F.interval_maxRangeOfIncrease, rr_m))  #
# res1_8 =
# list(map(DI.interval_monthOfPositiveReturn2,[[ds[0][0],ds[0][-1]],ds[4],ds[5],ds[6],ds[4],ds[5],ds[6]],[[DI.interval_return2(swanav[0])],rr[4],
# rr[5],
# rr[6],
# rr[4],
# rr[5],
# rr[6]]))
##
res1_8 = list(map(DI_F.interval_monthOfPositiveReturn, ds_m, rr_m))
res1_9 = list(map(DI_F.interval_winRate, rr_m, rr_bm_m, freq))
# res1_10 = list(map(DI.interval_minMonthlyReturnRate2,
# [[swanav[0][0],swanav[0][-1]], swanav[4], swanav[5], swanav[6], swanav[4],
# swanav[5], swanav[6]])) #
res1_10 = list(map(DI_F.interval_minMonthlyReturnRate, swanav_m))  #
# res1_11 = list(map(DI.interval_maxMonthlyReturnRate2,
# [[swanav[0][0],swanav[0][-1]],
# swanav[4],
# swanav[5],
# swanav[6],
# swanav[4],
# swanav[5],
# swanav[6]]))
##
res1_11 = list(map(DI_F.interval_maxMonthlyReturnRate, swanav_m))  #
res2_1 = list(map(DI_F.interval_stdev, rr))
res2_2 = list(map(DI_F.interval_stdev_a, rr, freq))
res2_3 = list(map(DI_F.interval_downsideDeviation_a, rr, rr_f, freq))
res2_4 = list(map(DI_F.interval_maxRetracement, [x[:-1] for x in swanav]))
res2_5 = list(map(DI_F.interval_spanOfMaxtracement, ds, [x[:-1] for x in swanav]))
res2_6 = list(map(DI_F.interval_beta, rr, rr_bm, rr_f))
res2_7 = list(map(DI_F.interval_corr, rr, rr_bm))
res2_8 = list(map(DI_F.interval_nsr, rr, rr_bm, rr_f))
res2_9 = list(map(DI_F.interval_trackError_a, rr, rr_bm, freq))
res2_10 = list(map(DI_F.interval_valueAtRisk, rr))
res2_11 = list(map(DI_F.interval_skewness, rr))
res2_12 = list(map(DI_F.interval_kurtosis, rr))
# res2_13 = list(map(DI.interval_maxRangeOfDecline2,
# [[DI.interval_return2(swanav[0])],rr[4], rr[5], rr[6], rr[4], rr[5], rr[6]]))
res2_13 = list(map(DI_F.interval_maxRangeOfDecline, rr_m))
# res2_14 =
# list(map(DI.interval_monthOfNegativeReturn2,[[ds[0][0],ds[0][-1]],ds[4],ds[5],ds[6],ds[4],ds[5],ds[6]],[[DI.interval_return2(swanav[0])],rr[4],
# rr[5], rr[6], rr[4], rr[5], rr[6]]))
res2_14 = list(map(DI_F.interval_monthOfNegativeReturn, ds_m, rr_m))

res3_1 = list(map(DI_F.interval_sharpe_a, rr, rr_f, freq))
res3_2 = list(map(DI_F.interval_calmar_a, swanav, rr_f, freq))  ######
res3_3 = list(map(DI_F.interval_sortino_a, rr, rr_f, freq))
res3_4 = list(map(DI_F.interval_treynor_a, rr, rr_bm, rr_f, freq))
res3_5 = list(map(DI_F.interval_info_a, rr, rr_bm, freq))
res3_6 = list(map(DI_F.interval_jensen_a, rr, rr_bm, rr_f, freq))
res3_7 = list(map(DI_F.interval_ERVaR, rr, rr_f, freq))

res4_1 = list(map(DI_F.interval_competency_timing, rr, rr_bm, rr_f))
res4_2 = list(map(DI_F.interval_competency_stock, rr, rr_bm, rr_f))
res4_3 = list(map(DI_F.interval_sustainabilityOfExcessReturn, rr, rr_bm))
res4_4 = list(map(DI_F.interval_maxContinuousIncrease, rr_m))  #
res4_5 = list(map(DI_F.interval_maxContinuousDecline, rr_m))  #
res1 = [res1_1, res1_2, res1_3, res1_7, res1_8, res1_9, res1_10, res1_11]
res2 = [res2_1, res2_2, res2_3, res2_4, res2_5, res2_6, res2_7, res2_8, res2_9, res2_10, res2_11, res2_12, res2_13,
        res2_14]
res3 = [res3_1, res3_2, res3_3, res3_4, res3_5, res3_6, res3_7]
res4 = [res4_1, res4_2, res4_3, res4_4, res4_5]
# res = [res1,res2, res3, res4]
res = []
res.extend(res1)
res.extend(res2)
res.extend(res3)
res.extend(res4)


def getValueOrComment(x, i):
    if type(x) == tuple:
        return x[i]
    else:
        if i == 0:
            return x
        else:
            return ""


d = pd.DataFrame(res)
d.columns = ["1w", "6w", "sw", "24w", "6m", "sm", "24m"]

res_w = d[["1w", "6w", "sw", "24w"]].T[0]
for cols in d[["1w", "6w", "sw", "24w"]].T.columns:
    if cols > 0:
        res_w = res_w.append(d[["1w", "6w", "sw", "24w"]].T[cols])

res_m = d[["6m", "sm", "24m"]].T[0]
for cols in d[["6m", "sm", "24m"]].T.columns:
    if cols > 0:
        res_m = res_m.append(d[["6m", "sm", "24m"]].T[cols])

q = pd.DataFrame()
q["indicator"] = res_w.tolist()
q["freq"] = res_w.index
q["comment"] = res_w.tolist()

q["comment"] = q["comment"].apply(lambda x: getValueOrComment(x, 1))
q["indicator"] = q["indicator"].apply(lambda x: getValueOrComment(x, 0))
q.to_csv("C:/Users/Kael Yu/Desktop/3.csv", index=None)

###########
q = pd.DataFrame()
q["indicator"] = res_m.tolist()
q["freq"] = res_m.index
q["comment"] = res_m.tolist()

q["comment"] = q["comment"].apply(lambda x: getValueOrComment(x, 1))
q["indicator"] = q["indicator"].apply(lambda x: getValueOrComment(x, 0))
q.to_csv("C:/Users/Kael Yu/Desktop/4.csv", index=None)


# t["日期"] = t["日期"].apply(lambda x: re.search("(\d+)-(\d+)-(\d+)",x).groups())
# t["日期"] = t["日期"].apply(lambda x: dt.datetime(int(x[0]), int(x[1]),
# int(x[2])))
def st(x):
    if type(x) == str:
        return dt.datetime.strptime(x, "%Y/%m/%d")
    else:
        return x


w["real"] = w["real"].apply(st)
w["std"] = w["std"].apply(st)

from sqlalchemy import *
# engine =
# create_engine("mysql+pymysql://licj_read:AAaa1234@rdsw5ilfm0dpf8lee609.mysql.rds.aliyuncs.com:3306/wealth_db?charset=utf8")
# engine.connect()
# q = pd.read_sql("select * from simu_jingzhi",con=engine)

# engine =
# create_engine("mysql+pymysql://zsadmin:zsadmin456@222.73.205.73:3306/base")
# engine.connect()


import requests

# r = requests.get('https://api.github.com/events')
ss = requests.Session()
ss.headers["Token"] = "ddd3c823-0b22-4d38-8d50-b94c460cfeb6"
ss.headers["Token"] = "cd19fe21-7ffd-4986-94cb-35a78f3f3b9f"
# headers = {"Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2",
# "Accept-Encoding": "gzip, deflate, sdch", "Connection": "keep-alive",
# "Accept":
# "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
# "Upgrade-Insecure-Requests": "1", "User-Agent": "Mozilla/5.0 (Windows NT 6.1)
# AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"}

# url =
# "http://120.55.69.127:8080/data-api/search/external/fund/nav_dividend/swanav_source?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
# payload = {"fund_ids":"JR000001",
# "start_time":"2015-12-17",
# "end_time":"2015-12-19",
# "data_source":3}

# r = s.post(url, get_data=payload,headers=headers)
# print(r.text)
u = "http://120.55.69.127:8080/get_data-api/external/search/fund/base_info"
data1 = {"from": "0", "size": 2000, "param_fields": "fund_id"}
data1 = {"fund_ids": "JR000001"}
r = ss.post(u + "/fund_name", data1)
r.text

u2 = "http://120.55.69.127:8080/get_data-api/external/search/fund/base_info/fund_status?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
d2 = {"fund_ids": "JR000001,JR000002"}

u2 = "http://120.55.69.127:8080/get_data-api/external/search/fund/base_info/fund_status"
d2 = {"token": "bda1850a2a907fa33b0cb1241f5f742bb4138b1c", "fund_ids": "JR000001,JR000002"}
ss.post(u2, d2).json()

ss.headers["Token"] = "bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
ss.headers
u2 = "http://120.55.69.127:8080/get_data-api/external/search/fund/base_info/fund_status"
d2 = {"fund_ids": "JR000001,JR000002"}
ss.post(u2, d2).text

r = ss.post(u2, d2)
r = requests.get(
    "http://120.55.69.127:8080/get_data-api/search/external/fund/base_info/fund_status?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c&fund_ids=JR000001,JR000002")

session = requests.session()
session["Token"] = "bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
api_url = "http://120.55.69.127:8080/get_data-api/internal/search/fund/base_info/NAVInfo_managed_by_invest"
payload = {"fund_ids": "JR000001"}
payload = {"fund_ids": "JR000001",
           "start_time": "2015-12-17",
           "end_time": "2016-3-31",
           "data_source": "3"}
r = session.post(api_url, payload)

r = requests.get(
    "http://120.55.69.127:8080/get_data-api/external/search/fund/nav_dividend/nav_source?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c&fund_ids=JR000001&start_time=2015-12-17&end_time=2015-12-19&data_source=3")

u4 = "http://120.55.69.127:8080/get_data-api/external/search/fund/performance_index/accumulative_return"
d4 = {"fund_ids": "JR000001", "interval_type": "year"}
r = ss.post(u4, d4)
r.text
r = requests.get(
    "http://120.55.69.127:8080/get_data-api/search/external/fund/nav_dividend/added_nav_source?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c&fund_ids=JR000001&start_time=2015-12-17&end_time=2015-12-19&data_source=3")

u5 = "http://120.55.69.127:8080/get_data-api/search/external/fund/nav_dividend/swanav_source?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
u5_2 = "http://120.55.69.127:8080/get_data-api/search/external/fund/nav_dividend/swanav_source?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c&fund_ids=JR000001&start_time=2015-12-17&end_time=2015-12-19&data_source=3"

u5_3 = "http://120.55.69.127:8080/get_data-api/external/search/fund/nav_dividend/nav_source?"
d5_3 = {"fund_ids": "JR000001",
        "end_time": "2016-03-25 00::00:01",
        "data_source": "3"}
r = ss.post(u5_3, d5_3)

u6 = "http://120.55.69.127:8080/get_data-api/external/search/fund/org_info/org_introduction?"
d6 = {"token": "bda1850a2a907fa33b0cb1241f5f742bb4138b1c",
      "org_id": "P1000131"}
r = ss.post(u6, d6)

r = requests.get(
    "http://120.55.69.127:8080/get_data-api/external/search/fund/org_info/org_introduction?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c&org_id=P1000131")

u10 = "http://120.55.69.127:8080/get_data-api/program/get_data/write?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
d10 = {"parms": {"exec_id": "e55e9282-202e-4b6d-ba9a-b3efead9fb22",
                 "exec_type": "TEST"},
       "result": {"db_name": "base",
                  "table_name": "fund_info",
                  "param_fields": "fund_id,fund_name,reg_code,fund_member",
                  "update_fields": "fund_member",
                  "datas": [{"fund_id": "JR000001", "fund_member": "YSY", "fund_name": "Test_1", "reg_code": "SC7525"},
                            {"fund_id": "JR027113", "fund_member": "YSY2", "fund_name": "Test_2", "reg_code": ""}]
                  }
       }

with open("C:/Users/Kael Yu/Desktop/write_test.json", "r") as f:
    data = json.load(f)

with open("C:/Users/Kael Yu/Desktop/write_test1.json", "w") as f:
    json.dump(data, f)

# "parms":{"exec_id":"e55e9282-202e-4b6d-ba9a-b3efead9fb22","exec_type":"TEST"},
# pl10 = {"db_name":"base","table_name":"fund_info","fields":"fund_id,fund_name,reg_code,fund_member","update_fields":"fund_member","datas":[{"fund_id":"Test_000001","fund_member":"ysy","fund_name":"test","reg_code":"SC7525"},{"fund_id":"JR027113","fund_member":"ysy2","fund_name":"Test2","reg_code":""}]}


r = ss.post(u10, d10)

r = ss.get(u5_2)
r = requests.get(
    "http://120.55.69.127:8080/get_data-api/search/external/fund/nav_dividend/swanav_source?token=bda1850a2a907fa33b0cb1241f5f742bb4138b1c&fund_ids=JR000001&start_time=2015-12-17&end_time=2015-12-19&data_source=3")
d5 = {"fund_ids": "JR000001", "start_time": "2015-12-17", "end_time": "2015-12-19", "data_source": "3"}

ss.request("get", "http://www.howbjs.com/min/f=/upload/auto/script/fund/smjjlsjz_P00075_v633.js")

d1.sort_values(["statistic_date"], 0, False, inplace=True)

np.array(d1["nav"].tolist())

# def match_timeSeries_weekly(std, real):
#    res = set(std).intersection(set(real))
#    drop = set(real) - res
#    drop = list(drop); drop.sort(reverse=True)
#    res = list(res); res.sort(reverse=True); res=np.array(res)
#    interval = res[:-1] - res[1:]
#    res = list(res)

#    idx = []
#    for i in range(len(interval)):
#        if interval[i].days != 7:
#            idx.append(i)

#    inserted = 0
#    for i in idx:
#        std_date = res[i+inserted] - dt.timedelta(7)
#        for j in range(len(drop)):
#            if (std_date - drop[j]).days > 0 and (std_date - drop[j]).days <
#            7:
#                res.insert(i+1+inserted, drop[j])
#                inserted += 1
#                break
#            if j == len(drop)-1:
#                res.insert(i+1+inserted, np.nan)
#                inserted +=1

#    return res

# Simple test
from sqlalchemy import create_engine, inspect

engine = create_engine("mysql+pymysql://zsadmin:zsadmin456@120.55.69.127:3306/base?charset=utf8")
engine.connect()

inspector = inspect(engine)
sch_db = inspector.get_schema_names()
tab_db = inspector.get_table_names()
col_db = inspector.get_columns("fund_nv_data")

d = pd.read_sql("select * FROM d_fund_nv_data WHERE fund_id = '060591'", engine)  # , index_col="statistic_date"
d = pd.read_csv("C:\\Users\\Kael Yu\\Desktop\\sqltest.csv", encoding="gbk")
d = pd.read_sql("select * FROM d_fund_nv_data WHERE fund_id = '060591'", engine)  # , index_col="statistic_date"
d = d[["nav", "sanav", "statistic_date"]]
d = d["sanav"]
DI_F.interval_return(q["sanav"].tolist())
t = d["statistic_date"].tolist()

pd.bdate_range()

ss = '{"responce_code":"1000","get_data":[{"nav":"2.531000","source_code":"3","statistic_date":"2016-06-17"},{"nav":"2.031000","source_code":"3","statistic_date":"2016-06-18"}]}'
q = pd.DataFrame(json.loads(ss)["get_data"])
q.set_index("statistic_date", inplace=True)

sd = dt.datetime(2016, 7, 14)
ed = dt.datetime(2015, 11, 14)


def match_timeseries_weekly(series_real, start_date, end_date):
    series_real = list(series_real)
    series_real.sort(reverse=True)

    # generate standard time series from real time series
    series_std = DI_F.genStdDateSeq(((sd.year * 12 + sd.month) - (ed.year * 12 + ed.month)) + 1, 52, start_date)
    series_std = [x for x in series_std if x >= series_real[-1]]

    res = set(series_std).intersection(set(series_real))
    drop = set(series_real) - res
    drop = list(drop)
    drop.sort(reverse=True)
    missing = set(series_std) - res
    missing = list(missing)
    missing.sort(reverse=True)

    res = list(res)
    res.sort(reverse=True)

    index = [series_std.index(dt) for dt in missing]

    for i in range(len(index)):
        for j in range(len(drop)):
            delta = (series_std[index[i]] - drop[j]).days
            if delta < 7 and delta >= 0:
                res.insert(index[i], drop[j])
                break
            if j == len(drop) - 1:
                res.insert(index[i], None)
    return list(zip(res, series_std))
    # return drop, missing, set(real_series).intersection(set(std_series))
    # return res
    # return std_series


t = list(numpy.array(t).T[0])


def match_timeseries_monthly(series_real, start_date, end_date):
    series_real = list(series_real)
    series_real.sort(reverse=True)

    # generate standard time series from real time series
    series_std = DI_F.genStdDateSeq(((sd.year * 12 + sd.month) - (ed.year * 12 + ed.month)) + 1, 12, start_date)
    series_std = [x for x in series_std if x >= series_real[-1]]

    # series_std =

    std_series = list()
    return series_std


def reshape_data(time_series, data, col_date, col_trans):
    """
    根据匹配的时间序列, 给定列名称, 将Dataframe格式的原数据塑形成dict格式的数据.

    *Args:
        time_series(list<datetime>): 匹配过后的时间序列(日期由近及远);
        get_data(pandas.Dataframe): 带有日期序列的原数据;
        col_date(str): 原数据中表示时间序列的列名;
        col_trans(list<string>): 需要保留的列名;

    *Returns:
        匹配, 重塑后的dict格式数据.
    """
    res = {}
    data = data.set_index(col_date).to_dict(orient="index")
    for k in col_trans:
        res_k = []
        for t in time_series:
            try:
                res_k.append(data[t][k])
            except:
                res_k.append(None)
        res[k] = res_k
    return res


import json

with open("C:\\Users\\Kael Yu\\Desktop\\395-info.json", "r", encoding="utf-8") as f:
    data = json.load(f)

res = []
for i in range(len(q) - 1):
    try:
        res.append(q[i] / q[i + 1] - 1)
    except:
        res.append(None)


def gs(pwd):
    password = {}
    for x, y in zip("abcdefghijklmnopqrstuvwxyz", range(26)):
        password[x] = y
    sum = 0
    for x in pwd:
        sum += password[x]

    return sum


import hashlib as h


def find_one(series):
    res = []
    sub = []
    for i in range(len(series)):
        if series[i] == 1:
            sub.append(i)
        else:
            if len(sub) > 0:
                res.append(sub)
            sub = []
    return res


sql = "\
    SELECT T1.fund_id FROM \
    (SELECT	DISTINCT fund_id, MIN(statistic_date) AS date_min FROM fund_nv_data GROUP BY fund_id HAVING	date_min < '2010-12-01') as T1\
    INNER JOIN (SELECT DISTINCT fund_id FROM fund_nv_data WHERE statistic_date >= '2010-12-01' AND statistic_date < '2011-01-08' AND nav IS NOT NULL) as T2 ON T1.fund_id = T2.fund_id\
    INNER JOIN (SELECT DISTINCT fund_id FROM fund_nv_data WHERE statistic_date >= '2011-01-01' AND statistic_date < '2011-02-08' AND nav IS NOT NULL) as T3 ON T1.fund_id = T3.fund_id\
    ORDER BY T1.fund_id"


def mirror(x):
    return x[::-1] + x


mirror("康王没脑子, 但经常脑子疼")


def ins(x):
    temp = [x + y for x, y in zip(x, x[::-1])]
    res = ""
    for x in temp:
        res += x
    return res


import pandas as pd
from utils.database import config as cfg

engine_rd = cfg.load_engine()["2Gb"]

tcs = list(range(60101, 60110))
sp_nums = [int(x) for x in [119.0, 11.0, 1.0, 11.0, 18.0, 1.0, 4.0, 9.0, 26.0]]
args = zip(tcs, sp_nums)

def get_random_id(type_code, num_sp):
    sql = "SELECT SQL_NO_CACHE DISTINCT ftm.fund_id \
    FROM fund_type_mapping ftm \
    JOIN fund_nv_data_standard fnds ON fnds.fund_id = ftm.fund_id \
    WHERE ftm.type_code = {tc} AND flag = 1 \
    AND fnds.statistic_date >= '20170301' \
    ORDER BY ftm.fund_id;".format(tc=type_code)

    df = pd.read_sql(sql, engine_rd)

    num_tt = len(df)
    step = num_tt / num_sp
    indexes = [int(N * step) for N in range(int(num_sp))]
    result = df.ix[indexes]["fund_id"].tolist()
    return result

result = []
for tc, sp_num in args:
    ids = get_random_id(tc, sp_num)
    result.append([tc, sp_num, str(ids)])

result = pd.DataFrame(result)
result.columns = ["type_code", "sample_num", "ids_used"]


from utils.database import config as cfg
import pandas as pd
import datetime as dt
engine_rd = cfg.load_engine()["2Gb"]
result = pd.DataFrame()
dates = [dt.date(2017, 1, 1) + dt.timedelta(x*7) for x in range(12)]
for sd in dates:
    print(sd, ed)
    ed = sd + dt.timedelta(6)
    sql = "SELECT COUNT(DISTINCT fund_id) as 'funds_num', COUNT(fund_id) as 'records_num', {sd} as 'date' \
    FROM fund_nv_data_standard \
    WHERE statistic_date BETWEEN '{sd}' AND '{ed}'".format(sd=sd.strftime("%Y%m%d"), ed=ed.strftime("%Y%m%d"))
    tmp = pd.read_sql(sql, engine_rd)
    print(tmp)
    result = result.append(tmp)

result.to_excel("C:/Users/Yu/Desktop/nums.xlsx", index=False)