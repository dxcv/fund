from importlib import reload
import sys
sys.path.append("D:/Projects/PythonProject/ZSAssetManagementResearch/Scripts/algorithm")
import datetime as dt
import json
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect
import TimeModule
import DerivedIndicator_Fund_2 as D
from dateutil.relativedelta import relativedelta

date_s = dt.datetime(2016,7,1)
date_e = date_s + dt.timedelta(1)
date_l = date_s-relativedelta(months=1)
date_s_str = date_s.strftime("%Y-%m-%d %H:%M:%S")
date_e_str = date_e.strftime("%Y-%m-%d %H:%M:%S")
date_l_str = date_l.strftime("%Y-%m-%d %H:%M:%S")
now = dt.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")

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
engine = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
engine.connect()

sql = "\
SELECT T1.fund_id, T1.nav, T1.statistic_date \
FROM (SELECT fund_id, nav, statistic_date \
      FROM fund_nv_data \
      WHERE statistic_date < '%s' \
        AND statistic_date >= '%s' \
	  GROUP BY fund_id) as T1 \
JOIN (SELECT fund_id, foundation_date \
      FROM fund_info WHERE foundation_date < '%s') as T2 ON T1.fund_id = T2.fund_id" % (date_e_str, date_s_str, date_l_str)

ids = engine.execute(sql).fetchall()    #funds which have updated get_data
ids = list(map(lambda x: x[0], ids))

def id_generator(ids, start, end):
    id_tupled = []
    end = min(end, len(ids)) if end is not None else len(ids)
    id_tupled = str(tuple(ids[start:end]))
    return id_tupled

variable_name = ["con_rise_months", "con_fall_months", "s_time", "s_security", ""]

interval_mapping = {"w":"week_", "m":"month_", "s":"quarter_", "y":"year_",
                    1:"m1_", 3:"m3_", 6:"m6_", 12:"y1_",
                    24:"y2_", 36:"y3_", 60:"y5_"}
indicator_mapping = {"stdev":"stdev", "stdev_a":"stdev_a", "dstd_a":"dd_a",
                     "mdd":"mdd", "beta":"beta", "corr":"benchmark_r", "var":"rvalue"}

end = dict(zip(interval, list(map(TimeModule.date_infimum, interval, [date_s] * len(interval)))))

#variable_name2 = ["odds"]

dlength = {"m":1, "s":date_s.month-6, "y":date_s.month, 
                   1:1, 3:3, 6:6, 12:12, 24:24, 36:36, 60:60}
bm_mapping = {1:"HS300", 2:"CSI500", 3:"CSI50"}
keys = ['index_method', 'type_code', 'benchmark','index_range','type_name','stype_name','update_time','stype_code','statistic_date','org_id']; 
fields = ""
for x in interval:
    for y in variable_name_1:
        keys.append(interval_mapping[x]+indicator_mapping[y])

for i in range(len(keys)):
    if i != 0 and i != len(keys) - 1:
        fields += ",%s" % keys[i]
    elif i == 0:
        fields = keys[i]
    elif i == len(keys) - 1:
        fields += ",%s" % keys[i]

bm_mapping = {1:"HS300", 2:"CSI500", 3:"CSI50"}
#
bm = pd.read_sql("SELECT %s, statistic_date FROM market_index WHERE statistic_date>= '"%(bm_mapping[1]) + data.end[60].strftime("%Y-%m-%d") + "' AND statistic_date <= '" + data.start.strftime("%Y-%m-%d") + "'", engine).sort_values("statistic_date", ascending =False)
bm.index = range(len(bm))
r_bm = []
for k in interval:
    t_match = TimeModule.match_timeseries_monthly(bm["statistic_date"].tolist(), k, date_s, data.end[k])
    bm_reshape = TimeModule.reshape_data(t_match, bm, "statistic_date", ["HS300"])["HS300"]
    r_bm.append(D.value_series(bm_reshape))

tbond = pd.read_sql("SELECT 1y_treasury_rate, statistic_date FROM market_index WHERE statistic_date>= '" + (data.end[60] - dt.timedelta(180)).strftime("%Y-%m-%d") + "' AND statistic_date <= '" + data.start.strftime("%Y-%m-%d") + "'", engine).sort_values("statistic_date", ascending =False)
tbond.index = range(len(tbond))
tbond["1y_treasury_rate"].fillna(method="backfill",inplace=True)
tbond = tbond.loc[tbond["statistic_date"] >= data.end[60]]
r_f = []
def test(x):
    if x is not None:
        return (1 + x / 100) ** (1 / 12) - 1
    else:
        return None
for k in interval:
    t_match = TimeModule.match_timeseries_monthly(tbond["statistic_date"].tolist(), k, date_s, data.end[k])
    tbond_reshape = TimeModule.reshape_data(t_match, tbond, "statistic_date", ["1y_treasury_rate"])["1y_treasury_rate"]
    r_f.append(list(map(test, tbond_reshape))[:-1])
for seq in r_f: #unstable
    for i in range(len(seq) - 1):
        if seq[i] is None:
            seq[i] = seq[i + 1]

fund_ids = data.get_data["fund_id"].drop_duplicates().tolist()
datas = [] #Container for each {}

rng_3 = [x for x in range(len(r_bm)) if x != 0 and x != 1 and x != 3]

def nan2none(x):
    if x is not None:
        if np.isnan(x): return None
        else: return x
    else:
        return x

for i in range(len(ids)):
    if i % int(len(fund_ids) / 20) == 0:
        print("Sending Progress%s: %s" % (str(int(i / int(len(fund_ids) / 20) * 5)), str(dt.datetime.now())))
        #pl_progress = pl.copy()
        #pl_progress["progress"] = str(int(i / int(len(fund_ids) / 20) * 5))
        #r = s.post(url_progress, pl_progress)
        #print("Done: %s" % (str(dt.datetime.now())))
        #print(r.text + "\n")
    data_i = data.get_data.loc[data.get_data["fund_id"] == fund_ids[i]]  #get get_data
    t_real = data_i["statistic_date"].tolist()
    
    nav = []
    r = []
    for k in interval:
        t_match = TimeModule.match_timeseries_monthly(t_real, k, date_s, data.end[k]) 
        nav.append(list(map(nan2none, TimeModule.reshape_data(t_match, data_i, "statistic_date", ["nav"])["nav"])))

    r = list(map(D.value_series, nav))
    i_r = map(lambda x: str(x), list(map(D.interval_return, nav)))
    r_a = map(lambda x: str(x), list(map(D.return_a, r, [12]*10)))
    er_a = map(lambda x: str(x), list(map(D.excess_return_a, r, r_bm, [12]*10)))
    odds = map(lambda x: str(x), list(map(D.odds, r, r_bm, [12]*10))[2:])
    sharpe_a = map(lambda x: str(x), np.array((list(map(D.sharpe_a, r, r_f, [12]*10))))[rng_3].tolist())
    calmar_a = map(lambda x: str(x), np.array((list(map(D.calmar_a, nav, r_f, [12]*10))))[rng_3].tolist())
    sortino_a = map(lambda x: str(x), np.array(list(map(D.sortino_a, r, r_f, [12]*10)))[rng_3].tolist())
    info_a = map(lambda x: str(x), np.array(list(map(D.info_a, r, r_bm, [12]*10)))[rng_3].tolist())
    jensen_a = map(lambda x: str(x), np.array(list(map(D.jensen_a, r, r_bm, r_f, [12]*10)))[rng_3].tolist())
    treynor_a = map(lambda x: str(x), np.array(list(map(D.treynor_a, r, r_bm, r_f, [12]*10)))[rng_3].tolist())
    
    res1 = [i_r, r_a, er_a]
    res2 = [odds]
    res3 = [sharpe_a, calmar_a, sortino_a, info_a, jensen_a, treynor_a]
    res_tmp = {"fund_id":fund_ids[i], "benchmark":"1", "statistic_date":date_s.strftime("%Y-%m-%d %H:%M:%S"), "update_time":dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    #res_tmp = {"fund_id":fund_ids[i], "statistic_date":date_s_str, "update_time":now_str}
    
    for seq, var in zip(res1, variable_name1):
        for res, int_ in zip(seq, interval):
            res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res

    for seq, var in zip(res2, variable_name2):
        for res, int_ in zip(seq, interval_2):
            res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res

    for seq, var in zip(res3, variable_name3):
        for res, int_ in zip(seq, interval_3):
            res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res    
    datas.append(res_tmp)

#for key in interval():
#    datas.append({"fund_id":key, "total_return":"%.7f" % res_total[key],
#    "statistic_date":"2016-08-04 16:17:00"})

pl_result = pl.copy()
fields = ""
keys = list(datas[0].keys())
for i in range(len(keys)):
    if i != 0 and i != len(keys) - 1:
        fields += ",%s" % keys[i]
    elif i == 0:
        fields = keys[i]
    elif i == len(keys) - 1:
        fields += ",%s" % keys[i]


pl_result["result"] = str({"db_name":"base",
                           "table_name":"fund_month_return",
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



