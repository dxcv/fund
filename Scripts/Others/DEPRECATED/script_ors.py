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
import DerivedIndicator_Fund_2 as D
import DerivedIndicator_Company as C

N1, N2 = 10, 20
date_s = dt.datetime(2016,7,10)
date_e = date_s + dt.timedelta(1)
date_s_str = date_s.strftime("'%Y-%m-%d %H:%M:%S'")
date_e_str = date_e.strftime("'%Y-%m-%d %H:%M:%S'")
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
SELECT DISTINCT org_id \
FROM fund_org_mapping"

ids = engine.execute(sql1).fetchall()    #funds which have updated get_data
ids = list(map(lambda x: x[0], ids))



interval = ["y",3,6,12,24,36,60]
#variable_name1 = ["ir", "r_a", "er_a"]
#variable_name2 = ["odds"]
variable_name = ["c_timing", "c_stock", "odds", "persistence"]

interval_mapping = {"w":"week_", "m":"month_", "s":"quarter_", "y":"year_",
                    1:"m1_", 3:"m3_", 6:"m6_", 12:"y1_",
                    24:"y2_", 36:"y3_", 60:"y5_"}
indicator_mapping = {"c_timing":"s_time", "c_stock":"s_security", "odds":"odds",
                     "persistence":"persistance"}

end = dict(zip(interval, list(map(TimeModule.date_infimum, interval, [date_s] * len(interval)))))

#variable_name2 = ["odds"]

dlength = {"m":1, "s":date_s.month-6, "y":date_s.month, 
                   1:1, 3:3, 6:6, 12:12, 24:24, 36:36, 60:60}
bm_mapping = {1:"HS300", 2:"CSI500", 3:"CSI50"}
keys = ['index_method', 'type_code', 'benchmark','index_range','type_name','stype_name','update_time','stype_code','statistic_date','org_id']; 

fields = ""
for x in interval:
    for y in variable_name:
        keys.append(interval_mapping[x]+indicator_mapping[y])

for i in range(len(keys)):
    if i != 0 and i != len(keys) - 1:
        fields += ",%s" % keys[i]
    elif i == 0:
        fields = keys[i]
    elif i == len(keys) - 1:
        fields += ",%s" % keys[i]
#
bm = pd.read_sql("SELECT %s, statistic_date FROM market_index WHERE statistic_date>= '"%(bm_mapping[1]) + end[60].strftime("%Y-%m-%d") + "' AND statistic_date <= '" + date_s.strftime("%Y-%m-%d") + "'", engine).sort_values("statistic_date", ascending =False)
bm.index = range(len(bm))

r_bm = []
for k in interval:
    t_match = TimeModule.match_timeseries_monthly(bm["statistic_date"].tolist(), k, date_s, end[k])
    bm_reshape = TimeModule.reshape_data(t_match, bm, "statistic_date", ["HS300"])["HS300"]
    r_bm.append(D.value_series(bm_reshape))

tbond = pd.read_sql("SELECT 1y_treasury_rate, statistic_date FROM market_index WHERE statistic_date>= '" + (end[60] - dt.timedelta(180)).strftime("%Y-%m-%d") + "' AND statistic_date <= '" + date_s.strftime("%Y-%m-%d") + "'", engine).sort_values("statistic_date", ascending =False)
tbond.index = range(len(tbond))
tbond["1y_treasury_rate"].fillna(method="backfill",inplace=True)
tbond = tbond.loc[tbond["statistic_date"] >= end[60]]
r_f = []

def test(x):
    if x is not None:
        return (1 + x / 100) ** (1 / 12) - 1
    else:
        return None

def none2nan(x):
    if x is None:
        return np.NaN
    else:
        return x

def nan2none(x):
    if np.isnan(x):
        return None
    else:
        return x

def nan2null(x):
    if isinstance(x, float):
        if not np.isnan(x) or np.isinf(x):
            return str(x)
        else:
            return "null"
    else:
        return "null"

for k in interval:
    t_match = TimeModule.match_timeseries_monthly(tbond["statistic_date"].tolist(), k, date_s, end[k])
    tbond_reshape = TimeModule.reshape_data(t_match, tbond, "statistic_date", ["1y_treasury_rate"])["1y_treasury_rate"]
    r_f.append(list(map(test, tbond_reshape))[:-1])
for seq in r_f: #unstable
    for i in range(len(seq) - 1):
        if seq[i] is None:
            seq[i] = seq[i + 1]

datas = [] #Container for each {}
for n in range(N1, N2):
    sql2 = "\
    SELECT T2.org_id, T1.fund_id, nav, statistic_date, T3.foundation_date \
    FROM fund_nv_data as T1,\
	    (SELECT org_id, fund_id FROM fund_org_mapping WHERE org_id in %s) as T2, \
	    (SELECT fund_id, foundation_date FROM fund_info WHERE foundation_date <= '%s') as T3 \
    WHERE T1.fund_id = T2.fund_id AND T1.fund_id= T3.fund_id AND t1.statistic_date<= '%s' \
    ORDER BY org_id ASC, fund_id ASC, statistic_date DESC" % (id_generator(ids, n*100, (n+1)*100), (date_s - relativedelta(months=3)).strftime("%Y-%m-%d %H:%M:%S"), date_s.strftime("%Y-%m-%d %H:%M:%S"))

    data = pd.read_sql(sql2, con=engine)
    data.dropna(inplace=True)
    data.index = range(len(data))

    #list
    ids_o = data["org_id"].drop_duplicates().tolist()
    ids_f = data["fund_id"].drop_duplicates().tolist()
    f_f = data.groupby("fund_id", as_index=False)["statistic_date"].min().as_matrix().T
    f_f = dict(zip(f_f[0], f_f[1]))

    interval_i = []
    for id in ids_f:
         temp = ["y"]
         for k in interval[1:]:
             if end[k] >= f_f[id]:
                 temp.append(k)
         interval_i.append(temp)
    interval_i_mapping = dict(zip(ids_f, interval_i))


    rng_3 = [x for x in range(len(r_bm)) if x != 0 and x != 1 and x != 3]

    for i in range(len(ids_o)):
        print(dt.datetime.now(),"——%s"%str(i))
        #if i % int(len(ids_o) / 20) == 0:
        #    print("Sending Progress%s: %s" % (str(int(i / int(len(ids_o) / 20) * 5/(N2-N1))), str(dt.datetime.now())))
        #    pl_progress = pl.copy()
        #    pl_progress["progress"] = str(int(i / int(len(ids_o) / 20) * 5))
        #    r = s.post(url_progress, pl_progress)
        #    print("Done: %s" % (str(dt.datetime.now())))
        #    print(r.text + "\n")
        d_i = data.loc[data["org_id"] == ids_o[i]]  #get get_data
        d_i_fids = d_i["fund_id"].drop_duplicates().tolist()
        d_i_f = [d_i.loc[d_i["fund_id"]==x] for x in d_i_fids]
        t_reals = [x["statistic_date"].tolist() for x in d_i_f]
        length = len(d_i_fids)

        intervals = [interval_i_mapping[x] for x in d_i_fids]
        intervals = pd.DataFrame(intervals)       
    
        idx = 2
        for x in intervals.columns[::-1]:
            if intervals[x].dropna().size > 0 :
                idx = x
                break
        interval_ = interval[:idx+1]
        length_ = len(interval_)
        rng = rng_3[:length_-3]
        interval_rng = [interval[x] for x in rng]
        print(length, length_)

        nav = []    #***
        r = []
        for k in interval[idx: idx+1]:
            t_matchs = list(map(TimeModule.match_timeseries_monthly, t_reals, [k]*length, [date_s]*length, [end[k]]*length))
            nav.append(list(map(lambda x: x["nav"], map(TimeModule.reshape_data, t_matchs, d_i_f, ["statistic_date"]*length, [["nav"]]*length))))   #*

        nav = np.array(nav)

        if nav.ndim == 2:
            nav = nav.T
            nav = nav[:, 0].tolist()    #i, freq_i
            nav = pd.DataFrame(nav).T.apply(none2nan).as_matrix()
        elif nav.ndim == 3:
            nav = nav.T[:,:,0].tolist()
            nav = [list(map(none2nan, x)) for x in nav]

        nav = [nav[:dlength[x]+1] for x in interval_]

        r = list(map(list, map(np.array, map(C.interval_comprehensiveReturn, nav, [None]*length_, ["all"]*length_))))
        r = [list(map(nan2none, x)) for x in r]
        try:
            nav_weighted = list(map(C.comprehensive_price, nav))
        except TypeError:
            print("!",n, i) 
            continue
    
        c_timing = map(nan2null, map(C.competency_timing, r, r_bm, r_f))  #
        c_stock = map(nan2null, map(C.competency_stock, r, r_bm, r_f))
        #odds = map(nan2null, map(lambda x: x[0],map(C.max_drawdown, nav_weighted)))
        odds = map(nan2null, map(C.odds, r, r_bm))
        persistence = map(nan2null, map(C.sustainability_excess_return, r, r_bm))

        res1 = [c_timing, c_stock, odds, persistence]
        #res2 = [odds]
        #res3 = [calmar_a, sortino_a, info_a, jensen_a, treynor_a]
        res_tmp = {"org_id":ids_o[i], "benchmark":"1", "index_method":"1", "index_range":"1", "type_code":"60001", "type_name":"全产品", "stype_code":"6000101", "stype_name":"全产品", "statistic_date":date_s.strftime("%Y-%m-%d %H:%M:%S"), "update_time":dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        #res_tmp = {"fund_id":fund_ids[i], "statistic_date":date_s_str, "update_time":now_str}
    
        for seq, var in zip(res1, variable_name):
            for res, int_ in zip(seq, interval_):
                res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res

        #for seq, var in zip(res3, variable_name3):
        #    for res, int_ in zip(seq, interval_rng):
        #        res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res    

        datas.append(res_tmp)

pl_result = pl.copy()
pl_result["result"] = str({"db_name":"base",
                           "table_name":"org_month_research",
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



