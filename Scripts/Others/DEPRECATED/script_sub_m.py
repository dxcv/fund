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

def t(your_string):
    print(dt.datetime.now(), "......%s......"%(your_string))

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
    if isinstance(x, float) or isinstance(x, int):
        if not (np.isnan(x) or np.isinf(x)):
            return str(x)
        else:
            return "null"
    else:
        return "null"

interval = ["y",3,6,12,24,36,60]

variable_name = ["con_rise_months", "con_fall_months", "competency_timing", "competency_stock",
                 "er_persistence", "odds", "unsystematic_risk", "tracking_error_a", "var",
                 "month_postive_return", "month_negative_return", "max_return", "min_return",
                 "skewness", "kurtosis", "var_adjusted"]

interval_mapping = {"y":"year_", 3:"m3_", 6:"m6_", 12:"y1_",
                    24:"y2_", 36:"y3_", 60:"y5_"}

indicator_mapping = {"con_rise_months":"con_rise_months", "con_fall_months":"con_fall_months", "competency_timing":"s_time", "competency_stock":"s_security",
                 "er_persistence":"persistence", "odds":"odds", "unsystematic_risk":"unsys_risk", "tracking_error_a":"tracking_error_a", "var":"rvalue",
                 "month_postive_return":"p_earning_months", "month_negative_return":"n_earning_months", "max_return":"max_rate_of_return", "min_return":"min_rate_of_return",
                 "skewness":"skewness", "kurtosis":"kurtosis", "var_adjusted":"rvalue_adjustment_ratio"}

url_status = "http://120.55.69.127:8080/get_data-api/program/forward/status"
url_progress = "http://120.55.69.127:8080/get_data-api/program/forward/progress"
url_result = "http://120.55.69.127:8080/get_data-api/program/get_data/write"

token = "bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
params = sys.argv[1]
#params = "\{exec_type:PROGRAM_TASK,exec_uuid:567e482c-364f-41c4-9e88-2f7364e7bcec\}"  #exec_type: TEST
pl = {"token":str(token), "parms":str(params)}

s = requests.session()
engine = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@127.0.0.1:3306/base?charset=utf8")
engine = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
engine.connect()

for date in range(1, 2):
    date_s = dt.datetime(2016, 7, date)
    date_e = date_s + dt.timedelta(1)
    date_l = date_s-relativedelta(months=1)
    date_s_str = date_s.strftime("%Y-%m-%d %H:%M:%S")
    date_e_str = date_e.strftime("%Y-%m-%d %H:%M:%S")
    date_l_str = date_l.strftime("%Y-%m-%d %H:%M:%S")
    now = dt.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    sql = "\
    SELECT T1.fund_id, T1.nav, T1.statistic_date \
    FROM (SELECT fund_id, nav, statistic_date \
          FROM fund_nv_data \
          WHERE statistic_date < '%s' \
            AND statistic_date >= '%s' \
	      GROUP BY fund_id) as T1 \
    JOIN (SELECT fund_id, foundation_date \
          FROM fund_info \
          WHERE (data_freq = '周度' or data_freq = '周' or data_freq = '月度' or data_freq = '月' or data_freq = '日度' or data_freq = '日') \
            AND foundation_date < '%s') as T2 ON T1.fund_id = T2.fund_id" % (date_e_str, date_s_str, date_l_str)

    ids = engine.execute(sql).fetchall()    #funds which have updated get_data
    ids = list(map(lambda x: x[0], ids))

    def id_generator(ids, start, end):
        id_tupled = []
        end = min(end, len(ids)) if end is not None else len(ids)
        id_tupled = str(tuple(ids[start:end]))
        return id_tupled


    end = dict(zip(interval, list(map(TimeModule.date_infimum, interval, [date_s] * len(interval)))))

    #variable_name2 = ["odds"]

    dlength = {"m":1+1, "s":date_s.month-6+1, "y":date_s.month+1, 
                       1:1+1, 3:3+1, 6:6+1, 12:12+1, 24:24+1, 36:36+1, 60:60+1}

    bm_mapping = {1:"HS300", 2:"CSI500", 3:"CSI50"}
    keys = ['fund_id', 'benchmark', 'statistic_date']; 
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

    bm_mapping = {1:"HS300", 2:"CSI500", 3:"CSI50"}
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

    for k in interval:
        t_match = TimeModule.match_timeseries_monthly(tbond["statistic_date"].tolist(), k, date_s, end[k])
        tbond_reshape = TimeModule.reshape_data(t_match, tbond, "statistic_date", ["1y_treasury_rate"])["1y_treasury_rate"]
        r_f.append(list(map(test, tbond_reshape))[:-1])
    for seq in r_f: #unstable
        for i in range(len(seq) - 1):
            if seq[i] is None:
                seq[i] = seq[i + 1]


    #datas = [] #Container for each {}

    rng_3 = [x for x in range(len(r_bm)) if x != 0 and x != 1 and x != 3]

    N1 = 0; N2 = int(len(ids)/1000)+1
    datas = [] #Container for each {}
    for n in range(N1, 1):
        t("Start getting get_data")
        sql2 = "\
        SELECT fund_id, nav, statistic_date \
        FROM fund_nv_data \
        WHERE fund_id in %s \
          AND statistic_date < '%s'" % (id_generator(ids, n*1000, (n+1)*1000), date_e_str)
        data = pd.read_sql(sql2, con=engine).sort_values(["fund_id", "statistic_date"], ascending=[True, False])
        data.dropna(inplace=True)
        data.index = range(len(data))
        t("Done")
    
        #list
        t("reshaping")
        ids_f = data["fund_id"].drop_duplicates().tolist()
        f_f = data.groupby("fund_id", as_index=False)["statistic_date"].min().as_matrix().T
        f_f = dict(zip(f_f[0], f_f[1]))

        interval_i = []
        for c in ids_f:
             temp = ["y"]
             for k in interval[1:]:
                 if end[k] >= f_f[c]:
                     temp.append(k)
             interval_i.append(temp)
        interval_i_mapping = dict(zip(ids_f, interval_i))

        rng_3 = [x for x in range(len(r_bm)) if x != 0 and x != 1 and x != 3]

        index = data.drop_duplicates("fund_id").index.tolist()
        d_i_f = [data[index[i]:index[i+1]] for i in range(len(index)-1)]
        d_i_f.append(data[index[-1]:])
        t_reals = [x["statistic_date"].tolist() for x in d_i_f]
        length = len(d_i_f)

        intervals_longest = [interval_i_mapping[x] for x in ids_f]
        intervals_longest = [x[-1] for x in intervals_longest]
        ends_longest = [end[k] for k in intervals_longest]

        t("matching timeseries")
        t_matchs = [TimeModule.match_timeseries_monthly(x1,x2,x3,x4) for x1, x2, x3, x4 in zip(t_reals, ["y"]*length, [date_s]*length, [end["y"]]*length)]
        nav_year = [TimeModule.reshape_data(x1, x2, x3, x4)["nav"] for x1, x2, x3, x4 in zip(t_matchs, d_i_f, ["statistic_date"]*length, [["nav"]]*length)]
        t("matching longest timeseries")
        t_matchs = [TimeModule.match_timeseries_monthly(x1,x2,x3,x4) for x1, x2, x3, x4 in zip(t_reals, intervals_longest, [date_s]*length, ends_longest)]
        nav_longest = [TimeModule.reshape_data(x1, x2, x3, x4)["nav"] for x1, x2, x3, x4 in zip(t_matchs, d_i_f, ["statistic_date"]*length, [["nav"]]*length)]

        nav = [[x] for x in nav_year]
        interval_ = [list(map(lambda x: dlength[x], interval[1:interval.index(x)])) for x in intervals_longest]
        intervals = [interval[:interval.index(x)+1] for x in intervals_longest]

        q = pd.DataFrame(nav_longest).as_matrix()
        for x,y in zip(pd.DataFrame(interval_).columns, interval[1:]):
            idxs = np.array(pd.DataFrame(interval_)[x].dropna().index)
            a = q[idxs, :dlength[y]]
            for i in range(len(idxs)):
                nav[idxs[i]].append(a[i].tolist())
        for i in range(length):
            if len(intervals[i]) != 1:  #ensure the longest interval != the first interval(year)
                nav[i].append(nav_longest[i])
        t("Go!")
        for i in range(1): #Calculate 1000 funds
            nav_i = nav[i]
            nav_i = [[nan2none(x) for x in y] for y in nav_i]             
            r = [D.value_series(x) for x in nav_i]

            con_rise_months = map(lambda x: nan2null(x[0]), map(D.month_continuous_rise, r))
            con_fall_months = map(lambda x: nan2null(x[0]), map(D.month_continuous_fall, r))
            competency_timing = map(nan2null, map(D.competency_timing, r, r_bm, r_f))
            competency_stock = map(nan2null, map(D.competency_stock, r, r_bm, r_f))
            er_persistence = map(nan2null, map(D.persistence_er, r, r_bm))
            odds = map(nan2null, map(D.odds, r, r_bm))
            unsystematic_risk = map(nan2null, map(D.unsystematic_risk, r, r_bm, r_f))
            tracking_error_a = map(nan2null, map(D.track_error_a, r, r_bm, [12]*len(r)))
            var = map(nan2null, map(D.value_at_risk, r))
            month_positive_return = map(str, map(D.month_positive_return, r))
            month_negative_return = map(str, map(D.month_negative_return, r))
            max_return = map(lambda x: nan2null(x[0]), map(D.max_return, r))
            min_return = map(lambda x: nan2null(x[0]), map(D.min_return, r))
            skewness = map(nan2null, map(D.skewness, r))
            kurtosis = map(nan2null, map(D.kurtosis, r))
            var_adjusted = map(nan2null, map(D.ERVaR, r, r_f, [12]*len(r)))

            res1 = [con_rise_months, con_fall_months, competency_timing, competency_stock,
                    er_persistence, odds, unsystematic_risk, tracking_error_a, var, 
                    month_positive_return, month_negative_return, max_return, min_return,
                    skewness, kurtosis, var_adjusted]

            res_tmp = {"fund_id":ids_f[i], "benchmark":"1", "statistic_date":date_s.strftime("%Y-%m-%d %H:%M:%S"), "update_time":dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            for seq, var in zip(res1, variable_name):
                for res, int_ in zip(seq, intervals[i]):
                    res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res
            datas.append(res_tmp)
            print("length of get_data:", len(datas))
            
            if len(datas) >= 2000:
                pl_result = pl.copy()
                pl_result["result"] = str({"db_name":"base",
                                           "table_name":"fund_subsidiary_month_index",
                                           "param_fields":fields,
                                           "update_fields":fields,
                                           "datas": datas
                                           })
                r = s.post(url_result, pl_result)
                print("result_api:\n%s" % r.text)
                datas = []

    pl_result = pl.copy()
    pl_result["result"] = str({"db_name":"base",
                                "table_name":"fund_subsidiary_month_index",
                                "param_fields":fields,
                                "update_fields":fields,
                                "datas": datas
                                })
    r = s.post(url_result, pl_result)
    print("result_api:\n%s" % r.text)
    datas = []

    t("Finish")  

pl_status = pl.copy()
pl_status["status"] = "EXEC_SUCCESS"
r = s.post(url_status, pl_status)
print("status_api:\n%s" % r.text)


