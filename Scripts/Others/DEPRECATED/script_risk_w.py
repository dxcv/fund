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

date_s = dt.datetime(2016,7,1)
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
engine = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
engine.connect()

sql = "\
SELECT fund_id FROM(\
    SELECT fund_id, nav, MAX(statistic_date) as statistic_date_latest \
    FROM fund_nv_data \
    WHERE statistic_date < %s \
    GROUP BY fund_id \
    HAVING statistic_date_latest >= %s AND \
    statistic_date_latest < %s) as T" % (date_e_str, date_s_str, date_e_str)

ids = engine.execute(sql).fetchall()    #funds which have updated get_data
ids = list(map(lambda x: x[0], ids))

class Data:
    def __init__(self, statistic_date_s=dt.datetime.now(), con=None, **kwargs):
        self._interval = ["w","m","s","y",1,3,6,12,24,36,60]
        self._start = statistic_date_s
        self._end = dict(zip(self._interval, list(map(TimeModule.date_infimum, self._interval, [statistic_date_s] * len(self._interval)))))
        self._con = con
        self._kwargs = kwargs
        self._data = {}

    def id_generator(self, start, end):
        ids = []
        for i in range(start, min(end, len(self._kwargs["ids"]))):
            ids.append(self._kwargs["ids"][i])
        ids = str(tuple(ids))
        return ids

    def get_data(self, id_start=None, id_end=None):
        if "start" in self._kwargs.keys() and "end" in self._kwargs.keys():
            id_start = self._kwargs["start"]
            id_end = self._kwargs["end"]

        fields = "fund_id, fund_name, statistic_date, nav, added_nav, swanav"
        table = "fund_nv_data"
        self._data = pd.read_sql("SELECT " + fields + " FROM " + table + " WHERE fund_id in " + self.id_generator(id_start, id_end) + "AND statistic_date < %s"%(date_e_str), con=self._con).sort_values(["fund_id","statistic_date"], ascending=[True, False])
        self._data.index = range(len(self._data))

    @property
    def data(self):
        return self._data

    @property
    def end(self):
        return self._end
    
    @property
    def start(self):
        return self._start

    @property
    def interval(self):
        return self._interval

data = Data(statistic_date_s=date_s, start=0, end=100, ids=ids, con=engine)
data.get_data()


interval = data.interval
variable_name = ["stdev", "stdev_a", "dstd_a", "mdd", "beta", "corr"]
interval_mapping = {"w":"week_", "m":"month_", "s":"quarter_", "y":"year_",
                    1:"m1_", 3:"m3_", 6:"m6_", 12:"y1_",
                    24:"y2_", 36:"y3_", 60:"y5_"}
indicator_mapping = {"stdev":"stdev", "stdev_a":"stdev_a", "dstd_a":"dd_a",
                     "mdd":"max_retracement", "beta":"beta", "corr":"benchmark_r"}
bm_mapping = {1:"HS300", 2:"CSI500", 3:"CSI50"}
#
bm = pd.read_sql("SELECT %s, statistic_date FROM market_index WHERE statistic_date>= '"%(bm_mapping[1]) + data.end[60].strftime("%Y-%m-%d") + "' AND statistic_date <= '" + data.start.strftime("%Y-%m-%d") + "'", engine).sort_values("statistic_date", ascending =False)
bm.index = range(len(bm))
r_bm = []
for k in interval:
    t_match = TimeModule.match_timeseries_weekly(bm["statistic_date"].tolist(), k, date_s, data.end[k])
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
for k in interval_3:
    t_match = TimeModule.match_timeseries_monthly(tbond["statistic_date"].tolist(), k, date_s, data.end[k])
    tbond_reshape = TimeModule.reshape_data(t_match, tbond, "statistic_date", ["1y_treasury_rate"])["1y_treasury_rate"]
    r_f.append(list(map(test, tbond_reshape))[:-1])
for seq in r_f: #unstable
    for i in range(len(seq) - 1):
        if seq[i] is None:
            seq[i] = seq[i + 1]

def get_first(x):
    if x[0] != "null":
        return "%.7f" % x[0]
    else:
        return x[0]

def get_7f(x):
    if x != "null":
        return "%.7f" % x
    else:
        return x

fund_ids = data.data["fund_id"].drop_duplicates().tolist()
datas = [] #Container for each {}

rng_3 = [x for x in range(len(r_bm)) if x != 0 and x != 1 and x != 3]

def nan2none(x):
    if x is not None:
        if np.isnan(x): return None
        else: return x
    else:
        return x

        
for i in range(len(fund_ids)):
    #if i % int(len(fund_ids) / 20) == 0:
    #    print("Sending Progress%s: %s" % (str(int(i / int(len(fund_ids) / 20) * 5)), str(dt.datetime.now())))
        #pl_progress = pl.copy()
        #pl_progress["progress"] = str(int(i / int(len(fund_ids) / 20) * 5))
        #r = s.post(url_progress, pl_progress)
        #print("Done: %s" % (str(dt.datetime.now())))
        #print(r.text + "\n")
    data_i = data.data.loc[data.data["fund_id"] == fund_ids[i]]  #get get_data
    t_real = data_i["statistic_date"].tolist()
    
    nav = []
    r = []
    for k in interval:
        t_match = TimeModule.match_timeseries_weekly(t_real, k, date_s, data.end[k]) 
        nav.append(list(map(nan2none, TimeModule.reshape_data(t_match, data_i, "statistic_date", ["nav"])["nav"])))
    

    r = list(map(D.value_series, nav))
    stdev = map(get_7f, list(map(D.standard_deviation, r)))
    stdev_a = map(get_7f, list(map(D.standard_deviation_a, r)))
    dstd_a = map(get_7f, list(map(D.downside_deviation_a, r, r_f)))
    mdd = map(get_first, list(map(D.max_drawdown, nav)))
    beta = map(get_7f, list(map(D.beta, r, r_bm, r_f)))
    corr = map(get_first, list(map(D.corr, r, r_bm)))

    res1 = [stdev, stdev_a, dstd_a, mdd, beta, corr]
    res_tmp = {"fund_id":fund_ids[i], "statistic_date":date_s.strftime("%Y-%m-%d %H:%M:%S"), "update_time":dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    for res_seq, var in zip(res1, variable_name):
        for res, int_ in zip(res_seq, interval):
            res_tmp[interval_mapping[int_] + indicator_mapping[var]] = res

    datas.append(res_tmp)


#i_mdd = list(map(lambda x: x[0],list(map(D.max_drawdown, nav))))

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

#q = datas[0].copy()
#for k in q.keys():
#    if type(q[k]) is not str:
#        q[k] = "%.7f" % float(q[k])

pl_result["result"] = str({"db_name":"base_test",
                           "table_name":"fund_weekly_return",
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

#{"parms":str({"exec_id":"e55e9282-202e-4b6d-ba9a-b3efead9fb22","exec_type":"TEST"}),
# "result":str({"db_name":"base",
#               "table_name":"fund_info",
#               "param_fields":"fund_id,fund_name,reg_code,fund_member",
#               "update_fields":"fund_member",
#               "datas":[{"fund_id":"JR000001","fund_member":"张小川","fund_name":"鹏华清水源","reg_code":"SC7525"},
#                        {"fund_id":"JR027113","fund_member":"康晓阳","fund_name":"天马量化1期","reg_code":""}]
#               })
# }

