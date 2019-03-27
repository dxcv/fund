import sys

sys.path.append("D:/Projects/PythonProject/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("D:/Projects/Python/3.5.2/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("/usr/local/python/3.5.2/user-defined-module")
import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np, pandas as pd
import requests
from sqlalchemy import create_engine
import utils.algorithm.timeutils as TU
from utils.script.scriptutils import *

engine = create_engine("mysql+pymysql://root:smyt1234@583e37fb680e7.sh.cdb.myqcloud.com:3774/base",
                       connect_args={"charset": "utf8"})

now = dt.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")

first_year = 2007
first_date = dt.datetime(first_year, 1, 1 + (6 - cld.monthrange(first_year, 1)[0] + 1) % 7)

result_r = {}
components_num = {}

for year in range(first_date.timetuple().tm_year, now.timetuple().tm_year + 1):

    if year == now.timetuple().tm_year:
        month = 12
        day = 5
    else:
        month = 12
        day = 31

    date_s = dt.date(year, month, day)  #
    date_e = date_s + dt.timedelta(1)
    date_e_str = date_e.strftime("%Y-%m-%d")

    sql_i = "\
    SELECT fund_id, nav, statistic_date FROM fund_nv_data_standard WHERE statistic_date <= '{0}' and statistic_date >= '{1}' \
    AND nav IS NOT NULL AND statistic_date IS NOT NULL \
    ORDER BY fund_id ASC, statistic_date DESC \
    ".format(dt.date(year, 12, 31), dt.date(year - 1, 12, 18))

    sql_mindate = "\
    SELECT fund_id, MIN(statistic_date) as statistic_date_earliest FROM fund_nv_data_standard WHERE fund_id IN \
    (SELECT fund_id FROM fund_nv_data_standard WHERE statistic_date <= '{0}' and statistic_date >= '{1}' \
    AND nav IS NOT NULL AND statistic_date IS NOT NULL \
    ORDER BY fund_id ASC, statistic_date DESC) GROUP BY fund_id\
    ".format(dt.date(year, 12, 31), dt.date(year - 1, 12, 18))

    conn = engine.connect()

    tic("Getting Data")
    d = pd.read_sql(sql_i, conn)
    d.index = range(len(d))

    t_min = pd.read_sql(sql_mindate, conn)["statistic_date_earliest"].tolist()
    t_min = [time.mktime(x.timetuple()) for x in t_min]  #

    tic("Preprocessing...")
    d["statistic_date"] = d["statistic_date"].apply(lambda x: time.mktime(x.timetuple()))
    d_dd = d.drop_duplicates("fund_id")
    idx_slice = d_dd.index.tolist()
    idx_slice.append(len(d))
    ids = d_dd["fund_id"].tolist()

    last_monday = date_s - dt.timedelta(cld.weekday(date_s.year, date_s.month, date_s.day))  #
    # t_std = T.timeseries_std(last_monday, T.periods_in_interval(last_monday, dt.date(year-1,12,31), 12))    #
    t_std = TU.timeseries_std(last_monday, "a", 52, extend=1)  #
    if year == first_date.timetuple().tm_year: t_std = t_std[:-1]

    #
    tic("Slicing")
    t_std_long = TU.timeseries_std(last_monday, TU.periods_in_interval(last_monday, dt.date(year - 1, 11, 30), 12))
    t_std_long_p1m = [(x + relativedelta(months=1)).timestamp() for x in TU.tr(t_std_long)]
    real_p1m = compare(t_min, t_std_long)  # 实际最早日期和标准序列日期比较
    p1m_std = compare(t_std_long_p1m, t_std)  # 加一个月的标准序列日期和标准序列日期比较
    data_used = [p1m_std[x - 1] for x in real_p1m]

    tic("Grouping...")
    ds = [d[idx_slice[i]:idx_slice[i + 1]] for i in range(len(idx_slice) - 1)]
    ts = [x["statistic_date"].tolist() for x in ds]
    navs = [x["nav"].tolist() for x in ds]

    tic("Matching...")
    matchs = [TU.outer_match4index_w(x, t_std, False) for x in ts]

    tic("Getting Result...")
    t_matchs = [x[0] for x in matchs]
    t_matchs = [TU.tr(x) for x in t_matchs]
    idx_matchs = [x[1] for x in matchs]
    nav_matchs = [[navs[i][idx] if idx is not None else None for idx in idx_matchs[i].values()] for i in
                  range(len(idx_matchs))]

    tic("Calculating Index...")
    nvs = pd.DataFrame(nav_matchs).T.astype(float).as_matrix()
    for i in range(len(ids)):
        nvs[data_used[i] + 1:, i] = np.nan

    rs = nvs[:-1] / nvs[1:] - 1
    rs[rs > 10] = np.nan
    rs[rs < -1] = np.nan
    r = np.nanmean(rs, axis=1)
    r[np.isnan(r)] = 0

    result_r[year] = r
    components_num[year] = np.sum(~np.isnan(rs), axis=1)
    tic("Year:{0}, Done...".format(year))

values_r = []
values_num = []
for year in range(first_date.timetuple().tm_year, now.timetuple().tm_year + 1):
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

tag = TU.timeseries_std(dt.datetime(year, month, day),
                        TU.periods_in_interval(dt.datetime(year, month, day), dt.datetime(first_year, 1, 1), 12), 52,
                        extend=5)[::-1]
tag = [x for x in tag if x >= first_date.timestamp()]
tag = [dt.date.fromtimestamp(x) for x in tag]

# op = pd.DataFrame(list(zip(tag, result, values_num)))
# op.columns = ["date", "index_w_yu", "components_num"]
# op.to_csv("C:\\Users\\Kael Yu\\Desktop\\fund_index_w.csv", index=False)

# Send Result

url_status = "http://120.55.69.127:8080/get_data-api/program/forward/status"
url_progress = "http://120.55.69.127:8080/get_data-api/program/forward/progress"
url_result = "http://120.55.69.127:8080/get_data-api/program/get_data/write"
s = requests.session()

token = "bda1850a2a907fa33b0cb1241f5f742bb4138b1c"
params = sys.argv[1]
pl = {"token": str(token), "parms": str(params)}

datas = []
for i in range(len(tag)):
    res_tmp = {"index_id": "FI01", "index_name": "私募全市场指数", "statistic_date": str(tag[i]), "update_time": now_str,
               "typestandard_code": "600", "typestandard_name": "按全产品", "type_code": "60001", "type_name": "全产品",
               "stype_code": "6000101", "stype_name": "全产品", "index_method": "1", "data_source": "0",
               "data_source_name": "私募云通",
               "index_value": str(result[i]), "funds_number": str(values_num[i])
               }
    datas.append(res_tmp)

fields = ""
for field in datas[0].keys():
    fields += ",%s" % (field)
fields = fields[1:]

pl_result = pl.copy()
pl_result["result"] = str({"db_name": "base",
                           "table_name": "fund_weekly_index",
                           "param_fields": fields,
                           "update_fields": fields,
                           "datas": datas
                           })
r = s.post(url_result, pl_result)
print("result_api:\n%s" % r.text)

pl_status = pl.copy()
pl_status["status"] = "EXEC_SUCCESS"
r = s.post(url_status, pl_status)
print("status_api:\n%s" % r.text)
