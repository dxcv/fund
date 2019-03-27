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


conn = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
conn.connect()


tstd_w = T.timeseries_std(dt.datetime(2016,9,26), T.periods_in_interval(dt.datetime(2016,9,26), dt.datetime(2007,1,1), 12), 52, 4)
tstd_m = T.timeseries_std(dt.datetime(2016,9,26), T.periods_in_interval(dt.datetime(2016,9,26), dt.datetime(2007,1,1), 12), 12, extend=2, use_lastday=True)
tstd_m1 = tstd_m[:-1]


bm = pd.read_sql("SELECT HS300, statistic_date FROM `market_index` WHERE statistic_date >= '2006-06-01' and statistic_date <= '2016-12-31' ORDER BY statistic_date DESC", conn)
bm["statistic_date"] = bm["statistic_date"].apply(lambda x: time.mktime(dt.date.timetuple(x)))

navs_bm = bm["HS300"].tolist()
ts_bm = bm["statistic_date"].tolist()

#MONTH
matchs1 = T.outer_match4index_f7(ts_bm, tstd_m1, False)
matchs2 = T.outer_match4index_b7_2(ts_bm, tstd_m1, False)
matchs3 = T.outer_match4index_m(ts_bm, tstd_m, False)
matchs = merge_result(matchs1, matchs2, matchs3)

bm_t_matchs = matchs[0]
bm_t_matchs = T.tr(bm_t_matchs)
bm_idx_matchs = matchs[1]
bm_nav_matchs = [navs_bm[idx] if idx is not None else None for idx in bm_idx_matchs.values()]

pd.DataFrame([T.tr(tstd_m[::-1]), bm_t_matchs[::-1],bm_nav_matchs[::-1]]).T.to_csv("C:\\Users\\Kael Yu\\Desktop\\bm_m.csv", index=False)


#WEEK
matchs = T.outer_match4index_w(ts_bm, tstd_w)
bm_t_matchs = T.tr(matchs[0])
bm_idx_matchs = matchs[1]
bm_nav_matchs = [navs_bm[idx] if idx is not None else None for idx in bm_idx_matchs.values()]

pd.DataFrame([T.tr(tstd_w[::-1]), bm_t_matchs[::-1], bm_nav_matchs[::-1]]).T.to_csv("C:\\Users\\Kael Yu\\Desktop\\bm_w.csv", index=False)
