from importlib import reload
import sys
sys.path.append("D:/Projects/PythonProject/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("D:/Projects/Python/3.5.2/ZSAssetManagementResearch/Scripts/algorithm")
sys.path.append("/usr/local/python/3.5.2/user-defined-module")
import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
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

def compare(t_min, t_std_m1):
    result = []
    for x in t_min:
        i = 0
        for y in t_std_m1:
            if x <= y: i += 1
            else:
                break
        result.append(i)
    return result

now = dt.datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")

t = 2007
first_date = dt.datetime(first_year, 1, 1 + (6-cld.monthrange(first_year, 1)[0]+1)%7)

d = pd.read_excel("C:\\Users\\Kael Yu\\Desktop\\指数结果核对\\PE_index\\index_zy.xls")[["时间", "私募全市场", "股票多头", "股票多空", "股票市场中性", "债券基金", "管理期货", "宏观策略", "定向增发", "套利策略", "多策略", "组合基金"]]
columns = d.columns.tolist()[1:]

d["时间"] = d["时间"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d"))
d = d.sort_values(["时间"], ascending=False)

t_std_w = T.timeseries_std(dt.datetime(2016,9,26), T.periods_in_interval(dt.datetime(2016,9,26), dt.datetime(2007,1,1), 12), 52, 4)
t_std_m = T.timeseries_std(dt.datetime(2016,9,26), T.periods_in_interval(dt.datetime(2016,9,26), dt.datetime(2007,1,1), 12), 12, extend=2, use_lastday=True)
t_std_m1 = t_std_m[:-1]

ts = d["时间"].apply(lambda x: time.mktime(x.timetuple())).tolist()
navs = [d[col].tolist() for col in columns]

# W


match_w = T.outer_match4index_w(ts, t_std_w, False)
t_match_w = match_w[0]
nav_matchs_w = [[nav[idx] if idx is not None else None for idx in match_w[1].values()] for nav in navs]
tag_w = t_std_w[:-1]

result_w = [tag_w]
result_w.extend(nav_matchs_w)

op1 = pd.DataFrame(result_w).T[::-1]
op1.columns = ["时间", "私募全市场", "股票多头", "股票多空", "股票市场中性", "债券基金", "管理期货", "宏观策略", "定向增发", "套利策略", "多策略", "组合基金"]
op1["时间"] = op1["时间"].apply(dt.datetime.fromtimestamp)
op1.to_csv("C:\\Users\\Kael Yu\\Desktop\\指数结果核对\\PE_index\\index_zy_match_w.csv", index=False)


# M

match_m1 = T.outer_match4index_f7(ts, t_std_m1, False)
match_m2 = T.outer_match4index_b7_2(ts, t_std_m1, False)
match_m3 = T.outer_match4index_m(ts, t_std_m, False)
match_m = merge_result(match_m1, match_m2, match_m3)

t_match_m = match_m[0]
nav_matchs_m = [[nav[idx] if idx is not None else None for idx in match_m[1].values()] for nav in navs]
tag_m = T.timeseries_std(dt.datetime(2016, 10, 10), 118, 12, -1)

result_m = [tag_m]
result_m.extend(nav_matchs_m)

op2 = pd.DataFrame(result_m).T[::-1]
op2.columns = ["时间", "私募全市场", "股票多头", "股票多空", "股票市场中性", "债券基金", "管理期货", "宏观策略", "定向增发", "套利策略", "多策略", "组合基金"]
op2["时间"] = op2["时间"].apply(dt.datetime.fromtimestamp)
op2.to_csv("C:\\Users\\Kael Yu\\Desktop\\指数结果核对\\PE_index\\index_zy_match_m.csv", index=False)
