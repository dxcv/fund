import requests
import json
import numpy as np
import pandas as pd

_url = {"nav_dividend": "http://120.55.69.127:8080/get_data-api/external/search/fund/nav_dividend",
        "base_info": "http://120.55.69.127:8080/get_data-api/external/search/fund/base_info",
        "performance_indicator": "http://120.55.69.127:8080/get_data-api/external/search/fund/return"}

_ss = requests.session()
_ss.headers["Token"] = "ddd3c823-0b22-4d38-8d50-b94c460cfeb6"


def get_nav(fund_ids, start, end, result_type="df"):
    u1 = _url["nav_dividend"] + "/nav"
    u2 = _url["nav_dividend"] + "/added_nav"

    start = str(start);
    end = str(end)
    start = start[:4] + "-" + start[4:6] + "-" + start[6:]
    end = end[:4] + "-" + end[4:6] + "-" + end[6:]

    payload1 = {"fund_ids": fund_ids, "start_time": start, "end_time": end}
    payload2 = {"fund_ids": fund_ids, "start_time": start, "end_time": end, "param_fields": "added_nav"}

    d1 = _ss.post(u1, payload1).json()["responseData"]
    d2 = _ss.post(u2, payload1).json()["responseData"]

    d = {"nav": list(map(lambda x: x["nav"], d1[fund_ids])),
         "statistic_date": list(map(lambda x: x["statistic_date"], d1[fund_ids])),
         }

    if result_type == "df":
        return pd.DataFrame(d)
    elif result_type == "dict":
        return d


def get_fundname(fund_ids):
    u = _url["base_info"] + "/fund_name"
    payload = {"fund_ids": fund_ids}
    return _ss.post(u, payload).json()["responseData"]


def info_filter():
    u = _url["base_info"] + "/filter_by"
    payload = {"from": "0", "size": "100", "foundation_date": "inf: 2015-01-01,sup: 2015-12-31"}
    return _ss.post(u, payload).json()["responseData"]
