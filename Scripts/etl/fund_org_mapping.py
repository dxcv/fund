from utils.database import config as cfg, io
import pandas as pd
from functools import wraps
from utils.decofactory import mydecorator
import difflib
import Levenshtein
import jieba
import re

engine = cfg.load_engine()["2G"]
engine_base = cfg.load_engine()["2Gb"]
engine_crawl = cfg.load_engine()["2Gc"]


def multireplace(string, replacements):
    """
    Given a string and a replacement map, it returns the replaced string.

    :param str string: string to execute replacements on
    :param dict replacements: replacement dictionary {value to find: value to replace}
    :rtype: str

    """
    # Place longer ones first to keep shorter substrings from matching
    # where the longer ones should take place
    # For instance given the replacements {'ab': 'AB', 'abc': 'ABC'} against
    # the string 'hey abc', it should produce 'hey ABC' and not 'hey ABc'
    substrs = sorted(replacements, key=len, reverse=True)

    # Create a big OR regex that matches any of the substrings to replace
    regexp = re.compile('|'.join(map(re.escape, substrs)))

    # For each match, look up the new string in the replacements
    return regexp.sub(lambda match: replacements[match.group(0)], string)


def cache(func):
    cached = {}

    @wraps(func)
    def wrap(*args):
        if args not in cached:
            cached[args] = func(*args)
        return cached[args]

    return wrap


def replace_blank(dataframe, fields):
    replacements = {
        "_": "",
        " ": "",
        "(": "",
        ")": "",
        "（": "",
        "）": "",
        "?": "",
        "？": "",
    }
    df_ = dataframe.copy(False)

    # 字符类型数据去除空字符
    processed_fields = ["__" + name + "__PROCESSED" for name in fields]
    df_[processed_fields] = df_[fields].applymap(lambda x: multireplace(x,  replacements=replacements) if x is not None else None)

    # 对于全空字符, 以None替换
    df_ = df_.applymap(lambda x: None if x == "" else x)
    return df_


def sorted_orgs(df_oi):
    import re
    df_oi_ = df_oi.sort_values(["org_id", "org_full_name"])
    df_oi_["_prefix"] = df_oi["org_id"].apply(
        lambda x: re.match("(?P<prefix>BK|FG|FU|SG|TG|P|JG).*", x).groupdict()["prefix"])

    result = pd.DataFrame()
    for prefix in ('JG', 'P', 'TG', 'SG', 'FU', 'FG', 'BK'):
        # 使用优先级越高的ID放越后面, 这样能确保在通过org_name, org_id构建字典时,
        # 如果org_name重复, 优先级高的org_id能够被加入字典;
        result = result.append(df_oi_.loc[df_oi_["_prefix"] == prefix])
    result.index = range(len(result))
    return result[df_oi.columns]


class Orgs:
    _cache = {}

    def __init__(self, engine):
        self._engine = engine

    def info(self):
        sql_oi = "SELECT org_id, org_name, org_full_name FROM base.org_info"
        df_oi = pd.read_sql(sql_oi, self._engine)
        df_oi = replace_blank(df_oi, ["org_name", "org_full_name"])
        df_oi = sorted_orgs(df_oi)
        return df_oi


class XFund:
    def __init__(self, source, engine):
        """
        Args:
            source: str, optional {"fundaccount", "security", "future"}
            engine:
        """
        self._source = source
        self._engine = engine

    def org(self):
        """
            从x_fund_{fundaccount, future, security}表调取基协源数据的基金产品, 相关机构数据;
        Returns:

        """
        sql_srcs = {
            "fundaccount":
                "SELECT fim.fund_ID, source_ID, fi.fund_name, fund_issue_org_amac, fund_custodian_amac, reg_code_amac, DATE(reg_time_amac) as reg_time_amac \
                FROM base.fund_id_match fim \
                JOIN base.fund_info fi ON fim.fund_id = fi.fund_id \
                JOIN crawl.x_fund_info_fundaccount tb_src ON fim.source_ID = tb_src.fund_id AND fim.match_type = 5",

            # x_fund_futures, x_fund_securities源的数据中, 没有备案日期;
            "future":
                "SELECT fim.fund_ID, source_ID, fi.fund_name, fund_issue_org_amac, fund_custodian_amac, reg_code_amac \
                FROM base.fund_id_match fim \
                JOIN base.fund_info fi ON fim.fund_id = fi.fund_id \
                JOIN crawl.x_fund_info_futures tb_src ON fim.source_ID = tb_src.fund_id AND fim.match_type = 5",

            "security":
                "SELECT fim.fund_ID, source_ID, fi.fund_name, fund_issue_org_amac, fund_custodian_amac, reg_code_amac \
                FROM base.fund_id_match fim \
                JOIN base.fund_info fi ON fim.fund_id = fi.fund_id \
                JOIN crawl.x_fund_info_futures tb_src ON fim.source_ID = tb_src.fund_id AND fim.match_type = 5",

            "private": ""
        }
        result = pd.read_sql(sql_srcs[self._source], self._engine)

        result = replace_blank(result, ["fund_issue_org_amac", "fund_custodian_amac", "reg_code_amac"])
        return result


def o(df):
    tmp = df[["fund_issue_org_amac", "matched_full_name", "accuracy"]].sort_values(["accuracy"], ascending=True)
    tmp.to_csv("c:/Users/Yu/Desktop/matched.csv", index=False)


def od(df):
    tmp = df[["fund_issue_org_amac", "matched_full_name", "accuracy"]].drop_duplicates(subset=["fund_issue_org_amac", "matched_full_name"]).sort_values(["accuracy"], ascending=True)
    tmp.to_csv("c:/Users/Yu/Desktop/matched_dd.csv", index=False)


org = Orgs(engine)
df_oi = org.info()

df = XFund("fundaccount", engine).org()
# df = XFund("future", engine).org()
# df = XFund("security", engine).org()

tmp = df_oi.drop_duplicates(subset=["__org_full_name__PROCESSED"], keep="last")
name_dict = dict(zip(tmp["org_id"], tmp["__org_full_name__PROCESSED"]))
oid_dict = dict(zip(tmp["__org_full_name__PROCESSED"], tmp["org_id"]))


@mydecorator.cache
def match_by_Levenshtein(org_full_name):
    print(org_full_name)
    val = 0
    matched_id = None
    matched_name = None
    for oid, ofn in name_dict.items():
        if ofn is not None:
            tmp = Levenshtein.ratio(org_full_name, ofn)
            if tmp > val:
                val, matched_id, matched_name = tmp, oid, ofn
    return matched_id, matched_name, val


df["result"] = df["__fund_issue_org_amac__PROCESSED"].apply(lambda x: match_by_Levenshtein(x))

df["matched_id"] = df["result"].apply(lambda x: x[0])
df["matched_full_name"] = df["result"].apply(lambda x: x[1])
df["accuracy"] = df["result"].apply(lambda x: x[2])

# o(df); od(df)
# 匹配率不等于1的记录占总记录的百分比

# 0.03685684800235884
# 0.03702884662636985
df.loc[df.accuracy != 1].shape[0] / df.shape[0]


# {1: '投资顾问', 2: '基金管理人', 4: '证券经纪人', 5: '托管机构', 7: '行政服务机构'}

fom = pd.read_csv("c:/Users/Yu/Desktop/fom.csv")
fom = fom.loc[fom.org_type_code == 2]   # 基金管理人
matched = df.loc[df.accuracy == 1][["fund_ID", "matched_id"]]

ids1 = set(fom["fund_id"])
d1 = dict(zip(fom["fund_id"], fom["org_id"]))

ids2 = set(matched["fund_ID"])
d2 = dict(zip(matched["fund_ID"], matched["matched_id"]))

diff = set()
for fid, oid in d2.items():
    oid_d1 = d1.get(fid)
    if oid_d1 is not None and oid_d1 != oid:
        diff.add(fid)

print("new: {len_new}, diff: {len_diff}".format(len_new=len(d2.keys() - d1.keys()), len_diff=len(diff)))


# fid = "JR127360"
# pd.read_sql("SELECT org_id, org_name, org_full_name, org_category, found_date, reg_code, reg_time, fund_num, total_fund_num \
# FROM base.org_info WHERE org_id IN ('{oid1}', '{oid2}') ".format(
#     oid1=fom.loc[fom.fund_id == fid]["org_id"].iloc[0],
#     oid2=matched.loc[matched.fund_ID == fid]["matched_id"].iloc[0]), engine)

tmp = df[["fund_ID", "fund_name", "matched_id"]]
tmp.columns = ["fund_id", "fund_name", "org_id"]
tmp["org_type_code"] = 2
tmp["org_type"] = "基金管理人"
io.to_sql("fund_org_mapping_", engine_base, tmp)








#######
a = "a _bd   "
multireplace(a, replacements={"_": "", " ": ""})

df["cut_1"] = df["org_full_name"].apply(lambda x: set(jieba.cut(x)) if x is not None else None)
df["cut_2"] = df["fund_issue_org_amac"].apply(lambda x: set(jieba.cut(x)))

df.loc[df["org_full_name"] != df["fund_issue_org_amac"]][["fund_issue_org_amac", "org_full_name", "accuracy", "cut_1", "cut_2"]].sort_values("accuracy").to_csv("c:/Users/Yu/Desktop/matched.csv", index=False)

# df_xifi.loc[df_xifi["result"].apply(lambda x: x[1]) != df_xifi["result2"].apply(lambda x: x[1])][["fund_issue_org_amac", "result", "result2"]].to_csv("c:/Users/Yu/Desktop/matched.csv", index=False)

s = [("前海开源基金管理有限公司", "前海开源资产管理有限公司"), ("长安国际信托股份有限公司", "长安基金管理有限公司"), ("鑫沅资产管理有限公司", "上海鑫沅股权投资管理有限公司")]
s1, s2 = s[2]
seq = difflib.SequenceMatcher(None, s1, s2)
seq.ratio()
Levenshtein.ratio(s1, s2)


df.loc[df["fund_issue_org_amac"] == df["org_full_name"]]
df.loc[df["fund_issue_org_amac"] != df["org_full_name"]].drop_duplicates("org_full_name")[["org_full_name", "fund_issue_org_amac"]]


cp = df[["fund_issue_org_amac", "org_name", "accuracy"]].copy(False)
cp = cp.drop_duplicates(subset=["fund_issue_org_amac", "org_name"])
cp = cp.loc[cp["fund_issue_org_amac"] != cp["org_name"]]
cp.to_excel("c:/Users/Yu/Desktop/matched.xlsx", index=False)


