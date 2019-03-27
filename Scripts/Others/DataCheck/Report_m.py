import os
import calendar as cld, datetime as dt
import pandas as pd
import utils.script.scriptutils as su
import utils.algorithm.timeutils as tu
import utils.database.config as cfg
import utils.database.sqlfactory as sf
import re

engine = cfg.load_engine()["2Gb"]
conn = engine.connect()

year, month = 2017, 4

tgt_folder = "{desktop_path}/Report_m".format(desktop_path=su.get_desktop_path())
has_folder = os.path.isdir(tgt_folder)

file_path = "{main}/{year}_{month}".format(main=tgt_folder, year=year, month="0"*(2-len(str(month)))+str(month))

if has_folder:
    os.mkdir(file_path)
else:
    os.mkdir(tgt_folder)
    os.mkdir(file_path)


mrange = cld.monthrange(year, month)[1]
date_s = dt.date(year, month, mrange)
statistic_date = '{0}-{1}-{2}'.format(year, month, mrange)

sql_fund_num = "SELECT COUNT(DISTINCT fund_id) FROM fund_info WHERE entry_time <= '{0}'".format(statistic_date)    # 截至当前日期的基金数量
sql_fund_withnv_num = "SELECT COUNT(DISTINCT fund_id) FROM fund_nv_data_standard WHERE fund_id IN (SELECT DISTINCT fund_id FROM fund_info WHERE entry_time <= '{0}')".format(statistic_date)
sql_fund_nv = "SELECT fund_id, nav, statistic_date FROM fund_nv_data_standard WHERE fund_id IN (SELECT fund_id FROM fund_info WHERE entry_time<= '{0}') AND statistic_date >= '1970-1-2' ORDER BY fund_id ASC, statistic_date DESC".format(statistic_date)
sql_fund_50m_num = "SELECT COUNT(DISTINCT fund_id) as NUM FROM fund_asset_scale WHERE asset_scale >= 5000 AND entry_time <= '{0}'".format(statistic_date)
sql_fund_w_num = "SELECT COUNT(fund_id) FROM fund_info WHERE data_freq = '周度'"
sql_fund_m_num = "SELECT COUNT(fund_id) FROM fund_info WHERE data_freq = '月度'"


sql_fund_foundationdate = "SELECT fund_id, foundation_date FROM fund_info"
d_fund_foundationdate = pd.read_sql(sql_fund_foundationdate, conn)
funds_with_foundationdate = set(d_fund_foundationdate.dropna()["fund_id"].tolist())
funds_without_foundationdate = set(d_fund_foundationdate["fund_id"].tolist()) - funds_with_foundationdate

sql_fund_firstnvdate = "SELECT fund_id, MIN(statistic_date) as first_nv_date FROM fund_nv_data_standard WHERE fund_id IN {0} AND statistic_date >= '1970-01-02' GROUP BY fund_id".format(tuple(funds_without_foundationdate))
d_fund_firstnvdate = pd.read_sql(sql_fund_firstnvdate, conn).dropna()
d_fund_foundationdate = d_fund_foundationdate.dropna()

tmins = dict(zip(d_fund_foundationdate["fund_id"], d_fund_foundationdate["foundation_date"]))
for fid, fnvdate in zip(d_fund_firstnvdate["fund_id"], d_fund_firstnvdate["first_nv_date"]):
    tmins[fid] = fnvdate

d_fund_nv = pd.read_sql(sql_fund_nv, conn)
d_fund_nv = d_fund_nv.dropna()
d_fund_nv = d_fund_nv.drop(d_fund_nv.loc[d_fund_nv["statistic_date"] < dt.date(1970,1,2)].index)
d_fund_nv["statistic_date"] = d_fund_nv["statistic_date"].apply(su.date2tstp)
d_fund_nv.index = range(len(d_fund_nv))

idxs = su.idx4slice(d_fund_nv, "fund_id")
navs = su.slice(d_fund_nv, idxs, "nav")
ts = su.slice(d_fund_nv, idxs, "statistic_date")
ids = d_fund_nv["fund_id"].drop_duplicates().tolist()

t_std_totals = []
for id_ in ids:
    try:
        t_std_total = tu.timeseries_std(date_s, tu.periods_in_interval(date_s, tmins[id_], 12), periods_y=12, extend=1, use_last_day=True)
        t_std_totals.append(t_std_total[:-1])
    except Exception as e:
        print(e)
        print(id_)

t_std_totals_len = [len(x)-1 for x in t_std_totals]

matchs = [tu.outer_match4indicator_m(t_real, t_std_total, False) for t_real, t_std_total in zip(ts, t_std_totals)]
idx_matchs = [list(match[1].values()) for match in matchs]
t_matchs = [match[0] for match in matchs]


r_con = {}
for since in [2014, 2015]:
    fund_discontinuous = []
    tolerance = 1
    shift = 0   #unit: month
    for i in range(len(idx_matchs)):
        if idx_matchs[i][shift:month+12*(year-since)].count(None) > tolerance or len(idx_matchs[i]) < month - shift + (year-since-1)*12:
            fund_discontinuous.append(i)
    r_con[since] = len(ids) - len(fund_discontinuous)


d_fund_nv2 = d_fund_nv.copy()
d_fund_nv2["statistic_date"] = d_fund_nv2["statistic_date"].apply(dt.date.fromtimestamp)
d_fund_nv2["year"] = d_fund_nv2["statistic_date"].apply(lambda x: x.year)
d_fund_nv2["month"] = d_fund_nv2["statistic_date"].apply(lambda x: x.month)
d_gbf = d_fund_nv2.groupby(["fund_id"])["fund_id"].count()
d_gbft = d_fund_nv2.groupby(["fund_id", "year"])["fund_id"].count()

# 1 CHFDB数据库统计
# 1-1
r_fund_num = conn.execute(sql_fund_num).fetchone()[0]   #总共基金数量

r_fund_withnv_num = conn.execute(sql_fund_withnv_num).fetchone()[0] #有净值基金数量

r_fund_with4nv_num = sum(d_gbf >= 4)    #成立以来至少有4个净值

tmp = (d_gbft >= 1)
r_fund_with1nva = [id for id in ids if sum(tmp[id]) >= (2016-tmins[id].year+1)] #成立以来每年有净值

tmp = (d_gbft >= 10)
r_fund_with4nva = [id for id in ids if sum(tmp[id]) >= (2016-tmins[id].year+1)] #成立以来每年有4个净值

r_fund_50m_num = conn.execute(sql_fund_50m_num).fetchone()[0]   #规模超过5千万

r_fund_w_num = conn.execute(sql_fund_w_num).fetchone()[0]   #周频基金数量
r_fund_m_num = conn.execute(sql_fund_m_num).fetchone()[0]   #月频基金数量


# 1-2
sql_issuance_accumulative = "\
SELECT type_code, type_name, COUNT(T_fi.fund_id) as NUM FROM \
(SELECT fund_id FROM fund_info WHERE entry_time < '{0}') as T_fi \
JOIN (SELECT fund_id, type_code, type_name FROM fund_type_mapping WHERE flag=1 AND typestandard_code = 3) as T_ft ON T_fi.fund_id = T_ft.fund_id \
GROUP BY type_code \
ORDER BY NUM DESC, type_code ASC".format(statistic_date)

sql_issuance = "\
SELECT type_code, type_name, COUNT(T_fi.fund_id) as NUM FROM \
(SELECT fund_id FROM fund_info WHERE entry_time < '{0}' AND foundation_date >= '{1}') as T_fi \
JOIN (SELECT fund_id, type_code, type_name FROM fund_type_mapping WHERE flag=1 AND typestandard_code = 3) as T_ft ON T_fi.fund_id = T_ft.fund_id \
GROUP BY type_code \
ORDER BY NUM DESC, type_code ASC".format(statistic_date, dt.date(year, month, mrange)-dt.timedelta(mrange-1))

r_issuance_accumulative = pd.read_sql(sql_issuance_accumulative, conn)  #各种发行渠道的累计产品数量
r_issuance = pd.read_sql(sql_issuance, conn)    #各种发行渠道的新增产品数量

# 1-3
sql_fund_liquidation_num = "SELECT COUNT(fund_id) FROM fund_info WHERE fund_status = '终止'".format(dt.date(year, 1, 1))
r_fund_liquidation_num = conn.execute(sql_fund_liquidation_num).fetchone()[0]   #累计已经清算的产品

# 2 产品维度
# 2-1
sql_strategy_accumulative = "\
SELECT type_code, type_name, COUNT(T_fi.fund_id) as NUM FROM \
(SELECT fund_id FROM fund_info WHERE entry_time < '{0}') as T_fi \
JOIN (SELECT fund_id, type_code, type_name FROM fund_type_mapping WHERE flag=1 AND typestandard_code = 1) as T_ft ON T_fi.fund_id = T_ft.fund_id \
GROUP BY type_code \
ORDER BY NUM DESC, type_code ASC".format(statistic_date)

sql_strategy_stype_accumulative = "\
SELECT stype_code, stype_name, COUNT(T_fi.fund_id) as NUM FROM \
(SELECT fund_id FROM fund_info WHERE entry_time < '{0}') as T_fi \
JOIN (SELECT fund_id, stype_code, stype_name FROM fund_type_mapping WHERE flag=1 AND typestandard_code = 1) as T_ft ON T_fi.fund_id = T_ft.fund_id \
GROUP BY stype_code \
ORDER BY NUM DESC, stype_code ASC".format(statistic_date)

r_strategy_accumulative = pd.read_sql(sql_strategy_accumulative, conn)  #各种一级策略下的截至当前日期的累计产品数量

r_strategy_stype_accumulative = pd.read_sql(sql_strategy_stype_accumulative, conn)  #各种二级策略下的截至当前日期的累计产品数量

# 3 投顾维度
# 3-1
sql_org_num = "SELECT COUNT(DISTINCT org_id) FROM org_info WHERE entry_time <= '{0}'".format(statistic_date)
sql_org_mfund = "SELECT COUNT(DISTINCT org_id) FROM fund_org_mapping WHERE org_type_code = 1 AND org_id IN (SELECT DISTINCT org_id FROM org_info WHERE entry_time <= '{0}')".format(statistic_date)
sql_org_mfund_withnv = "SELECT COUNT(DISTINCT org_id) FROM fund_org_mapping WHERE org_type_code = 1 AND org_id IN (SELECT DISTINCT org_id FROM org_info WHERE entry_time <= '{0}') AND fund_id IN (SELECT DISTINCT fund_id FROM fund_nv_data_standard)".format(statistic_date)
sql_org_3strt = "SELECT COUNT(*) FROM ( \
SELECT org_id, COUNT(type_code) as mgttp_num FROM ( \
SELECT org_id, type_code FROM \
(SELECT org_id, fund_id FROM fund_org_mapping WHERE org_type_code=1 AND org_id IN (SELECT DISTINCT org_id FROM org_info WHERE entry_time <= '{0}')) as T_of \
JOIN (SELECT fund_id, type_code FROM fund_type_mapping WHERE flag=1 AND typestandard_code=1) as T_fs ON T_of.fund_id = T_fs.fund_id \
GROUP BY org_id, type_code) as T \
GROUP BY org_id HAVING mgttp_num >= 3) as T_main".format(statistic_date)
sql_org_mgt_scale = "SELECT asset_mgt_scale_range, COUNT(org_id) as org_num FROM org_info WHERE \
((asset_mgt_scale_range >= 7 and asset_mgt_scale_range <= 9) or (asset_mgt_scale_range >= 12 and asset_mgt_scale_range <= 13)) \
AND org_id IN (SELECT DISTINCT org_id FROM org_info WHERE entry_time <= '{0}') \
GROUP BY asset_mgt_scale_range".format(statistic_date)
mgt_scale_mapping = {7: "10至20亿",
                     8: "20至50亿",
                     9: "50亿以上",
                     13: "0至1亿",
                     12: "1至10亿"}

r_org_num = conn.execute(sql_org_num).fetchone()[0] #截至当前日期的投顾总数
r_org_mfund = conn.execute(sql_org_mfund).fetchone()[0] #有管理基金的投顾数
r_org_mfund_withnv = conn.execute(sql_org_mfund_withnv).fetchone()[0]   #有管理并且有披露净值的
r_org_3strt = conn.execute(sql_org_3strt).fetchone()[0] #拥有超过3个策略的投顾数
r_org_mgt_scale = pd.read_sql(sql_org_mgt_scale, conn)
#r_org_mgt_scale["asset_mgt_scale_range"] = r_org_mgt_scale["asset_mgt_scale_range"].apply(lambda x: mgt_scale_mapping[x])


r_org_mNy = {}  #管理基金超过N年的投顾公司
for N in range(1, 4):
    sql_org_mNy = "SELECT COUNT(*) FROM ( \
    SELECT DISTINCT org_id FROM( \
    SELECT T_of.org_id, MIN(T_fi.foundation_date) as Tmin FROM \
    (SELECT	org_id, fund_id FROM fund_org_mapping WHERE	org_type_code = 1 AND org_id IN \
    (SELECT DISTINCT org_id FROM org_info WHERE entry_time <= '{0}')) as T_of \
    JOIN (SELECT fund_id, foundation_date FROM fund_info) as T_fi ON T_of.fund_id = T_fi.fund_id \
    GROUP BY T_of.org_id HAVING Tmin <= '{1}') as T1 \
    UNION \
    SELECT DISTINCT org_id FROM( \
    SELECT T_of.org_id, MIN(T_fi.Tmin_f) as Tmin FROM \
    (SELECT org_id, fund_id FROM fund_org_mapping WHERE org_type_code = 1 AND org_id IN \
    (SELECT DISTINCT org_id FROM org_info WHERE entry_time <= '{0}')) as T_of \
    JOIN (SELECT fund_id, MIN(statistic_date) as Tmin_f FROM fund_nv_data_standard GROUP BY fund_id) as T_fi ON T_of.fund_id = T_fi.fund_id \
    GROUP BY T_of.org_id HAVING Tmin <= '{1}') as T2 \
    ) as T_main".format(statistic_date, dt.date(year-N, month, mrange))
    r_org_mNy[N] = conn.execute(sql_org_mNy).fetchone()[0]

# 4 投资经理维度
# 4-1
sql_manager_num = "SELECT COUNT(DISTINCT user_id) FROM `manager_info` WHERE (duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%') AND entry_time <= '{0}'".format(statistic_date)
sql_manager_undergraduate_num = "SELECT COUNT(DISTINCT user_id) FROM manager_info \
WHERE (duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%') AND education IN ('初中及以下', '高中', '大专', '本科', '学士') \
AND entry_time <= '{0}'".format(statistic_date)
sql_manager_graduate_num = "SELECT COUNT(DISTINCT user_id) FROM manager_info \
WHERE (duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%') AND education IN ('硕士') \
AND entry_time <= '{0}'".format(statistic_date)
sql_manager_phd_num = "SELECT COUNT(DISTINCT user_id) FROM manager_info \
WHERE (duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%') AND education IN ('博士') \
AND entry_time <= '{0}'".format(statistic_date)
sql_manager_year = "SELECT user_id, investment_years FROM manager_info \
WHERE (duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%') AND investment_years IS NOT NULL AND investment_years <> '' AND entry_time <= '{0}'".format(statistic_date)
sql_manager_qualified = "SELECT COUNT(DISTINCT user_id) FROM manager_info \
WHERE (duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%') AND is_fund_qualification = 1 \
AND entry_time <= '{0}'".format(statistic_date)

sql_manager_background = "SELECT background, COUNT(user_id) as num FROM manager_info WHERE duty LIKE '%%基金经理%%' or duty LIKE '%%投资经理%%' \
AND entry_time <= '{0}' AND background IS NOT NULL GROUP BY background".format(statistic_date)

r_manager_num = conn.execute(sql_manager_num).fetchone()[0] #基金经理总数
r_manager_undergraduate_num = conn.execute(sql_manager_undergraduate_num).fetchone()[0]
r_manager_graduate_num = conn.execute(sql_manager_graduate_num).fetchone()[0]
r_manager_phd_num = conn.execute(sql_manager_phd_num).fetchone()[0]
r_manager_year = pd.read_sql(sql_manager_year, conn)
def get_investmentyear(x):
    patt = "\d+"
    tmp = re.search(patt, x)
    if tmp is None:
        return None
    else:
        return int(tmp.group(0))
r_manager_year = r_manager_year.dropna()
r_manager_year["investment_years"] = r_manager_year["investment_years"].apply(lambda x: get_investmentyear(x))
r_manager_year = r_manager_year.dropna()
manager_year = {0: len(r_manager_year.loc[r_manager_year["investment_years"]<5]),
                1: len(r_manager_year.loc[(r_manager_year["investment_years"]<10) & (r_manager_year["investment_years"]>=5)]),
                2: len(r_manager_year.loc[(r_manager_year["investment_years"]<15) & (r_manager_year["investment_years"]>=10)]),
                3: len(r_manager_year.loc[r_manager_year["investment_years"]>=15])
    }

r_manager_qualified = conn.execute(sql_manager_qualified).fetchone()[0]

r_manager_background = pd.read_sql(sql_manager_background, conn)


#EXPORT
report_11 = "截止{0},私募云通共收录了{1}只私募产品, 其中: 有净值的基金共有{2}只;\n\
成立以来, 披露净值超过四次的有{3}只, 每年都有净值的基金有{4}只, 每年披露净值不小于四次的基金共有{5}只, 规模在5千万以上的基金共有{6}只;\n\
成立以来, 连续披露(2015年至今最多中断一次)的基金有{7}只;成立以来,连续披露(2014年至今最多中断一次)的基金有{8}只.\
".format(statistic_date, r_fund_num, r_fund_withnv_num, r_fund_with4nv_num, len(r_fund_with1nva), len(r_fund_with4nva), r_fund_50m_num, r_con[2015], r_con[2014])

with open(file_path + "/report_11.txt", "w") as f:
    f.write(report_11)

report_12 = [r_issuance_accumulative, r_issuance]
report_names = ["issuance_accumulative", "issuance"]
for report, name in zip(report_12, report_names):
    report.to_csv(file_path + "/report_12-{0}.csv".format(name), index=False)

report_21 = [r_strategy_accumulative, r_strategy_stype_accumulative]
report_names = ["strategy_accumulative", "strategy_stype_accumulative"]
for report, name in zip(report_21, report_names):
    report.to_csv(file_path + "/report_21-{0}.csv".format(name), index=False)

report_31 = "截止{0},私募云通共收录了{1}家机构信息, 其中: 有管理基金的投顾共有{2}家;\n\
有管理基金并披露过净值的有{3}家; 管理基金超过1/2/3年的投顾分别有{4}/{5}/{6}家; 拥有两个以上策略的投顾有{7}家, 管理规模在10亿以上的投顾有{8}家.\
".format(statistic_date, r_org_num, r_org_mfund, r_org_mfund_withnv, r_org_mNy[1], r_org_mNy[2], r_org_mNy[3], r_org_3strt,
         r_org_mgt_scale.loc[(r_org_mgt_scale["asset_mgt_scale_range"]<=9) & (r_org_mgt_scale["asset_mgt_scale_range"]>=7)]["org_num"].sum())
with open(file_path + "/report_31.txt", "w") as f:
    f.write(report_31)

report_41 = "截止{0},私募云通共收录了{1}位基金经理, 其中: 学士及以下学历的有{2}位, \
硕士学历的有{3}位, 博士学历的有{4}位;\n投资年限在5年以下的有{5}位, 5至10年的有{6}位, 10至15年的有{7}位, 15年以上的有{8}位;\n具备基金从业资格的基金经理有{9}位.\n\
".format(statistic_date, r_manager_num, r_manager_undergraduate_num, r_manager_graduate_num, r_manager_phd_num,
         manager_year[0], manager_year[1], manager_year[2], manager_year[3], r_manager_qualified)
with open(file_path + "/report_41.txt", "w") as f:
    f.write(report_41)

report_42 = [r_manager_background]
report_names = ["r_manager_background"]
for report, name in zip(report_42, report_names):
    report.to_csv(file_path + "/report_42-{0}.csv".format(name), index=False)

