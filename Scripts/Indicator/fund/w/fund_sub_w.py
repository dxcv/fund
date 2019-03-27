import calendar as cld, datetime as dt
import numpy as np, pandas as pd
from utils.database import sqlfactory as sf, config as cfg, io
import utils.script.scriptutils as su
from dateutil.relativedelta import relativedelta
from utils.algorithm import fund_indicator as fi, timeutils as tu
from utils.script.scriptutils import tic

table = sf.Table("w", "sub")
columns = ["fund_id", "statistic_date", "benchmark"]
columns.extend(table.columns())

engines = cfg.load_engine()
engine_wt = engines["2Gb"]

now = dt.datetime.now()
df_whole = pd.DataFrame()

for year in range(2017, 2018):
    for month in range(1, 2):
        conn = engines["2Gb"].connect()

        month_range = cld.monthrange(year, month)[1]
        time_to_fill = sf.Time(dt.datetime(year, month, month_range))
        year, month = time_to_fill.year, time_to_fill.month
        month_range = time_to_fill.month_range

        sql_bm = sf.SQL.market_index(time_to_fill.today)  # Get benchmark prices
        sql_pe = sf.SQL.pe_index(time_to_fill.today, freq="w")

        bm = pd.read_sql(sql_bm, conn)
        bm["y1_treasury_rate"] = bm["y1_treasury_rate"].fillna(method="backfill")
        bm["y1_treasury_rate"] = bm["y1_treasury_rate"].apply(su.annually2weekly)
        bm["statistic_date"] = bm["statistic_date"].apply(su.date2tstp)
        pe = pd.read_sql(sql_pe, conn)
        pe["statistic_date"] = pe["statistic_date"].apply(su.date2tstp)

        prices_bm = [bm["hs300"].tolist(), bm["csi500"].tolist(), bm["sse50"].tolist(), bm["cbi"].tolist(),
                     bm["nfi"]]  ###
        price_pe = pe["index_value"].tolist()
        r_tbond = bm["y1_treasury_rate"].tolist()
        t_bm = bm["statistic_date"].tolist()
        t_pe = pe["statistic_date"].tolist()

        intervals = table.intervals
        intervals3 = [0, 1, 2, 3, 4, 5, 6, 10, 11]

        conn.close()

        result = []
        for mday in range(1, month_range + 1):
            print("Day {0}: {1}".format(mday, dt.datetime.now()))
            conn = engines["2Gb"].connect()

            date_s = sf.Time(dt.datetime(year, month, mday))  # Generate statistic_date

            sql_ids_used = sf.SQL.ids_updated_sd(date_s.today)  # Get ids of fund which has new nav get_data on this day
            ids_used = [x[0] for x in conn.execute(sql_ids_used).fetchall()]

            print("length of ids: {0}".format(len(ids_used)))

            if len(ids_used) == 0: continue  # Check whether there are updated get_data this day

            tic("Getting Data")
            sql_fdate = sf.SQL.foundation_date(ids_used)  # Get their foundation date
            fdate = pd.read_sql(sql_fdate, conn)
            ids_fdate = fdate["fund_id"].tolist()
            t_min = fdate

            if len(ids_fdate) != len(
                    ids_used):  # Check whether all fund has foundation date get_data. If not, use first nv get_data instead.
                ids_diff = list(set(ids_used) - set(ids_fdate))
                sql_fnvdate = sf.SQL.firstnv_date(ids_diff)
                fnvdate = pd.read_sql(sql_fnvdate, conn)
                ids_diff = fnvdate["fund_id"].tolist()
                t_min = pd.merge(fdate, fnvdate, "outer")
                print("Some foundation_date missed, use first nv date...", len(ids_diff), len(ids_fdate), len(t_min))
                t_min = t_min.sort_values("fund_id", ascending=True)  # Sort the df by fund id ASC

            sql_nav = sf.SQL.nav(ids_used)  # Get their navs
            d = pd.read_sql(sf.SQL.nav(ids_used), conn)

            tic("Preproessing")
            d["statistic_date"] = d["statistic_date"].apply(su.date2tstp)

            tic("Grouping")
            idx4slice = su.idx4slice(d, slice_by="fund_id")  # Grouping the datas By fund_id
            navs = su.slice(d, idx4slice, "nav")
            t_reals = su.slice(d, idx4slice, "statistic_date")
            t_mins = t_min["t_min"].tolist()
            t_mins_tstp = [su.date2tstp(x) for x in t_mins]

            print("length of Data: {0}".format(len(d)))
            conn.close()
            #
            t_stds = [tu.timeseries_std(date_s.today, interval, 52, extend=1) for interval in intervals]  # 标准序列
            t_std_y5 = t_stds[6]
            t_stds_len = [len(x) - 1 for x in t_stds]  # 标准序列净值样本个数
            t_std_weeks = tu.timeseries_std(date_s.today, "w", 52, 1)  # 标准序列_本周
            t_std_alls = [tu.timeseries_std(date_s.today, tu.periods_in_interval(date_s.today, t_min, 12), extend=4) for
                          t_min in t_mins]  # 标准序列_成立以来
            t_std_alls = [t_std_all[:len([x for x in t_std_all if x >= t_min]) + 1] for t_std_all, t_min in
                          zip(t_std_alls, t_mins_tstp)]

            matchs = [tu.outer_match4indicator_w(t_real, t_std_all, False) for t_real, t_std_all in
                      zip(t_reals, t_std_alls)]  # 匹配
            idx_matchs = [x[1] for x in matchs]
            t_matchs = [[t_real[ix] if ix is not None else None for ix in idx.values()] for t_real, idx in
                        zip(t_reals, idx_matchs)]
            nav_matchs = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                          zip(navs, idx_matchs)]
            navs_len = dict(zip(range(len(ids_used)), [len(x) for x in nav_matchs]))  # 实际序列净值样本个数

            # 基准指数的标准序列_成立以来
            matchs_bm = [tu.outer_match4indicator_w(t_bm, t_std_all, False) for t_std_all in t_std_alls]
            idx_matchs_bm = [x[1] for x in matchs_bm]
            price_bm0_all = [[prices_bm[0][ix] if ix is not None else None for ix in idx.values()] for idx in
                             idx_matchs_bm]
            price_bm1_all = [[prices_bm[1][ix] if ix is not None else None for ix in idx.values()] for idx in
                             idx_matchs_bm]
            price_bm2_all = [[prices_bm[2][ix] if ix is not None else None for ix in idx.values()] for idx in
                             idx_matchs_bm]
            price_bm3_all = [[prices_bm[3][ix] if ix is not None else None for ix in idx.values()] for idx in
                             idx_matchs_bm]
            price_bm4_all = [[prices_bm[4][ix] if ix is not None else None for ix in idx.values()] for idx in
                             idx_matchs_bm]

            matchs_pe = [tu.outer_match4indicator_w(t_pe, t_std_all, False) for t_std_all in t_std_alls]
            idx_matchs_pe = [x[1] for x in matchs_pe]
            price_pe_all = [[price_pe[ix] if ix is not None else None for ix in idx.values()] for idx in idx_matchs_pe]

            # 基准指标的收益率_成立以来
            r_bm0_all = [fi.gen_return_series(x) for x in price_bm0_all]
            r_bm1_all = [fi.gen_return_series(x) for x in price_bm1_all]
            r_bm2_all = [fi.gen_return_series(x) for x in price_bm2_all]
            r_bm3_all = [fi.gen_return_series(x) for x in price_bm3_all]
            r_bm4_all = [fi.gen_return_series(x) for x in price_bm4_all]

            r_pe_all = [fi.gen_return_series(x) for x in price_pe_all]

            tmp = [len(idx_matchs_bm[i]) for i in range(len(idx_matchs_bm))]
            tmp_id = tmp.index(max(tmp))
            tmp_list = [r_tbond[ix] if ix is not None else None for ix in idx_matchs_bm[tmp_id].values()]
            tmp = pd.DataFrame(tmp_list)[0].fillna(method="backfill").tolist()
            r_f_all = [[r_tbond[idx[k]] if idx[k] is not None else tmp[k] for k in idx.keys()] for idx in idx_matchs_bm]
            r_f_all = [x[1:] for x in r_f_all]

            # 基准指标的收益率_不同频率
            matchs_bm = tu.outer_match4indicator_w(t_bm, t_std_y5, False)  # 基准指数标准序列_成立以来
            matchs_pe = tu.outer_match4indicator_w(t_pe, t_std_y5, False)
            idx_matchs_bm = matchs_bm[1]
            idx_matchs_pe = matchs_pe[1]
            price_bm0_y5 = [prices_bm[0][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
            price_bm1_y5 = [prices_bm[1][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
            price_bm2_y5 = [prices_bm[2][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
            price_bm3_y5 = [prices_bm[3][ix] if ix is not None else None for ix in idx_matchs_bm.values()]
            price_bm4_y5 = [prices_bm[4][ix] if ix is not None else None for ix in idx_matchs_bm.values()]

            price_pe_y5 = [price_pe[ix] if ix is not None else None for ix in idx_matchs_pe.values()]

            # 基准指标的收益率_不同频率
            r_bm0_y5 = fi.gen_return_series(price_bm0_y5)
            r_bm1_y5 = fi.gen_return_series(price_bm1_y5)
            r_bm2_y5 = fi.gen_return_series(price_bm2_y5)
            r_bm3_y5 = fi.gen_return_series(price_bm3_y5)
            r_bm4_y5 = fi.gen_return_series(price_bm4_y5)

            r_pe_y5 = fi.gen_return_series(price_pe_y5)

            r_f_y5 = [r_tbond[ix] if ix is not None else None for ix in idx_matchs_bm.values()]
            r_f_y5 = r_f_y5[1:]

            rs_bm0 = [r_bm0_y5[:length - 1] for length in t_stds_len]
            rs_bm1 = [r_bm1_y5[:length - 1] for length in t_stds_len]
            rs_bm2 = [r_bm2_y5[:length - 1] for length in t_stds_len]
            rs_bm3 = [r_bm3_y5[:length - 1] for length in t_stds_len]
            rs_bm4 = [r_bm4_y5[:length - 1] for length in t_stds_len]

            rs_pe = [r_pe_y5[:length - 1] for length in t_stds_len]
            rs_f = [r_f_y5[:length - 1] for length in t_stds_len]

            benchmark = {1: rs_bm0, 2: rs_bm1, 3: rs_bm2, 4: rs_pe, 6: rs_bm3, 7: rs_bm4}
            benchmark_all = {1: r_bm0_all, 2: r_bm1_all, 3: r_bm2_all, 4: r_pe_all, 6: r_bm3_all, 7: r_bm4_all}

            tic("Calculating...\n")
            for i in range(len(ids_used)):
                navs = []
                ts = []
                for j in range(7):
                    if t_mins[i] + relativedelta(months=intervals[j]) < date_s.today:
                        length = min(navs_len[i], t_stds_len[j])
                        navs.append(nav_matchs[i][:length])
                        ts.append(t_matchs[i][:length])
                    else:
                        navs.append(None)
                        ts.append(None)
                for j in range(7, 11):
                    length = min(navs_len[i], t_stds_len[j])
                    navs.append(nav_matchs[i][:length])
                    ts.append(t_matchs[i][:length])

                rs_f_ = rs_f
                rs_f_.append(r_f_all[i])
                rs_f1_ = rs_f_
                rs_f3_ = [rs_f1_[i] for i in intervals3]

                navs1 = navs
                navs1.append(nav_matchs[i])
                ts.append(t_matchs[i])
                navs3 = [navs1[i] for i in intervals3]
                ts3 = [ts[i] for i in intervals3]
                rs3 = [fi.gen_return_series(x) for x in navs3]

                con_rise_weeks = [fi.periods_continuous_rise(r)[0] for r in rs3]
                con_fall_weeks = [fi.periods_continuous_fall(r)[0] for r in rs3]
                p_earning_weeks = [fi.periods_positive_return(r) for r in rs3]
                n_earning_weeks = [fi.periods_npositive_return(r) for r in rs3]

                for k in benchmark.keys():
                    rs_bm = benchmark[k]  # 指定benchmark
                    rs_bm1 = rs_bm
                    rs_bm1.append(benchmark_all[k][i])
                    rs_bm3 = [rs_bm1[i] for i in intervals3]

                    s_time = [fi.competency_timing(r, r_bm, r_f) for r, r_bm, r_f in zip(rs3, rs_bm3, rs_f3_)]
                    s_security = [fi.competency_stock(r, r_bm, r_f) for r, r_bm, r_f in zip(rs3, rs_bm3, rs_f3_)]
                    persistence = [fi.persistence_er(r, r_bm) for r, r_bm in zip(rs3, rs_bm3)]
                    odds = [fi.odds(r, r_bm) for r, r_bm in zip(rs3, rs_bm3)]
                    unsys_risk = [fi.unsystematic_risk(r, r_bm, r_f) for r, r_bm, r_f in zip(rs3, rs_bm3, rs_f3_)]
                    tracking_error_a = [fi.tracking_error_a(r, r_bm) for r, r_bm in zip(rs3, rs_bm3)]
                    rvalue = [fi.value_at_risk(r) for r in rs3]
                    min_return = [fi.min_return(r)[0] for r in rs3]
                    max_return = [fi.max_return(r)[0] for r in rs3]
                    mdd_ = [fi.max_drawdown(nv, t)[1] for nv, t in zip(navs3, ts3)]
                    mdd_repair_time = [x[1] if x is not None else None for x in mdd_]
                    mdd_time = [x[0] if x is not None else None for x in mdd_]
                    skewness = [fi.skewness(r) for r in rs3]
                    kurtosis = [fi.kurtosis(r) for r in rs3]
                    ERVaR = [fi.ERVaR(r, r_f) for r, r_f in zip(rs3, rs_f3_)]

                    tmp = [con_rise_weeks, con_fall_weeks, s_time, s_security, persistence,
                           odds, unsys_risk, tracking_error_a, rvalue, p_earning_weeks, n_earning_weeks,
                           min_return, max_return, mdd_repair_time, mdd_time, skewness, kurtosis, ERVaR]
                    result_i = [ids_used[i], date_s.today, k]
                    for x in tmp:
                        result_i.extend(x)
                    result.append(result_i)

        df = pd.DataFrame(result)
        df[list(range(3, 165))] = df[list(range(3, 165))].astype(np.float64)
        df[list(range(3, 165))] = df[list(range(3, 165))].apply(lambda x: round(x, 6))
        df.columns = columns

        df_whole = df_whole.append(df)
        df_whole = df_whole.sort_values(by=["fund_id", "statistic_date", "benchmark"], ascending=[True, False, True])
        df_whole = df_whole.drop_duplicates(subset=["fund_id", "benchmark"])
        df_whole.index = range(len(df_whole))

io.to_sql(table.name, engine_wt, df)
