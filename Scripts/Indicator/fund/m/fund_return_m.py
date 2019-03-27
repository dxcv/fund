import calendar as cld, datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np, pandas as pd
from utils.database import sqlfactory as du, config as cfg, io
from utils.script import scriptutils as su
from utils.algorithm import fund_indicator as fi, timeutils as tu
from utils.script.scriptutils import tic

table = du.Table("m", "return")  ###w->m
columns = ["fund_id", "statistic_date", "benchmark"]
columns.extend(table.columns())

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]

df_whole = pd.DataFrame()

for year in range(2017, 2018):
    for month in range(3, 4):
        conn = engine_rd.connect()

        now = dt.datetime.now()

        year, month = year, month
        month_range = cld.monthrange(year, month)[1]
        time_to_fill = du.Time(dt.datetime(year, month, month_range))
        year, month = time_to_fill.year, time_to_fill.month
        month_range = time_to_fill.month_range

        sql_bm = du.SQL.market_index(time_to_fill.today)  # Get benchmark prices
        sql_pe = du.SQL.pe_index(time_to_fill.today, freq="m")  ###w->m

        bm = pd.read_sql(sql_bm, engine_rd)
        bm["y1_treasury_rate"] = bm["y1_treasury_rate"].fillna(method="backfill")
        bm["y1_treasury_rate"] = bm["y1_treasury_rate"].apply(su.annually2monthly)
        bm["statistic_date"] = bm["statistic_date"].apply(su.date2tstp)
        pe = pd.read_sql(sql_pe, engine_rd)
        pe["statistic_date"] = pe["statistic_date"].apply(su.date2tstp)
        conn.close()

        prices_bm = [bm["hs300"].tolist(), bm["csi500"].tolist(), bm["sse50"].tolist(), bm["cbi"].tolist(), bm["nfi"]]
        price_pe = pe["index_value"].tolist()
        r_tbond = bm["y1_treasury_rate"].tolist()
        t_bm = bm["statistic_date"].tolist()
        t_pe = pe["statistic_date"].tolist()

        intervals = table.intervals
        # intervals1 = [1, 3, 6, 12, 24, 36, 60, "w", "m", "s", "y", "a"]
        intervals1 = [1, 2, 3, 4, 5, 6, 9, 10, 11]
        intervals2 = [1, 2, 3, 4, 5, 6, 10, 11]

        result = []
        for mday in range(3, 4):
            print("Day {0}: {1}".format(mday, dt.datetime.now()))
            conn = engine_rd.connect()

            date_s = du.Time(dt.datetime(year, month, mday) - dt.timedelta(mday))  # Generate statistic_date

            sql_ids_updated = du.SQL.ids_updated_sd(date_s.today, freq="m")
            ids_used = [x[0] for x in engine_rd.execute(sql_ids_updated).fetchall()]

            print("length of ids: {0}".format(len(ids_used)))

            if len(ids_used) == 0: continue  # Check whether there are updated get_data this day

            tic("Getting Data")
            sql_fdate = du.SQL.foundation_date(ids_used)  # Get their foundation date
            fdate = pd.read_sql(sql_fdate, engine_rd)
            ids_fdate = fdate["fund_id"].tolist()
            t_min = fdate

            if len(ids_fdate) != len(
                    ids_used):  # Check whether all fund has foundation date get_data. If not, use first nv get_data instead.
                ids_diff = list(set(ids_used) - set(ids_fdate))
                sql_fnvdate = du.SQL.firstnv_date(ids_diff)
                fnvdate = pd.read_sql(sql_fnvdate, engine_rd)
                ids_diff = fnvdate["fund_id"].tolist()
                t_min = pd.merge(fdate, fnvdate, "outer")
                print("Some foundation_date missed, use first nv date...", len(ids_diff), len(ids_fdate), len(t_min))
                t_min = t_min.sort_values("fund_id", ascending=True)  # Sort the df by fund id ASC

            sql_nav = du.SQL.nav(ids_used)  # Get their navs
            d = pd.read_sql(du.SQL.nav(ids_used), engine_rd)

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
            t_stds = [tu.timeseries_std(date_s.today, interval, periods_y=12, use_lastday=True, extend=1) for interval
                      in intervals]  # 标准序列   ###w->m

            t_std_y5 = t_stds[6]
            t_stds_len = [len(x) - 1 for x in t_stds]  # 标准序列净值样本个数
            t_std_alls = [tu.timeseries_std(date_s.today, tu.periods_in_interval(date_s.today, t_min, 12), periods_y=12,
                                            use_lastday=True, extend=6) for t_min in t_mins]  # 标准序列_成立以来
            t_std_alls = [t_std_all[:len([x for x in t_std_all if x >= t_min]) + 1] for t_std_all, t_min in
                          zip(t_std_alls, t_mins_tstp)]

            # 基金标准序列_成立以来
            matchs_all = [tu.outer_match4indicator_m(t_real, t_std_all, False) for t_real, t_std_all in
                          zip(t_reals, t_std_alls)]  # 匹配 ###w->m
            idx_matchs_all = [x[1] for x in matchs_all]
            nav_matchs_all = [[nav[ix] if ix is not None else None for ix in idx.values()] for nav, idx in
                              zip(navs, idx_matchs_all)]
            navs_all_len = dict(zip(range(len(ids_used)), [len(x) for x in nav_matchs_all]))  # 实际序列净值样本个数

            # 基准指数的标准序列_成立以来
            matchs_bm = [tu.outer_match4indicator_m(t_bm, t_std_all, False) for t_std_all in t_std_alls]
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

            matchs_pe = [tu.outer_match4indicator_m(t_pe, t_std_all, False) for t_std_all in t_std_alls]
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
            matchs_bm = tu.outer_match4indicator_m(t_bm, t_std_y5, False)  # 基准指数标准序列_成立以来
            matchs_pe = tu.outer_match4indicator_m(t_pe, t_std_y5, False)
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
                for j in range(7):
                    if t_mins[i] + relativedelta(months=intervals[j]) <= date_s.today:
                        length = min(navs_all_len[i], t_stds_len[j])
                        navs.append(nav_matchs_all[i][:length])
                    else:
                        navs.append(None)

                for j in range(7, 11):
                    length = min(navs_all_len[i], t_stds_len[j])
                    navs.append(nav_matchs_all[i][:length])

                rs_f_ = rs_f.copy()
                rs_f_.append(r_f_all[i])
                rs_f1_ = [rs_f_[i] for i in intervals1]
                rs_f2_ = [rs_f_[i] for i in intervals2]

                navs_ = navs.copy()
                navs_.append(nav_matchs_all[i])
                navs1 = [navs_[i] for i in intervals1]
                navs2 = [navs_[i] for i in intervals2]
                rs1 = [fi.gen_return_series(x) for x in navs1]
                rs2 = [fi.gen_return_series(x) for x in navs2]

                ir = [fi.accumulative_return(r) for r in navs1[:-1]]
                ir.append(fi.accumulative_return([x for x in navs1[-1] if x is not None]))  #
                ir_a = [fi.return_a(r, 12) for r in rs1]
                sharpe = [fi.sharpe_a(r, r_f, 12) for r, r_f in zip(rs2, rs_f2_)]
                calmar = [fi.calmar_a(nv, r_f, 12) for nv, r_f in zip(navs2, rs_f2_)]
                sortino = [fi.sortino_a(r, r_f, 12) for r, r_f in zip(rs2, rs_f2_)]

                for k in benchmark.keys():
                    rs_bm_ = benchmark[k].copy()  # 指定benchmark
                    rs_bm_.append(benchmark_all[k][i])
                    rs_bm1 = [rs_bm_[i] for i in intervals1]
                    rs_bm2 = [rs_bm_[i] for i in intervals2]

                    ier_a = [fi.excess_return_a(r, r_bm, 12) for r, r_bm in zip(rs1, rs_bm1)]
                    odds = [fi.odds(r, r_bm) for r, r_bm in zip(rs2, rs_bm2)]
                    info = [fi.info_a(r, r_bm, 12) for r, r_bm in zip(rs2, rs_bm2)]
                    jensen = [fi.jensen_a(r, r_bm, r_f, 12) for r, r_bm, r_f in zip(rs2, rs_bm2, rs_f2_)]
                    treynor = [fi.treynor_a(r, r_bm, r_f, 12) for r, r_bm, r_f in zip(rs2, rs_bm2, rs_f2_)]

                    tmp = [ir, ir_a, ier_a, odds, sharpe, calmar, sortino, info, jensen, treynor]
                    result_i = [ids_used[i], date_s.today, k]
                    for x in tmp:
                        result_i.extend(x)
                    result.append(result_i)

        df = pd.DataFrame(result)
        df[list(range(3, 86))] = df[list(range(3, 86))].astype(np.float64)
        df[list(range(3, 86))] = df[list(range(3, 86))].apply(lambda x: round(x, 6))
        df.columns = columns

        df_whole = df_whole.append(df)
        df_whole = df_whole.sort_values(by=["fund_id", "statistic_date", "benchmark"], ascending=[True, False, True])
        df_whole = df_whole.drop_duplicates(subset=["fund_id", "benchmark"])
        df.index = range(len(df))

io.to_sql(table.name, engine_wt, df, chunksize=2000)
