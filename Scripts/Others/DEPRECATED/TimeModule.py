
import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np
import time as t

# Standard Time Series

def timeseries_std(date_start, interval, periods_y=52, extend=0, **kwargs):
    '''
    Args:
        extend: The extra period number needed besides periods in interval.
    '''
    tmtp_s, tmstp_s = time_tuple_stamp(date_start)
    date_end = date_infimum(interval, date_start)
    periods_num = periods_in_interval(date_start, date_end, periods_y)

    if periods_y == 52:
        return [tmstp_s - i * 604800 for i in range(periods_num + 1 + extend)]   #604800 is the seconds of a week, because timestamp use second as unit

    elif periods_y == 12:
        dates_Ym = [(tmtp_s.tm_year - (i + 12-tmtp_s.tm_mon)//12, 12 - (i + 12 - tmtp_s.tm_mon)%12) for i in range(periods_num + 1 + extend)]
        month_range = [cld.monthrange(tmtp_s.tm_year - (i + 12-tmtp_s.tm_mon)//12, 12 - (i + 12 - tmtp_s.tm_mon)%12)[1] for i in range(periods_num + 1 + extend)]
        
        if "use_lastday" not in kwargs.keys():
            return [dt.datetime(Y, m, min(d, tmtp_s.tm_mday), tmtp_s.tm_hour, tmtp_s.tm_min, tmtp_s.tm_sec).timestamp() for (Y, m), d in zip(dates_Ym, month_range)]
        else:
            if kwargs["use_lastday"]:
                return [dt.datetime(Y, m, d, 23, 59, 59, 999999).timestamp() for (Y, m), d in zip(dates_Ym, month_range)]
                #return [dt.datetime(Y, m, d).timestamp() for (Y, m), d in zip(dates_Ym, month_range)]
            else:
                return [dt.datetime(Y, m, min(d, tmtp_s.tm_mday), tmtp_s.tm_hour, tmtp_s.tm_min, tmtp_s.tm_sec).timestamp() for (Y, m), d in zip(dates_Ym, month_range)]


# Time Matching

def match_timeseries(ts_realistic, ts_std, method="forward", drop_none=True):

    pass

# Support Function

def time_tuple_stamp(time):
    '''
    Check the type of `time`, and return a timetuple(time.struct_time), timestamp(float).
    Notice that if the `time` is an instancce of int or float, then the timestamp will be
    equivalent to the `time`.
    '''

    if isinstance(time, dt.datetime):
        return time.timetuple(), time.timestamp()

    elif isinstance(time, dt.date):
        ttp = time.timetuple()
        return ttp, t.mktime(ttp)

    elif isinstance(time, (float, int)):
        ttp = dt.datetime.fromtimestamp(time).timetuple()
        return ttp, time

def periods_in_interval(date_start, date_end, periods_y=52):
    '''
    The return periods included in the given interval.
    '''

    tmtp_s, tmstp_s = time_tuple_stamp(date_start)
    tmtp_e, tmstp_e = time_tuple_stamp(date_end)

    if periods_y == 52:
        delta_day = (tmstp_s - tmstp_e) / 86400
        n_integer = delta_day // 7
        n_decimal = delta_day % 7
        delta_weeks = int(n_integer + int(bool(n_decimal)))
        return delta_weeks
    
    elif periods_y == 12:
        delta_month = (tmtp_s.tm_year*12 + tmtp_s.tm_mon) - (tmtp_e.tm_year*12 + tmtp_e.tm_mon)
        return delta_month

def date_infimum(interval, qDate=dt.datetime.today()):    #UNDER DEVELOPING
    qDate = dt.datetime.fromtimestamp(t.mktime(qDate.timetuple()))
    if type(interval) is not str:
        totalMonth = qDate.year*12 + qDate.month - interval
        y = totalMonth // 12
        m = totalMonth % 12
        if m == 0: 
            m+=12
            y-=1
        if cld.monthrange(y,m)[1] < qDate.day:
            date_inf = dt.datetime(y, m, cld.monthrange(y,m)[1],qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
        else:
            date_inf = dt.datetime(y, m, qDate.day, qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
    else:
        if interval == "d":
            date_inf = dt.datetime(qDate.year, qDate.month, qDate.day) - dt.timedelta(0, 0, 1)
        elif interval == "w":
            date_inf = qDate - dt.timedelta(qDate.weekday()+2) - dt.timedelta(0, 0, 1)
            pass
        elif interval == "m":
            date_inf = dt.datetime(qDate.year, qDate.month, 1) - dt.timedelta(0, 0, 1)
        elif interval == "s":
            m = qDate.month
            y = qDate.year
            s1 = [1, 2, 3]
            s2 = [4, 5, 6]
            s3 = [7, 8, 9]
            s4 = [10, 11, 12]
            if qDate.month in s1:
                date_inf = dt.datetime(qDate.year, s1[0], 1) - dt.timedelta(0, 0, 1)
            elif qDate.month in s2:
                date_inf = dt.datetime(qDate.year, s2[0], 1) - dt.timedelta(0, 0, 1)
            elif qDate.month in s3:
                date_inf = dt.datetime(qDate.year, s3[0], 1) - dt.timedelta(0, 0, 1)
            elif qDate.month in s4:
                #if y == 12: date_inf = dt.datetime(qDate.year, s4[0], 1) - relativedelta(microseconds=1)
                #else: date_inf = dt.datetime(qDate.year-1, s4[0], 1) - relativedelta(microseconds=1)
                date_inf = dt.datetime(qDate.year, s4[0], 1) - dt.timedelta(0, 0, 1)
        elif interval == "y":
            date_inf = dt.datetime(qDate.year, 1, 1) - dt.timedelta(0, 0, 1)
    return date_inf 































# To be DEPRECATED in the future
def genStdDateSeq(interval, period_num, qDate=dt.datetime.today(),extend=0):
    dateSeq = []
    if type(interval) != str:
        date_inf = date_infimum(interval, qDate)
        if period_num == 250:   #frequency = day
            for i in range(interval * 31 +1):
                date_next = qDate - dt.timedelta(i)
                if date_next > date_inf and date_next.isoweekday() != 6 and date_next.isoweekday() != 7:
                    dateSeq.append(date_next)
                elif date_next > date_inf and (date_next.isoweekday == 6 or date_next.isoweekday == 7):
                    continue
                elif date_next <= date_inf and date_next.isoweekday() != 6 and date_next.isoweekday() != 7:
                    dateSeq.append(date_next)
                    break
                elif date_next <= date_inf and (date_next.isoweekday() == 6 or date_next.isoweekday() == 7):
                    dateSeq.append(date_next-dt.timedelta(2))
                    break
        elif period_num == 52:    #frequency = week
            for i in range(int(interval*31/7+1)+1):   #the max num of period in the interval
                date_next = qDate - dt.timedelta(i * 7)
                if date_next > date_inf:
                    dateSeq.append(date_next)
                else: 
                    dateSeq.append(date_next)
                    break
        elif period_num == 12:  #frequency = month
            for i in range(interval + 1):   #UNSTABLE
                if (qDate.month - i%12) > 0:
                    date_next = dt.datetime(qDate.year - i//12, qDate.month - i%12, checkDay(qDate.year - i//12, qDate.month - i%12, qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
                else:
                    date_next = dt.datetime(qDate.year - i//12 - 1, 12 - (i%12 - qDate.month), checkDay(qDate.year - i//12 - 1, 12 - (i%12 - qDate.month), qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
                if date_next > date_inf:
                    dateSeq.append(date_next)
                else:
                    dateSeq.append(date_next)
                    break
        elif period_num == 1:   #frequency = year
            for i in range(int(interval//12)+1):
                date_next = dt.datetime(qDate.year - i,qDate.month, checkDay(qDate.year - i,qDate.month,qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
                if date_next > date_inf:
                    dateSeq.append(date_next)
                else:
                    dateSeq.append(date_next)
                    break
    elif type(interval) == str:
        date_inf = date_infimum(interval, qDate)
        if period_num == 250:   #frequency = day
            for i in range(12 * 31 +1):
                date_next = qDate - dt.timedelta(i)
                if date_next > date_inf and date_next.isoweekday() != 6 and date_next.isoweekday() != 7:
                    dateSeq.append(date_next)
                elif date_next > date_inf and (date_next.isoweekday == 6 or date_next.isoweekday == 7):
                    continue
                elif date_next <= date_inf and date_next.isoweekday() != 6 and date_next.isoweekday() != 7:
                    dateSeq.append(date_next)
                    break
                elif date_next <= date_inf and (date_next.isoweekday() == 6 or date_next.isoweekday() == 7):
                    dateSeq.append(date_next-dt.timedelta(2))
                    break
        elif period_num == 52:    #frequency = week
            for i in range(int(12*31/7+1)+1):   #the max num of period in the interval
                date_next = qDate - dt.timedelta(i * 7)
                if date_next > date_inf:
                    dateSeq.append(date_next)
                else: 
                    dateSeq.append(date_next)
                    break
        elif period_num == 12:  #frequency = month
            for i in range(12 + 1):   #UNSTABLE
                if (qDate.month - i%12) > 0:
                    date_next = dt.datetime(qDate.year - i//12, qDate.month - i%12, checkDay(qDate.year - i//12, qDate.month - i%12, qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
                else:
                    date_next = dt.datetime(qDate.year - i//12 - 1, 12 - (i%12 - qDate.month), checkDay(qDate.year - i//12 - 1, 12 - (i%12 - qDate.month), qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
                if date_next > date_inf:
                    dateSeq.append(date_next)
                else:
                    dateSeq.append(date_next)
                    break
        elif period_num == 1:   #frequency = year
            for i in range(int(12//12)+1):
                date_next = dt.datetime(qDate.year - i,qDate.month, checkDay(qDate.year-i, qDate.month, qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
                if date_next > date_inf:
                    dateSeq.append(date_next)
                else:
                    dateSeq.append(date_next)
                    break
    return dateSeq

def genStdDateSeq2(interval, period_num, qDate=dt.datetime.today(),extend=0):
    dateSeq = []
    if type(interval) is str: interval = 12
    date_inf = date_infimum(interval, qDate)
    if period_num == 250:   #frequency = day
        for i in range(12 * 31 +1):
            date_next = qDate - dt.timedelta(i)
            if date_next > date_inf and date_next.isoweekday() != 6 and date_next.isoweekday() != 7:
                dateSeq.append(date_next)
            elif date_next > date_inf and (date_next.isoweekday == 6 or date_next.isoweekday == 7):
                continue
            elif date_next <= date_inf and date_next.isoweekday() != 6 and date_next.isoweekday() != 7:
                dateSeq.append(date_next)
                break
            elif date_next <= date_inf and (date_next.isoweekday() == 6 or date_next.isoweekday() == 7):
                dateSeq.append(date_next-dt.timedelta(2))
                break
    elif period_num == 52:    #frequency = week
        for i in range(int(interval*31/7+1)+1):   #the max num of period in the interval
            date_next = qDate - dt.timedelta(i * 7)
            if date_next > date_inf:
                dateSeq.append(date_next)
            else: 
                dateSeq.append(date_next)
                break
    elif period_num == 12:  #frequency = month
        for i in range(interval + 1):   #UNSTABLE
            if (qDate.month - i%12) > 0:
                date_next = dt.datetime(qDate.year - i//12, qDate.month - i%12, checkDay(qDate.year - i//12, qDate.month - i%12, qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
            else:
                date_next = dt.datetime(qDate.year - i//12 - 1, 12 - (i%12 - qDate.month), checkDay(qDate.year - i//12 - 1, 12 - (i%12 - qDate.month), qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
            if date_next > date_inf:
                dateSeq.append(date_next)
            else:
                dateSeq.append(date_next)
                break
    elif period_num == 1:   #frequency = year
        for i in range(int(interval//12)+1):
            date_next = dt.datetime(qDate.year - i,qDate.month, checkDay(qDate.year-i, qDate.month, qDate.day), qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
            if date_next > date_inf:
                dateSeq.append(date_next)
            else:
                dateSeq.append(date_next)
                break
    return dateSeq

def checkDay(year, month, day):
    if cld.monthrange(year, month)[1] >= day:
        return  day
    else:
        return cld.monthrange(year, month)[1]

def match_timeseries_weekly(real_series, interval, start_date, end_date):
    real_series = list(real_series)
    real_series.sort(reverse=True)

    # generate standard time series from real time series
    #std_series = genStdDateSeq(((start_date.year * 12 + start_date.month) - (end_date.year * 12 + end_date.month)) + 1, 52, start_date)
    #std_series = [x for x in std_series if x >= real_series[-1]]
    std_series = genStdDateSeq(interval, 52, start_date)
    #std_series = [x for x in std_series if x >= real_series[-1]]
    std_series = [x for x in std_series if x >= max(end_date, real_series[-1])]

    res = set(std_series).intersection(set(real_series))
    drop = set(real_series) - res
    drop = list(drop)
    drop.sort(reverse=True)
    missing = set(std_series) - res
    missing = list(missing)
    missing.sort(reverse=True)

    res = list(res)
    res.sort(reverse=True)

    index = [std_series.index(dt) for dt in missing]

    for i in range(len(index)):
        for j in range(len(drop)):
            delta = (std_series[index[i]] - drop[j]).days
            if delta < 7 and delta >= 0:
                res.insert(index[i], drop[j])
                break
            if j == len(drop) - 1:
                res.insert(index[i], None)

    return res

def match_timeseries_monthly(real_series, interval, start_date, end_date):
    real_series = list(real_series)
    real_series.sort(reverse=True)

    last_day = dt.datetime(start_date.year, start_date.month, cld.monthrange(start_date.year, start_date.month)[1])
    std_series = genStdDateSeq(interval, 12, last_day)
    std_series = [x for x in std_series if x >= max(end_date, real_series[-1])] #testing

    res = set(std_series).intersection(set(real_series))
    drop = set(real_series) - res
    drop = list(drop)
    drop.sort(reverse=True)
    missing = set(std_series) - res
    missing = list(missing)
    missing.sort(reverse=True)

    res = list(res)
    res.sort(reverse=True)

    index = [std_series.index(dt) for dt in missing]

    for i in range(len(index)):
        for j in range(len(drop)):
            delta = (std_series[index[i]] - drop[j]).days
            if delta < last_day.day and delta >= 0:
                res.insert(index[i], drop[j])
                break
            if j == len(drop) - 1:
                res.insert(index[i], None)
    return res

def reshape_data(time_series, data, col_date, col_trans):
    """
    根据匹配的时间序列, 给定列名称, 将Dataframe格式的原数据塑形成dict格式的数据.

    *Args:
        time_series(list<datetime>): 匹配过后的时间序列(日期由近及远);
        get_data(pandas.Dataframe): 带有日期序列的原数据;
        col_date(str): 原数据中表示时间序列的列名;
        col_trans(list<string>): 需要保留的列名;

    *Returns:
        匹配, 重塑后的dict格式数据.
    """
    res = {}
    data.index = data[col_date]
    data = data.to_dict(orient="index")

    for k in col_trans:
        res_k = []
        for t in time_series:
            try:
                res_k.append(data[t][k])
            except:
                res_k.append(None)
        res[k] = res_k
    return res

def match_timeseries_old(ts_realistic, interval, period_num=52, qDate=dt.datetime.now(), method="forward"):
    date_inf = date_infimum(interval, qDate)
    
    ts_std = genStdDateSeq(interval, period_num, qDate)
    ts_std = [x for x in ts_std if x >= max(date_inf, ts_realistic[-1])]

    temp = [[x for x in ts_realistic if x <= ts_std[i] and x > ts_std[i+1]] for i in range(len(ts_std)-1)]
    temp.append([x for x in ts_realistic if x < ts_std[-1]])
    temp = [x[0] if x else None for x in temp]

    return temp

def genInfDate(interval, qDate=dt.datetime.today()):    #UNDER DEVELOPING
    qDate = dt.datetime.fromtimestamp(t.mktime(qDate.timetuple()))
    if type(interval) is not str:
        totalMonth = qDate.year*12 + qDate.month - interval
        y = totalMonth // 12
        m = totalMonth % 12
        if m == 0: 
            m+=12
            y-=16
        if cld.monthrange(y,m)[1] < qDate.day:
            date_inf = dt.datetime(y, m, cld.monthrange(y,m)[1],qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
        else:
            date_inf = dt.datetime(y, m, qDate.day, qDate.hour, qDate.minute, qDate.second, qDate.microsecond)
    else:
        if interval == "d":
            date_inf = dt.datetime(qDate.year, qDate.month, qDate.day) - dt.timedelta(0, 0, 1)
        elif interval == "w":
            date_inf = qDate - dt.timedelta(qDate.weekday()+2) - dt.timedelta(0, 0, 1)
            pass
        elif interval == "m":
            date_inf = dt.datetime(qDate.year, qDate.month, 1) - dt.timedelta(0, 0, 1)
        elif interval == "s":
            m = qDate.month
            y = qDate.year
            s1 = [1, 2, 3]
            s2 = [4, 5, 6]
            s3 = [7, 8, 9]
            s4 = [10, 11, 12]
            if qDate.month in s1:
                date_inf = dt.datetime(qDate.year, s1[0], 1) - dt.timedelta(0, 0, 1)
            elif qDate.month in s2:
                date_inf = dt.datetime(qDate.year, s2[0], 1) - dt.timedelta(0, 0, 1)
            elif qDate.month in s3:
                date_inf = dt.datetime(qDate.year, s3[0], 1) - dt.timedelta(0, 0, 1)
            elif qDate.month in s4:
                #if y == 12: date_inf = dt.datetime(qDate.year, s4[0], 1) - relativedelta(microseconds=1)
                #else: date_inf = dt.datetime(qDate.year-1, s4[0], 1) - relativedelta(microseconds=1)
                date_inf = dt.datetime(qDate.year, s4[0], 1) - dt.timedelta(0, 0, 1)
        elif interval == "y":
            date_inf = dt.datetime(qDate.year, 1, 1) - dt.timedelta(0, 0, 1)
    return date_inf 

def tr(seq):
    return [dt.datetime.fromtimestamp(x) if x is not None else None for x in seq] #Only for debugging

def outer_match4index_f7(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std)):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):
            #print([dt.datetime.fromtimestamp(x) for x in [ts_std[i], ts_real[j]]])

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] > ts_std[i] - 604800 and ts_real[j] <= ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j
                    
                    is_missing_i = False
                    #print("matched: ", i, j)
                    break

                elif ts_real[j] <= ts_std[i] - 604800:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    #print("not matched: ", i, j)
                    break

                else:
                    current_position = j
                    #print("still matching: ", i, j)
                    continue

        if i == len(ts_std)-1:
            if drop_none:
                break
            else:
                num_std = len(ts_std)
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None]* num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched

def outer_match4index_b7_2(ts_real, ts_std, drop_none=True):
    length_std = len(ts_std)
    tmp = [[x for x in ts_real if x <= ts_std[i] + 604800 and x > ts_std[i]] for i in range(length_std)]
    ts_matched = [min(x) if len(x) > 0 else None for x in tmp]
    idx_matched = [ts_real.index(x) if x is not None else None for x in ts_matched]
    idx_matched = dict(zip(range(length_std), idx_matched))
    return ts_matched, idx_matched

def outer_match4index_b7(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std)):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):
            #print([dt.datetime.fromtimestamp(x) for x in [ts_std[i], ts_real[j]]])

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] <= ts_std[i] + 604800 and ts_real[j] > ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j
                    
                    is_missing_i = False
                    #print("matched: ", i, j)
                    break

                elif ts_real[j] <= ts_std[i]:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    #print("not matched: ", i, j)
                    break

                else:
                    current_position = j
                    #print("still matching: ", i, j)
                    continue

        if i == len(ts_std)-1:
            if drop_none:
                break
            else:
                num_std = len(ts_std)
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None]* num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched

def outer_match4index_m(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std)-1):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):
            #print([dt.datetime.fromtimestamp(x) for x in [ts_std[i], ts_real[j]]])

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] > ts_std[i+1] and ts_real[j] <= ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j
                    
                    is_missing_i = False
                    #print("matched: ", i, j)
                    break

                elif ts_real[j] <= ts_std[i+1]:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    #print("not matched: ", i, j)
                    break

                else:
                    current_position = j
                    #print("still matching: ", i, j)
                    continue

        if i == len(ts_std)-2:
            if drop_none:
                break
            else:
                num_std = len(ts_std) - 1
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None]* num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched

def outer_match4index_w(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std)-1):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):
            #print([dt.datetime.fromtimestamp(x) for x in [ts_std[i], ts_real[j]]])

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] >= ts_std[i+1] and ts_real[j] < ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j
                    
                    is_missing_i = False
                    #print("matched: ", i, j)
                    break

                elif ts_real[j] < ts_std[i+1]:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    #print("not matched: ", i, j)
                    break

                else:
                    current_position = j
                    #print("still matching: ", i, j)
                    continue

        if i == len(ts_std)-2:
            if drop_none:
                break
            else:
                num_std = len(ts_std) -1
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None]* num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched
