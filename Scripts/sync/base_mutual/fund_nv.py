import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.config import ConfigSource
from utils.database.models.base_public import FundInfo, FundNvSource, FundNv

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]


_entities_map = [
    (FundInfo.fund_id, FundNv.fund_id), (FundInfo.fund_name, FundNv.fund_name), (FundNvSource.data_source, FundNv.data_source),
    (FundNvSource.statistic_date, FundNv.statistic_date), (FundNvSource.nav, FundNv.nav), (FundNvSource.added_nav, FundNv.added_nav),
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_nv(update_time_l, update_time_r=None):
    """
    Fetch records of DOrgInfo table where record update time >= `update_time`
    Args:
        update_time_l: record update time

    Returns:
        pandas.DataFrame
    """
    if update_time_r is None:
        condition = and_(FundNvSource.update_time >= update_time_l)
    else:
        condition = and_(FundNvSource.update_time >= update_time_l, FundNvSource.update_time <= update_time_r)

    query_fnv = _session.query(FundNvSource).join(
        FundInfo, and_(FundNvSource.fund_id == FundInfo.fund_id)
    ).filter(
        condition
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fnv.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundInfo.fund_id.name, FundNvSource.statistic_date.name]]
    return df


def transform():
    # fids = pd.read_sql("SELECT DISTINCT fund_id FROM fund_nv_source", _engine_wt)["fund_id"].tolist()
    # for i in range(0, len(fids), 50):
    #     fid = str(tuple(fids[i: i+50]))
    #     print(i)
    #     df = pd.read_sql("SELECT fund_id, fund_name, data_source, statistic_date, nav, added_nav, swanav FROM fund_nv_source WHERE fund_id IN {fid}".format(fid=fid), _engine_wt)
    #     df.index = df[["fund_id", "statistic_date"]]
    #     df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    #     df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    #     df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]
    #     result = df_020002.join(
    #         df_020001, how="outer", rsuffix="_020001"
    #     ).join(
    #         df_020003, how="outer", rsuffix="_020003"
    #     )[df_020002.columns].fillna(df_020001).fillna(df_020003)
    #
    #     io.to_sql("fund_nv", _engine_wt, result)
    # import datetime as dt
    # l, r = dt.datetime(2017, 9, 1), dt.datetime(2017, 9, 30)
    # df = fetch_multisource_nv(l, r)
    df = fetch_multisource_nv(UPDATE_TIME)

    df_020001 = df.ix[df[FundInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundInfo.data_source.name] == "020003"]

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[df_020002.columns].fillna(df_020001).fillna(df_020003).dropna(subset=["fund_id"]).dropna(subset=["statistic_date"])

    return result


def main():
    io.to_sql(FundNv.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()
