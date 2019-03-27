import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundAssetScale
from utils.database.models.base_public import IdMatch, FundInfo, FundAssetScale

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["incremental"]

_entities_map = [
    (FundInfo.fund_id, FundAssetScale.fund_id), (FundInfo.fund_name, FundAssetScale.fund_name), (DFundAssetScale.data_source, FundAssetScale.data_source),
    (DFundAssetScale.statistic_date, FundAssetScale.statistic_date), (DFundAssetScale.purchase_amount, FundAssetScale.purchase_amount),
    (DFundAssetScale.redemption_amounnt, FundAssetScale.redemption_amount), (DFundAssetScale.total_asset, FundAssetScale.total_asset),
    (DFundAssetScale.total_share, FundAssetScale.total_share)
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_asset_scale(update_time):
    """
    Fetch records of DFundAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oas = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundAssetScale, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundAssetScale.fund_id, IdMatch.data_source == DFundAssetScale.data_source)
    ).filter(
        and_(DFundAssetScale.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundAssetScale.fund_id.name, FundAssetScale.statistic_date.name]]
    return df


def transform():
    df = fetch_multisource_fund_asset_scale(UPDATE_TIME)

    df_020001 = df.ix[df[FundAssetScale.data_source.name] == "020001"]
    df_020001[FundAssetScale.total_asset.name] = df_020001[FundAssetScale.total_asset.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x))
    df_020001[FundAssetScale.total_share.name] = df_020001[FundAssetScale.total_share.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))

    df_020002 = df.ix[df[FundAssetScale.data_source.name] == "020002"]
    df_020002[FundAssetScale.total_asset.name] = df_020002[FundAssetScale.total_asset.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x))
    df_020002[FundAssetScale.total_share.name] = df_020002[FundAssetScale.total_share.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "亿"))

    result = df_020002.join(
        df_020001, how="outer", rsuffix="_020001"
    )[[x.name for x in _output_entities]].fillna(df_020001)
    return result


def main():
    io.to_sql(FundAssetScale.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
