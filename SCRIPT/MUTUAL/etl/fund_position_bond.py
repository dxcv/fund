import datetime as dt
import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DFundPosition
from utils.database.models.base_public import IdMatch, FundInfo, FundPositionBond, FundAssetScale

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (FundInfo.fund_id, FundPositionBond.fund_id), (FundInfo.fund_name, FundPositionBond.fund_name), (DFundPosition.data_source, FundPositionBond.data_source),
    (DFundPosition.statistic_date, FundPositionBond.statistic_date), (DFundPosition.subject_id, FundPositionBond.subject_id),
    (DFundPosition.subject_name, FundPositionBond.subject_name), (DFundPosition.scale, FundPositionBond.scale),
    (DFundPosition.quantity, FundPositionBond.quantity), (FundAssetScale.total_asset, FundPositionBond.asset_scale),
    (DFundPosition.proportion, FundPositionBond.proportion_net)
]


_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]
_derivative_entities = []
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_multisource_fund_position_stock(update_time):
    """
    Fetch records of DOrgAssetScale table where record update time >= `update_time`
    Args:
        update_time: record update time

    Returns:
        pandas.DataFrame
    """

    query_oas = _session.query(IdMatch).join(
        FundInfo, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundInfo.fund_id)
    ).join(
        DFundPosition, and_(IdMatch.id_type == 1, IdMatch.source_id == DFundPosition.fund_id, IdMatch.data_source == DFundPosition.data_source, DFundPosition.type == "债券")
    ).join(
        FundAssetScale, and_(IdMatch.id_type == 1, IdMatch.matched_id == FundAssetScale.fund_id, DFundPosition.statistic_date == FundAssetScale.statistic_date)
    ).filter(
        and_(DFundPosition.update_time >= update_time, IdMatch.is_used == 1)
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_oas.all())
    df.columns = [x.name for x in _map_entities]
    df.index = df[[FundPositionBond.fund_id.name, FundPositionBond.statistic_date.name, FundPositionBond.subject_id.name]]
    return df


def transform():
    """

    Args:
        style:

    Returns:

    """
    # General process
    df = fetch_multisource_fund_position_stock(UPDATE_TIME)

    df[FundPositionBond.proportion_net.name] = df[FundPositionBond.proportion_net.name].fillna("").apply(lambda x: etl.StringParser.percentage(x))
    df[FundPositionBond.scale.name] = df[FundPositionBond.scale.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "万"))
    df[FundPositionBond.quantity.name] = df[FundPositionBond.quantity.name].fillna("").apply(lambda x: etl.StringParser.num_with_unit(x, "万"))
    df[FundPositionBond.scale.name] = df[FundPositionBond.scale.name].fillna(
        df[FundPositionBond.asset_scale.name].apply(lambda x: float(x) if x is not None else None) * df[FundPositionBond.proportion_net.name]
        * etl.Unit.tr("亿", "万")
    )

    # Process of different sources
    df_020001 = df.ix[df[FundPositionBond.data_source.name] == "020001"]
    df_020002 = df.ix[df[FundPositionBond.data_source.name] == "020002"]
    df_020003 = df.ix[df[FundPositionBond.data_source.name] == "020003"]

    merged = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    )[[x.name for x in _output_entities]].fillna(df_020002).fillna(df_020003)

    result = merged
    return result


def main():
    io.to_sql(FundPositionBond.__tablename__, _engine_wt, transform())


if __name__ == "__main__":
    main()
