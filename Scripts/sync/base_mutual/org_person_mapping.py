import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from utils.database import io, config as cfg
from utils.algorithm import etl
from utils.database.models.crawl_public import DOrgFund
from utils.database.models.base_public import IdMatch, PersonInfo, OrgInfo, OrgPersonMapping

_engine_wt = cfg.load_engine()["2Gbp"]
_db_session = sessionmaker(bind=_engine_wt)
_session = _db_session()
UPDATE_TIME = etl.update_time["all"]


_entities_map = [
    (DOrgFund.data_source, OrgPersonMapping.data_source),
    (DOrgFund.org_id, OrgPersonMapping.org_id), (DOrgFund.person_id, OrgPersonMapping.person_id),
]
_input_entities = [x[0] for x in _entities_map]
_map_entities = [x[1] for x in _entities_map]

_derivative_entities = [OrgPersonMapping.person_name, OrgPersonMapping.org_name]
_output_entities = [*_map_entities, *_derivative_entities]


def fetch_org_info():
    query = _session.query(IdMatch).filter(
        and_(IdMatch.id_type == 2, IdMatch.is_used == 1)
    ).join(
        OrgInfo, IdMatch.matched_id == OrgInfo.org_id
    ).with_entities(
        IdMatch.data_source, IdMatch.source_id, IdMatch.matched_id, OrgInfo.org_name
    )
    orgs = pd.DataFrame(query.all())
    org_id = dict(zip(
        [tuple(x) for x in orgs[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        orgs[IdMatch.matched_id.name]

    ))
    org_name = dict(zip(
        [tuple(x) for x in orgs[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        orgs[OrgInfo.org_name.name]

    ))
    return org_id, org_name


def fetch_person_info():
    query = _session.query(IdMatch).filter(
        IdMatch.id_type == 1
    ).join(
        PersonInfo, IdMatch.matched_id == PersonInfo.person_id
    ).with_entities(
        IdMatch.data_source, IdMatch.source_id, IdMatch.matched_id, PersonInfo.person_name
    )
    funds = pd.DataFrame(query.all())
    person_id = dict(zip(
        [tuple(x) for x in funds[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        funds[IdMatch.matched_id.name]

    ))
    person_name = dict(zip(
        [tuple(x) for x in funds[[IdMatch.data_source.name, IdMatch.source_id.name]].as_matrix()],
        funds[PersonInfo.person_name.name]

    ))
    return person_id, person_name


def fetch_multisource_fom(update_time):
    query_fom = _session.query(DOrgFund).filter(
        DOrgFund.update_time >= update_time
    ).with_entities(
        *_input_entities
    )
    df = pd.DataFrame(query_fom.all())
    df.columns = [x.name for x in _map_entities]
    return df


def transform():
    df = fetch_multisource_fom(UPDATE_TIME)
    fid, fname = fetch_person_info()
    oid, oname = fetch_org_info()

    df[OrgPersonMapping.person_name.name] = list(map(lambda x, y: fname.get((x, y)), df[OrgPersonMapping.data_source.name], df[OrgPersonMapping.person_id.name]))
    df[OrgPersonMapping.person_id.name] = list(map(lambda x, y: fid.get((x, y)), df[OrgPersonMapping.data_source.name], df[OrgPersonMapping.person_id.name]))

    df[OrgPersonMapping.org_name.name] = list(map(lambda x, y: oname.get((x, y)), df[OrgPersonMapping.data_source.name], df[OrgPersonMapping.org_id.name]))
    df[OrgPersonMapping.org_id.name] = list(map(lambda x, y: oid.get((x, y)), df[OrgPersonMapping.data_source.name], df[OrgPersonMapping.org_id.name]))
    df = df.dropna(subset=[OrgPersonMapping.person_id.name, OrgPersonMapping.org_id.name])
    df.index = df[[OrgPersonMapping.person_id.name, OrgPersonMapping.org_id.name]]

    df_020001 = df.ix[df[OrgInfo.data_source.name] == "020001"]
    df_020002 = df.ix[df[OrgInfo.data_source.name] == "020002"]
    df_020003 = df.ix[df[OrgInfo.data_source.name] == "020003"]

    result = df_020001.join(
        df_020002, how="outer", rsuffix="_020002"
    ).join(
        df_020003, how="outer", rsuffix="_020003"
    ).fillna(df_020002).fillna(df_020003)[df_020001.columns]
    result = result.drop(OrgPersonMapping.data_source.name, axis=1)
    return result


def main():
    io.to_sql(OrgPersonMapping.__tablename__, _engine_wt, transform())

if __name__ == "__main__":
    main()


# def split_source(dataframe, source_key):
#     sources = set(dataframe[source_key])
#     dfs = {source: dataframe.ix[dataframe[source_key] == source] for source in sources}
#     return dfs
#
#
# def merge_source(dataframe, source_key, main):
#     dfs = split_source(dataframe, source_key)
#     df_main = dfs[main]
#
#     keys = {set}