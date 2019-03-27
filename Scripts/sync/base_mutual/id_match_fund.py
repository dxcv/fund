import pandas as pd
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from utils.database.models.crawl_public import DFundInfo
from utils.database.models.base_public import IdMatch, FundInfo
from utils.database import io

engine_rd = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171", echo=False,
                          connect_args={"charset": "utf8"})
engine_wt = create_engine("mysql+pymysql://root:smyt0317@58cb57c164977.sh.cdb.myqcloud.com:4171/{schema}".format(
    schema=IdMatch.__table_args__["schema"]), echo=False, connect_args={"charset": "utf8"})
db_session = sessionmaker(bind=engine_rd)
session = db_session()


# Fetch all org info from crawl_public.d_org_info, base_public.org_info
def fetch_unmatched_fund(data_source):
    fields = [IdMatch.source_id, IdMatch.matched_id, DFundInfo.fund_id]

    query_foi = session.query(DFundInfo).outerjoin(
        IdMatch, and_(IdMatch.source_id == DFundInfo.fund_id, IdMatch.data_source == DFundInfo.data_source, IdMatch.id_type == 1)
    ).filter(
        DFundInfo.data_source == data_source
    ).with_entities(
        *fields
    )
    df = pd.DataFrame(query_foi.all())
    df_unmatched = df.ix[df[IdMatch.matched_id.name].apply(lambda x: x is None)]
    df_unmatched.index = range(len(df_unmatched))
    return df_unmatched


def match(data_source):
    unmatched = fetch_unmatched_fund(data_source)
    result = pd.DataFrame()
    result[IdMatch.source_id.name] = result[IdMatch.matched_id.name] = unmatched[DFundInfo.fund_id.name]
    result[IdMatch.data_source.name] = data_source
    result[IdMatch.id_type.name] = 1
    result[IdMatch.accuracy.name] = 1

    return result


def main():
    data_sources = ["020001", "020002", "020003"]
    try:
        for data_source in data_sources:
            print(data_source)
            print(match(data_source))
            io.to_sql(IdMatch.__tablename__, engine_wt, match(data_source))
    except Exception as e:
        print(data_source, e)

if __name__ == "__main__":
    main()
