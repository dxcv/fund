from utils.database import config as cfg, io
from utils.script import swanv
from utils.script.scriptutils import tic

engines = cfg.load_engine()
engine_rd = engines["2Gb"]
engine_wt = engines["2Gb"]


def main():
    sql_update = "UPDATE fund_nv_data_standard SET swanav = nav WHERE nav = added_nav AND swanav IS NULL"
    engine_wt.execute(sql_update)

    sql_check = "UPDATE fund_nv_data_standard SET swanav = NULL WHERE nav <> added_nav AND swanav = nav"
    engine_wt.execute(sql_check)

    df = swanv.calculate_swanav()
    if len(df) > 0:
        df.index = range(len(df))
        print("{num} records to update".format(num=len(df)))
        io.to_sql("fund_nv_data_standard", engine_wt, df, "update")

    engine_wt.execute(sql_check)


if __name__ == "__main__":
    tic("swanav...")
    main()
    tic("Done...")
