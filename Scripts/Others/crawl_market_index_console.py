import sys
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
from utils.crawl import wind_market_index as wmi
from utils.database import io

__engine_rd = create_engine("mysql+pymysql://jr_admin_4:jr_admin_4@db.chfdb.cc:4171/base",
                            connect_args={"charset": "utf8"})
__previous_day = 5


def main():
    print("fetching data...{time}\n\n".format(time=dt.datetime.now()))
    data = wmi.construct_mi(dt.date.today(), dt.date.today() - dt.timedelta(__previous_day))
    print(data)
    checked = input("check your data...press 1 to continue, any other key to break...\n\n")
    if checked == "1":
        io.to_sql("market_index", __engine_rd, data)
    else:
        pass
    input("done...\npress any key to continue...")

if __name__ == "__main__":
    main()
