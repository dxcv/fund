import datetime as dt
from dateutil.relativedelta import relativedelta
from utils.crawl.wind_stock import WindStockPriceCrawler


def main():
    WindStockPriceCrawler(dt.date(2018, 3, 6), dt.date(2018, 4, 9)).crawl()
    # crawl_date_s = dt.date(2018, 3, 6)
    # crawl_date_e = crawl_date_s + relativedelta(days=1)


if __name__ == "__main__":
    main()
