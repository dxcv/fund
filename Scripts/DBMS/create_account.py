from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from utils.database import config as cfg
import pandas as pd


def create(acc, pwd, conn):
    sql_create = "CREATE USER '{acc}'@'%%' IDENTIFIED BY '{pwd}'".format(acc=acc, pwd=pwd)
    conn.execute(sql_create)


def grant(acc, conn, acc_type="trial"):
    if acc_type == "trial":
        sql_grant1 = "GRANT SELECT ON product.* TO '{acc}'@'%%' WITH MAX_USER_CONNECTIONS 0;".format(acc=acc)
        sql_grant2 = "GRANT SELECT ON product_mutual.* TO '{acc}'@'%%' WITH MAX_USER_CONNECTIONS 0;".format(acc=acc)
        sql_grant = "{0}{1}".format(sql_grant1, sql_grant2)

    elif acc_type == "2G":
        sql_grant1 = "GRANT SELECT ON `base`.* TO '{acc}'@'%%';".format(acc=acc)
        sql_grant2 = "GRANT SELECT ON `test_gt`.* TO '{acc}'@'%%';".format(acc=acc)
        sql_grant = "{0}{1}".format(sql_grant1, sql_grant2)

    elif acc_type == "4G":
        sql_grant1 = "GRANT SELECT ON `product`.* TO '{acc}'@'%%';".format(acc=acc)
        sql_grant2 = "GRANT SELECT ON `easy`.* TO '{acc}'@'%%';".format(acc=acc)
        sql_grant = "{0}{1}".format(sql_grant1, sql_grant2)
    conn.execute(sql_grant)


def drop_user(acc, conn):
    # conn.execute("DROP USER '{acc}'@'%%'".format(acc=acc))
    conn.execute("DROP USER {acc}".format(acc=acc))


def generate_account(acc, pwd, conn, acc_type="trial"):
    create(acc, pwd, conn)
    grant(acc, conn, acc_type)


def test_connection(acc, pwd, server_alias="4G"):
    engine_test = create_engine(
        "mysql+pymysql://{acc}:{pwd}@{ip}:{port}".format(
            acc=acc,
            pwd=pwd,
            ip=cfg.servers[server_alias]["outer"][0],
            port=cfg.servers[server_alias]["outer"][1]
        )
    )
    try:
        conn_test = engine_test.connect()
        print(not conn_test.closed)
        conn_test.close()
    except OperationalError as e:
        print(e)


#
def main():
    engines = cfg.load_engine()
    engine_4G = engines["4G"]
    conn_4G = engine_4G.connect()

    record = pd.read_excel("C:/Users/Yu/Desktop/CRM/Accounts.xlsx").iloc[-1]
    acc, pwd = record["Account"], record["Pwd"]
    # acc, pwd = record["Account"], record["Pwd"]

    generate_account(acc, pwd, conn_4G, "trial")
    test_connection(acc, pwd)


if __name__ == "__main__":
    main()
