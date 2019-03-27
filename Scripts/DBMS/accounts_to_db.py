import pandas as pd
from utils.script import encryption
from utils.database import config as cfg


#### SETTING HERE ####
ACCOUNT_FILEPATH = "C:/Users/Yu/Desktop/CRM/Accounts.xlsx"
ACCOUNT_CREATOR = "余劭彦"
######################

engines = cfg.load_engine()
engine = engines["2Gu"]

conn = engine.connect()


def main():
    user_accounts = pd.read_excel(ACCOUNT_FILEPATH)
    user_accounts.columns = ["company", "department", "company_type", "account", "account_type", "password", "entry_time",
                             "salesman"]

    user_accounts["server_ip"] = cfg.servers["4G"]["outer"][0]
    user_accounts["server_port"] = cfg.servers["4G"]["outer"][1]
    user_accounts["created_by"] = ACCOUNT_CREATOR
    user_accounts["altered_by"] = ACCOUNT_CREATOR
    user_accounts["password_salted"] = [
        encryption.salted(pwd, salt) for pwd, salt in zip(user_accounts["password"], user_accounts["account"])]
    user_accounts["password"] = user_accounts["password_salted"]
    del user_accounts["password_salted"]

    user_accounts[-1:].to_sql("accounts", conn, if_exists="append", index=False)


if __name__ == "__main__":
    main()
