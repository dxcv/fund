import sys
import datetime as dt
import hashlib as h
import pandas as pd


def gs(pwd):
    password = {}
    for x, y in zip("abcdefghijklmnopqrstuvwxyz", range(26)):
        password[x] = y
    sum = 0
    for x in pwd:
        sum += password[x]
    return sum


def sum_ord(acc):
    return sum([ord(c) for c in acc])


today = dt.date.today()
today = dt.datetime(today.year, today.month, today.day)

file_path = "C:/Users/Yu/Desktop/CRM"
file_accounts = "Accounts.xlsx"
accounts = pd.read_excel(file_path + "/" + file_accounts, sheetname="121.40.18.150")
cols = accounts.columns
accounts_existed = {
    0: set(accounts.loc[accounts["Account_type"] == 0]["Account"].apply(lambda x: x[8:])),
    1: set(accounts.loc[accounts["Account_type"] == 1]["Account"].apply(lambda x: x[3:]))
}

app = pd.read_excel(input("input a application:\n")[1:-1], header=None)

print("\n\n", app)
app = app.T
app = dict(zip(app.iloc[0].tolist(), app.iloc[1].tolist()))

while True:
    acc_type = int(input("\n\nAccount type: {0: Trial, 1: Formal}\n"))
    if acc_type not in {0, 1}:
        print("Incorrect account_name type, retry...")
        continue

    acc_1 = input("Input Account:")
    if acc_1 in accounts_existed[acc_type]:
        print("DUPLICATE Account: {0}, retry...\n".format(acc_1))
        continue

    acc_2 = input("Verify the Account:")

    if acc_1 != acc_2:
        print("Different input, retry...\n")
        continue
    else:
        break

acctype_prifix = {0: "jr_test_", 1: "jr_"}
acc = acctype_prifix[acc_type] + acc_1
if acc_1[-1] not in [str(x) for x in range(10)]:
    acc_suffixed = "{0}_{1}".format(acc, gs(acc_1))
else:
    acc_suffixed = "{0}_{1}".format(acc, gs(acc_1[:-1]) + int(acc_1[-1]))

pwd = h.sha1(bytes(acc_suffixed, encoding="UTF-8")).hexdigest()
print("New Account: {0}:{1}".format(acc, pwd))

new_record = pd.DataFrame(
    [app["Company_name"], app["Department"], app["Company_type"], acc, acc_type, pwd, today, app["Salesman"]]).T
new_record.columns = cols
accounts = accounts.append(new_record)
accounts.index = range(len(accounts))

while True:
    exit = input("\nYou're going to create a new account_name: {0}, input 1 to comfirm...".format(acc_1))
    if exit == "1":
        accounts.to_excel(file_path + "/" + file_accounts, sheet_name="121.40.18.150")
        print("Successfully created account_name...")
        break
    elif exit == "0":
        print("Rejected...")
        break
    else:
        print("Retry...")
        continue
