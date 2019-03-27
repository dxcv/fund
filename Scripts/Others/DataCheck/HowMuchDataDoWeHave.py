import sys
import pandas as pd
from sqlalchemy import create_engine
import time

conn = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/base?charset=utf8")
conn.connect()

#q = pd.read_excel("c:\\users\\kael yu\\desktop\\questions_about_data.xlsx", index=False).dropna()

print(">>>Reading Your Questions...\n\n")
q = pd.read_excel(sys.argv[1], index=False).dropna()
print(">>>Done.\n")


print(">>>Connecting to MySql & Getting Data...")
result = []
for qid, question, sql in zip(q["QID"].tolist(), q["Question"].tolist(), q["SQL"].tolist()):
    temp = []
    temp.append(qid)
    temp.append(question)
    temp.append(conn.execute(sql).fetchall()[0])
    result.append(temp)
print(">>>Done.\n")

print(">>>Output the Results to csv...")
d = pd.DataFrame(result)
d.columns = ["QID", "Question", "Answer"]
d.to_csv("%s/Answers_%s.csv"%("D:" if not sys.argv[2:] else sys.argv[2], time.strftime("%Y%m%d%H%M%S",time.localtime(time.time()))), index=False)
print(">>>Done.")

    #print(conn.execute(t).fetchone()[0])
    #run = True
#schema_mapping = {1:"base", 2:"product", 3:"hello"}

#conn = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/%s?charset=utf8" % (schema_mapping[1]))
#conn.connect()

#print("ss")

#sql_fund_nvs = "SELECT COUNT(fund_id) FROM fund_nv_data "

#schema_old = input("\n\n>>>Choose A Schema: 1:base; 2:product\n")
#schema_new = None
#new_command = None
#while run:
#    if schema_new is None or schema_new != schema_old:
#        schema_new = schema_old
#        conn = create_engine("mysql+pymysql://jr_admin_test_0:e6524540d9733355c27d344876b15cf467251215@120.55.69.127:3306/%s?charset=utf8" % (schema_mapping[int(schema_new)]))
#        try:
#            conn.connect()
#            print(">>>Successfully Connect to DB...\n")
#        except Exception as e:
#            print(e)
    
#    else:
#        if new_command is None or new_command or new_command != "0":
#            new_command = input(">>>Input your command: 0:Rechoose the Schema; 1:Checkout Result\n")
#        if new_command == "0":
#            schema_old = input("\n\n>>>Choose A Schema: 1:base; 2:product\n")
#        if new_command == "1":
#            new_command = input(u">>>\nSuccessfully Checkout. You can input\n  0:Choose the Schema again;  \n  5:Exit\n")

#        if new_command == "5":
#            conn.close()
#            break

