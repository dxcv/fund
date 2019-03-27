# import hashlib as h
# import base64
# import zlib
#
# token = q = "PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw"
# b.b64decode(b"PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw", '-_')
#
#
#
# url_decode = Base64UrlDecode(token) # The token is the same string as the above one.
# # https://docs.python.org/2/library/zlib.html#zlib.compressobj
# for i in range(-15, 32): # try all possible ones, but none works.
#     try:
#         decode = zlib.decompress(url_decode, i)
#     except:
#         pass
#
#
# def decode_base64(data):
#     """Decode base64, padding being optional.
#
#     :param data: Base64 data as an ASCII byte string
#     :returns: The decoded byte string.
#
#     """
#     missing_padding = len(data) % 4
#     if missing_padding != 0:
#         # data += b'='* (4 - missing_padding)
#         # data = data[:-missing_padding]
#     return base64.b64decode(data)
#
# q = decode_base64(b"PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw")
#
#
#
# def test_Inflate(self):
#     self.assertEquals(
#         "{\"access_token\":\"\",\"token_type\":\"Bearer\",\"id_token\":\"eyJhbGciOiJSU0EtT0FFUCIsImVuYyI6IkExMjhDQkMtSFMyNTYiLCJ6aXAiOiJERUYifQ.sQT2n2NFO-6vSXJ3MZpTbWgYiRz5PKC0OFy_EHTUIQCtpde0eZAowwRGheAte68wnNeN7LqRvkCnrrTn_HavHwBMCGS4eYxoGFx2w1Tu_iWvL-47hIEy1kAQsdw_ziztmZJ5vAN15hDNw9flmfIUI2sAPMnO4kVHlba47Hu8fxA.jCEn6O6U99SY1ZxB7yBxVA.UgkPlaYT6PY1oMZ-gfV_VhrzE6Dx5Ga08Pz3QXLsiobqgkTgV6_uufUOBgghLBDeZXv8kWPKuTsRIGmdqHgah-Sa94hQOQlR1IgtX-La81-T2KKgVhrCVwKCb3QdQLrEUsZYPmupXL5Jn7bQ2CSllMFs9FIpuKI4NuPFo5spgJsiEcyeAIlB2a5j62Up3IS1WsKoXU9OZfBWMuTeNUBVsfqjlKlsuuckC77zwJqZVKb_zrDqpj_Ut_3EoT1m-FNPcCcc86wm8YdGdRv3yCqE_MihQUyLJ-fIWMy8dFlDhV6GJyjqvrqQan8h1gIePb3a.zPW3ICw0KiYLHMH1NK_5JQ\",\"refresh_token\":null,\"expires_in\":86400}",
#         SlRestUtils.Inflate(
#             "PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw")
#     )
#
# def b64url_decode(data):
#     """Decode base64, padding being optional.
#
#     :param data: Base64 data as an ASCII byte string
#     :returns: The decoded byte string.
#
#     """
#     data = data.encode("utf8")
#     missing_padding = len(data) % 4
#     if missing_padding != 0:
#         data += b'=' * (4 - missing_padding)
#     return base64.b64decode(data)
#
# q1 = "PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw"
# q2 = b"PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw"
#
# q = b64url_decode(q1)
#
# s = "PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw"
# zlib.decompress(s.encode("utf8"))
#
# s = "{\"access_token\":\"\",\"token_type\":\"Bearer\",\"id_token\":\"eyJhbGciOiJSU0EtT0FFUCIsImVuYyI6IkExMjhDQkMtSFMyNTYiLCJ6aXAiOiJERUYifQ.sQT2n2NFO-6vSXJ3MZpTbWgYiRz5PKC0OFy_EHTUIQCtpde0eZAowwRGheAte68wnNeN7LqRvkCnrrTn_HavHwBMCGS4eYxoGFx2w1Tu_iWvL-47hIEy1kAQsdw_ziztmZJ5vAN15hDNw9flmfIUI2sAPMnO4kVHlba47Hu8fxA.jCEn6O6U99SY1ZxB7yBxVA.UgkPlaYT6PY1oMZ-gfV_VhrzE6Dx5Ga08Pz3QXLsiobqgkTgV6_uufUOBgghLBDeZXv8kWPKuTsRIGmdqHgah-Sa94hQOQlR1IgtX-La81-T2KKgVhrCVwKCb3QdQLrEUsZYPmupXL5Jn7bQ2CSllMFs9FIpuKI4NuPFo5spgJsiEcyeAIlB2a5j62Up3IS1WsKoXU9OZfBWMuTeNUBVsfqjlKlsuuckC77zwJqZVKb_zrDqpj_Ut_3EoT1m-FNPcCcc86wm8YdGdRv3yCqE_MihQUyLJ-fIWMy8dFlDhV6GJyjqvrqQan8h1gIePb3a.zPW3ICw0KiYLHMH1NK_5JQ\",\"refresh_token\":null,\"expires_in\":86400}"
#
# s64 = base64.b64encode(s.encode())
# # b'eyJhY2Nlc3NfdG9rZW4iOiIiLCJ0b2tlbl90eXBlIjoiQmVhcmVyIiwiaW
# zlib.decompress(q[2:-4])
# zlib.compress(q).decompress(q)
# zlib.compressobj(q)
#
# base64.b64decode(s64.decode().encode())
#
# StringUtils.newStringUtf8(
#     inflate(
#         Base64.decodeBase64("PZLHrptQAET_xevHE8VgnB1gyqVjig0bRLkUg-k9yr_HiZTsZo5mZjU_T1GSwHEMp7aCzenH6fR1-ivDae_gx7MwGuDwoWX6PwN3uYjFpDRK2XZRfnJQQXA5MIK3N_s7oEDFb9qruFmVNtmCtuuOX6qcTEVP5k-Hv7t-mVnfo-XgDa4LBkIt9lMmtKBz4kful_eDNBUONYQ95CXHBRY3dSlEYcC063oXC8hMkKLXRof6Re3vS8U1w-A0oRQt0spqnGifob-1orDhK-bMYflYVOR8KQC_YxVjjekaHuUxvQOZXBgdI4ubvl6z-p0BF-AjY2qNca48qY6j80Wa6Wxjvl8c31AG5V6vto8FG3vZ2c1jvt28MuvIdyjTx1otQPLMC71iOHjqtpFihNLmQVhPdSzbuM8rJ_eocJ4z12DzvFDZGwyeC109TGV2xjsQ32kv5VGB2NH1XFiGVd8xkE9PRI1oDHFwRck_25y3KlxMWKmlDrw7Br75nrunSsrNJbZwzq5rTRivAuhmBZz12RRacuxyeSz5ZIcMqFk8Il8U7nYEsLHHqLRP92oEGfvQZgfqLuuNWf-qlXqc56TiLpdjlfvAU-LwGG599wrdKST41sHeiKCbCZckNLW-aT8V0_tC7FzPh1pZWO6uykgGHtpOp0J9KzxKlPdXvwy9FTV0geUAmjERfR_mgwDciiqlr0qahOlKSMrW524DzAY4Fv8-18x1_XWCW1d-aFh-CE2dUfTXbw")
#     )
# )
#
#
#
# import pandas as pd
# from utils.database import config as cfg
# engine = cfg.load_engine()["2Gb"]
#
# result = []
#
# binlogs = pd.read_sql("SHOW MASTER LOGS;", engine)["Log_name"].tolist()
# step = 1000
# log_name = binlogs[-5]
# for i in range(0, 10000000, step):
#     if i % 100000 == 0:
#         print(i)
#     q = pd.read_sql("SHOW BINLOG EVENTS IN '{log_name}' LIMIT {offset}, {limit}".format(offset=i, limit=step, log_name=log_name), engine)
#     tmp = q.ix[q["Info"].apply(lambda x: "fund_nv_data_standard" in x)]
#     if len(tmp) > 0:
#         print("got one")
#         result.append(tmp)
#     if len(q) == 0:
#         print(i, "done")
#         break
#
# a = []
# for log in result:
#     tmp = log.ix[log["Info"].apply(lambda x: "nav" in x)]
#     if len(tmp) > 0:
#         a.append(tmp)
#         print(tmp)
# with open("c:/Users/Yu/Desktop/txt/log.txt", "a") as f:
#     for log in a:
#         for query in log["Info"]:
#             f.write(query.replace("\n", ""))
#
#
# ls = pd.read_sql("SELECT DISTINCT fund_id FROM fund_nv_data_standard", engine)["fund_id"].tolist()
# tables = ["fund_month_return", "fund_month_risk", "fund_subsidiary_month_index"]
# for table in tables:
#     print()
#     q = engine.execute("DELETE FROM {table} WHERE fund_id NOT IN {ids}".format(
#         ids=str(tuple(ls)), table=table
#     ))