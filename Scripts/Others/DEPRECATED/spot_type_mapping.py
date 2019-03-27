import pandas as pd
from utils.database import io, config as cfg
type_code_sws = {
    "农林牧渔": 110000,
    "采掘": 210000,
    "化工": 220000,
    "钢铁": 230000,
    "有色金属": 240000,
    "电子": 270000,
    "汽车": 280000,
    "家用电器": 330000,
    "食品饮料": 340000,
    "纺织服装": 350000,
    "轻工制造": 360000,
    "医药生物": 370000,
    "公用事业": 410000,
    "交通运输": 420000,
    "房地产": 430000,
    "商业贸易": 450000,
    "休闲服务": 460000,
    "银行": 480000,
    "非银金融": 490000,
    "综合": 510000,
    "建筑材料": 610000,
    "建筑装饰": 620000,
    "电气设备": 630000,
    "机械设备": 640000,
    "国防军工": 650000,
    "计算机": 710000,
    "传媒": 720000,
    "通信": 730000
}

engine_wt = cfg.load_engine()["2Gt"]

_path = "C:/Users/Yu/Desktop/上市股票一览.xlsx"


def read_type(path):
    df = pd.read_excel(path)
    df = df[["代码", "名称", "申万一级分类名称"]]
    df.columns = ["subject_id", "subject_name", "type_name_sws"]
    df.loc[df["type_name_sws"] == 0, "type_name_sws"] = None
    df["type_code_sws"] = df["type_name_sws"].apply(lambda x: str(type_code_sws.get(x)))
    return df


def main():
    df = read_type(_path)
    print(df[["type_code_sws", "type_name_sws"]].drop_duplicates())
    io.to_sql("spot_type_mapping", engine_wt, df, "replace")


if __name__ == "__main__":
    main()
