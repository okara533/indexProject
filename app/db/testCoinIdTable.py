
import sqlite3
import pandas as pd
conn = sqlite3.connect("data/coindb.db")
cursor = conn.cursor()
cursor.execute("SELECT * FROM coins")
rows = cursor.fetchall()
conn.close()
df=pd.DataFrame(rows, columns=["id", "symbol", "name", "image", "current_price", "market_cap", 
                              "market_cap_rank","fully_diluted_valuation", "total_volume", "high_24h", "low_24h", "price_change_24h",
                              "price_change_percentage_24h", "market_cap_change_24h", "market_cap_change_percentage_24h",
                              "circulating_supply", "total_supply", "max_supply", "ath", "ath_change_percentage",
                              "ath_date", "atl", "atl_change_percentage", "atl_date",
                              "roi_times", "roi_currency", "roi_percentage", "last_updated",
                              "created_date", "updated_date"])

df["created_date"] = pd.to_datetime(df["created_date"])
df["updated_date"] = pd.to_datetime(df["updated_date"])
df["last_updated"] = pd.to_datetime(df["last_updated"])
print(df.head(5))
print(df.tail(5))
print("Toplam Id:",len(df))
print("Unique Id Count:",len(df["id"].unique()))
def minMax_Date_id(df, ref_col, desire_col):
    min_val = df[ref_col].min()
    min_col= df[df[ref_col] == min_val][desire_col].values[0]
    max_val = df[ref_col].max()
    max_col = df[df[ref_col] == max_val][desire_col].values[0]
    return min_val, min_col, max_val,max_col
for i in ["created_date", "updated_date", "last_updated"]:
    desireCol = "id"
    min_val, min_col, max_val,max_col = minMax_Date_id(df,i,desireCol)
    print(f"{i} için en büyük değer: {max_val}, {desireCol}: {max_col}")
    print(f"{i} için en küçük değer: {min_val}, {desireCol}: {min_col}")
