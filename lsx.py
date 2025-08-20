import pandas as pd
import re

# ==== 1. Đọc file ====
file_path = "02.08.2025 LSX XC NM.HRC1 1.xlsx"
df = pd.read_excel(file_path, sheet_name="02.08.25", skiprows=6)

time_col = "Thời gian dự kiến SX\nTime/Date"
df[time_col] = df[time_col].ffill()
# ==== 2. Hàm tách ngày bắt đầu & kết thúc ====
def extract_dates(val):
    if pd.isna(val):
        return None, None
    text = str(val).replace("\n", " ").strip()
    found = re.findall(r"(\d{2}/\d{2}/\d{4})", text)
    if len(found) >= 2:
        start = pd.to_datetime(found[0], dayfirst=True, errors="coerce")
        end = pd.to_datetime(found[-1], dayfirst=True, errors="coerce")
        return start, end
    elif len(found) == 1:
        d = pd.to_datetime(found[0], dayfirst=True, errors="coerce")
        return d, d
    else:
        return None, None

# ==== 3. Tạo DataFrame block thời gian ====
block_days = df[[time_col]].drop_duplicates().copy()
block_days[["Ngày bắt đầu block", "Ngày kết thúc block"]] = block_days[time_col].apply(
    lambda x: pd.Series(extract_dates(x))
)

# Chuyển sang datetime
block_days["Ngày bắt đầu block"] = pd.to_datetime(block_days["Ngày bắt đầu block"], dayfirst=True, errors="coerce")
block_days["Ngày kết thúc block"] = pd.to_datetime(block_days["Ngày kết thúc block"], dayfirst=True, errors="coerce")

# Tính số ngày yêu cầu của block
block_days["Số ngày yêu cầu block"] = (
    (block_days["Ngày kết thúc block"] - block_days["Ngày bắt đầu block"]).dt.days + 1
)

# ==== 4. Gộp số ngày này vào df gốc ====
df = df.merge(block_days, on=time_col, how="left")

# ==== 5. Điền giá trị Order còn trống ====
df["Số Order number"] = df["Số Order number"].ffill()

# ==== 6. Ép kiểu số lượng ====
df["Unnamed: 4"] = pd.to_numeric(df["Unnamed: 4"], errors="coerce").fillna(0)
df["Unnamed: 5"] = pd.to_numeric(df["Unnamed: 5"], errors="coerce").fillna(0)

# ==== 7. Tính toán theo Order ====
agg_df = df.groupby(
    ["Số Order number", "Ngày bắt đầu block", "Ngày kết thúc block", "Số ngày yêu cầu block"],
    as_index=False
).agg({
    "Unnamed: 4": "sum",
    "Unnamed: 5": "sum"
})

agg_df["SL yêu cầu (tấn)"] = (agg_df["Unnamed: 4"] + agg_df["Unnamed: 5"])*1000
agg_df["SL trung bình/ngày"] = (agg_df["SL yêu cầu (tấn)"] / agg_df["Số ngày yêu cầu block"])

# ==== 8. Xuất kết quả ====
agg_df = agg_df[[
    "Số Order number","SL yêu cầu (tấn)",  "SL trung bình/ngày"
]]
agg_df.to_excel("lsx14.xlsx", index=False)
print("Đã xuất file: sanluong_theo_block1.xlsx")
