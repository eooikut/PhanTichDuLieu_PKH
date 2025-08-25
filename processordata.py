import pandas as pd
import re
import os
from datetime import datetime
# ==============================
# 1. XỬ LÝ FILE LỆNH SẢN XUẤT
# ==============================

import re
import pandas as pd

def get_lsx_range_from_file(file_path, sheet_name=0, row_index=5, col_index=0):
    val = pd.read_excel(file_path, sheet_name=sheet_name, header=None).iloc[row_index, col_index]
    if pd.isna(val):
        return None, None

    text = str(val)
    found = re.findall(r"(\d{2}/\d{2}/\d{4})", text)

    if len(found) >= 2:
        start = pd.to_datetime(found[0], dayfirst=True, errors="coerce")
        end = pd.to_datetime(found[-1], dayfirst=True, errors="coerce")
    elif len(found) == 1:
        start = end = pd.to_datetime(found[0], dayfirst=True, errors="coerce")
    else:
        start = end = None

    return start, end

def extract_dates(val):
    """Trích xuất ngày bắt đầu/kết thúc từ chuỗi thời gian."""
    if pd.isna(val):
        return None, None

    text = str(val).replace("\n", " ").strip()
    found = re.findall(r"(\d{2}/\d{2}/\d{4})", text)

    if len(found) >= 2:
        start = pd.to_datetime(found[0], dayfirst=True, errors="coerce")
        end = pd.to_datetime(found[-1], dayfirst=True, errors="coerce")
    elif len(found) == 1:
        start = end = pd.to_datetime(found[0], dayfirst=True, errors="coerce")
    else:
        start = end = None

    return start, end


def process_lsx(file_path, sheet_name=3, skip_rows=6):
    """Đọc và xử lý file LSX."""
    time_col = "Thời gian dự kiến SX\nTime/Date"
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows)

    # Điền giá trị thời gian cho các dòng trống
    df[time_col] = df[time_col].ffill()

    # Tạo bảng block_days
    block_days = df[[time_col]].drop_duplicates().copy()
    block_days[["Ngày bắt đầu block", "Ngày kết thúc block"]] = block_days[time_col].apply(
        lambda x: pd.Series(extract_dates(x))
    )

    # Ép kiểu datetime
    for col in ["Ngày bắt đầu block", "Ngày kết thúc block"]:
        block_days[col] = pd.to_datetime(block_days[col], dayfirst=True, errors="coerce")

    # Tính số ngày yêu cầu block
    block_days["Số ngày yêu cầu block"] = (
        (block_days["Ngày kết thúc block"] - block_days["Ngày bắt đầu block"]).dt.days + 1
    )

    # Merge lại vào df gốc
    df = df.merge(block_days, on=time_col, how="left")
    df["Số Order number"] = df["Số Order number"].ffill()

    # Chuyển cột SL sang số
    for col in ["Unnamed: 4", "Unnamed: 5"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Tính tổng SL theo block
    agg_df = df.groupby(
        ["Số Order number", "Ngày bắt đầu block", "Ngày kết thúc block", "Số ngày yêu cầu block"],
        as_index=False
    ).agg({
        "Unnamed: 4": "sum",
        "Unnamed: 5": "sum"
    })

    agg_df["SL yêu cầu (tấn)"] = (agg_df["Unnamed: 4"] + agg_df["Unnamed: 5"]) * 1000
    agg_df["SL trung bình/ngày"] = agg_df["SL yêu cầu (tấn)"] / agg_df["Số ngày yêu cầu block"]

    # Chuẩn bị dữ liệu để merge
    df_req = agg_df.rename(columns={
        "Số Order number": "Order",
        "SL yêu cầu (tấn)": "Tổng yêu cầu"
    })[["Order", "Tổng yêu cầu", "SL trung bình/ngày"]]

    return df_req



# ==============================
# 2. XỬ LÝ FILE SẢN LƯỢNG THỰC TẾ
# ==============================
def process_actual(file_path, sheet_name="Data"):
    """Đọc và xử lý file sản lượng thực tế."""
    df = pd.read_excel(file_path, sheet_name=sheet_name).dropna(how="all")
    df["Ngày sản xuất"] = pd.to_datetime(df["Ngày sản xuất"], errors="coerce")
    df["Khối lượng"] = pd.to_numeric(df["Khối lượng"], errors="coerce")
    df = df.dropna(subset=["Order", "Ngày sản xuất"])

    # Gom nhóm theo Order và Ngày
    df_daily = df.groupby(["Order", "Ngày sản xuất"], as_index=False)["Khối lượng"].sum()
    df_daily = df_daily.rename(columns={
        "Khối lượng": "Sản lượng thực tế",
        "Ngày sản xuất": "Ngày"
    })

    # Tổng sản lượng theo Order
    total_actual = df_daily.groupby("Order", as_index=False)["Sản lượng thực tế"].sum()
    total_actual = total_actual.rename(columns={"Sản lượng thực tế": "Tổng sản lượng thực tế"})

    return df_daily, total_actual
#3 XỬ LÝ FILE SẢN LƯỢNG KHO

def classify(row):
    if pd.isna(row["SL trung bình/ngày"]):
        return "Chờ LSX mới"
    if not row.get("Trong_khoang_LSX", False):
        return "Ngoài phạm vi LSX"
    if row["Sản lượng thực tế"] < row["SL trung bình/ngày"]:
        diff = row["SL trung bình/ngày"] - row["Sản lượng thực tế"]
        return f"Thất thoát {diff:,.0f} kg"
    elif row["Tổng sản lượng thực tế"] > row["Tổng yêu cầu"]:
        diff = row["Tổng sản lượng thực tế"] - row["Tổng yêu cầu"]
        return f"Vượt tổng {diff:,.0f} kg"
    else:
        return "Đúng/nhỉnh hơn tiến độ"

# ==============================
# 4. TỔNG HỢP BÁO CÁO

# ==============================
def generate_report(lsx_path, actual_path):
    df_req = process_lsx(lsx_path)                  
    df_daily, total_actual = process_actual(actual_path)  

    lsx_start, lsx_end = get_lsx_range_from_file(lsx_path)

    df_report = df_daily.merge(df_req, on="Order", how="left")
    df_report = df_report.merge(total_actual, on="Order", how="left")

    if lsx_start and lsx_end:
        df_report["Trong_khoang_LSX"] = (
            (df_report["Ngày"] >= lsx_start) & 
            (df_report["Ngày"] <= lsx_end)
        )
    else:
        df_report["Trong_khoang_LSX"] = False

    df_report = df_report[
        df_report["Trong_khoang_LSX"] | df_report["SL trung bình/ngày"].isna()
    ]

    df_report["Trạng thái"] = df_report.apply(classify, axis=1)

    # 🔹 Nếu có yêu cầu thì lưu ra file JSON để giữ lịch sử

    return df_report


def xu_ly_ton_kho(file_sanluong, file_kho):

    # Đọc dữ liệu
    df_sanluong = pd.read_excel(file_sanluong)
    df_kho = pd.read_excel(file_kho)

    # Bỏ các dòng trống ID Cuộn Bó
    df_kho = df_kho.dropna(subset=["ID Cuộn Bó"])
    df_sanluong = df_sanluong.dropna(subset=["ID Cuộn Bó"])

    # Chuẩn hóa kiểu dữ liệu
    df_kho["ID Cuộn Bó"] = df_kho["ID Cuộn Bó"].astype(str).str.strip()
    df_sanluong["ID Cuộn Bó"] = df_sanluong["ID Cuộn Bó"].astype(str).str.strip()

    # 1️⃣ Xác định cuộn bó chưa nhập kho
    id_kho_set = set(df_kho["ID Cuộn Bó"].unique())
    df_chua_nhap = df_sanluong[~df_sanluong["ID Cuộn Bó"].isin(id_kho_set)].copy()

    # Gộp theo Order để tính tổng khối lượng chưa nhập kho
    df_chua_nhap_sum = df_chua_nhap.groupby("Order").agg(
        Số_lượng_chưa_nhập_kho=("Khối lượng", "sum"),
        Material_Description=("Material Description", "first")   # Lấy mô tả vật liệu
    ).reset_index()

    # 2️⃣ Phân loại tồn kho
    df_kho["Tồn kho loại"] = df_kho["SO Mapping"].apply(
        lambda x: "Không tự do" if pd.notna(x) else "Tự do"
    )

    # 3️⃣ Tổng hợp tồn kho theo Order
    summary = df_kho.groupby("Order").agg(
        Material_Description=("Material Description", "first"),  # Lấy mô tả vật liệu
        Tổng_tồn_kho_tự_do=("Khối lượng", lambda x: x[df_kho.loc[x.index, "Tồn kho loại"] == "Tự do"].sum()),
        Tổng_tồn_kho_mapping_SO=("Khối lượng", lambda x: x[df_kho.loc[x.index, "Tồn kho loại"] == "Không tự do"].sum()),
        Tổng_tồn_kho=("Khối lượng", "sum")
    ).reset_index()

    # 4️⃣ Merge thêm cột số lượng chưa nhập kho
    summary = summary.merge(df_chua_nhap_sum, on=["Order", "Material_Description"], how="outer").fillna(0)

    # Đổi tên cột
    summary = summary.rename(columns={
        "Order": "Mã Order",
        "Material_Description": "Material Description",  # ✅ sửa tên cột về giống file SO
        "Tổng_tồn_kho_tự_do": "Tồn kho chưa Mapping SO",
        "Tổng_tồn_kho_mapping_SO": "Tồn kho Mapping SO",
        "Tổng_tồn_kho": "Tổng tồn kho",
        "Số_lượng_chưa_nhập_kho": "Số lượng chờ nhập kho"
})


    # Bỏ .0 trong Order
    summary["Mã Order"] = summary["Mã Order"].astype(str).str.replace(r"\.0$", "", regex=True)

    # Chuyển số về dạng int nếu không cần phần thập phân
    for col in ["Tồn kho chưa Mapping SO", "Tồn kho Mapping SO", "Tổng tồn kho", "Số lượng chờ nhập kho"]:
        summary[col] = summary[col].astype(int)

    return summary

def process_so_file(file1_path, file2_path):

    # ===== 1. Đọc file1 và tính tiến độ =====
    df1 = pd.read_excel(file1_path, skiprows=2)
    print(f"Số dòng trong file1: {len(df1)}")
    print("Các cột trong file1:", df1.columns.tolist())

    # Tính tiến độ (Process) cho từng SO + Material
    df1['Process'] = df1.apply(
        lambda row: (row['Shipped Quantity (KG)'] / row['Quantity (KG)'] * 100)
        if row['Quantity (KG)'] > 0 else 0,
        axis=1
    ).round(2)

    # Chỉ giữ các cột cần thiết
    df1_processed = df1[['Sales Document', 'Material','Shipped Quantity (KG)','Quantity (KG)', 'Process']].copy()

    # ===== 2. Đọc file2 (thông tin Order, Material Description, Customer N) =====
    df2 = pd.read_excel(file2_path)

    # ===== 3. Merge hai DataFrame =====
    merged_df = pd.merge(
        df2,
        df1_processed,
        left_on=['SO Mapping', 'Material'],
        right_on=['Sales Document', 'Material'],
        how='inner'
    )

    # ===== 4. Chuẩn hóa dữ liệu =====
    for col in ['Order','SO Mapping','Material Description','Customer N','Process']:
        if merged_df[col].dtype == 'object':  # chỉ áp dụng với cột chuỗi
            merged_df[col] = merged_df[col].str.strip()
    
    # Chuyển kiểu dữ liệu
    merged_df['Process'] = merged_df['Process'].astype(float)
    merged_df['Order'] = merged_df['Order'].astype(str)
    merged_df['SO Mapping'] = merged_df['SO Mapping'].astype(str)

    # ===== 5. Loại bỏ trùng lặp =====
    merged_df = merged_df.drop_duplicates(
        subset=['Order','SO Mapping','Material Description','Customer N','Process']
    )

    # ===== 6. Lấy các cột cần xuất =====
    merged_df1 = merged_df[['Order','SO Mapping', 'Material Description','Customer N','Shipped Quantity (KG)','Quantity (KG)','Process']]
    return merged_df1

def xu_ly_ton_kho_full(file_sl, file_kh, file_so):
    # --- B1: Xử lý file tồn kho ---
    data1 = xu_ly_ton_kho(file_sl, file_kh)
    data1 = data1.rename(columns={"Mã Order": "Order"})
    data1['Order'] = pd.to_numeric(data1['Order'], errors="coerce").fillna(0).astype(int)

    # --- B2: Xử lý file SO ---
    data2 = process_so_file(file_so, file_kh)
    data2['Order'] = pd.to_numeric(data2['Order'], errors="coerce").fillna(0).astype(int)

    # --- B3: Merge 2 bảng ---
    data = pd.merge(
        data1,
        data2,
        how='left',   # Giữ tất cả Order trong data1
        on=["Order", "Material Description"]
    )

    # --- B4: Thêm dòng "Chưa có SO" cho tồn kho chưa mapping ---
    rows_to_add = []
    for _, row in data1.iterrows():
        if row["Tồn kho Mapping SO"] == 0:
            rows_to_add.append({
                "Order": row["Order"],
                "Tồn kho chưa Mapping SO": row["Tồn kho chưa Mapping SO"],
                "Tồn kho Mapping SO": row["Tồn kho Mapping SO"],
                "Tổng tồn kho": row["Tổng tồn kho"],
                "Số lượng chờ nhập kho": row["Số lượng chờ nhập kho"],
                "SO Mapping": "Chưa có SO",
                "Material Description": row["Material Description"],
                "Customer N": "Chưa có SO",
                "Shipped Quantity (KG)": 0,
                "Quantity (KG)": 0,
                "Process": 0
            })

    if rows_to_add:
        data = pd.concat([data, pd.DataFrame(rows_to_add)], ignore_index=True)

    # --- B5: Fill NaN ---
    data = data.fillna({
        "SO Mapping": "Chưa có SO",
        "Shipped Quantity (KG)": 0,
        "Quantity (KG)": 0,
        "Process": 0
    })

    # --- B6: Ép kiểu số cho các cột số ---
    num_cols = [
        "Tồn kho chưa Mapping SO","Tồn kho Mapping SO","Tổng tồn kho","SO Mapping",
        "Số lượng chờ nhập kho","Shipped Quantity (KG)",
        "Quantity (KG)","Process"
    ]
    for col in num_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0).astype(int)

    return data
def load_data(file_sl, file_kh, file_so):
    df=xu_ly_ton_kho_full(file_sl, file_kh, file_so)
    # Sort dữ liệu
    df = df.sort_values(by=['Order', 'Material Description', 'SO Mapping'])

    grouped = []
    for (order, material), group in df.groupby(['Order','Material Description']):
        grouped.append({
            'Order': order,
            'Material': material,
            'TonKhoChuaMapping': group['Tồn kho chưa Mapping SO'].iloc[0],
            'TonKhoMapping': group['Tồn kho Mapping SO'].iloc[0],
            'TongTonKho': group['Tổng tồn kho'].iloc[0],
            'ChoNhapKho': group['Số lượng chờ nhập kho'].iloc[0],
            'SOs': group[['SO Mapping','Shipped Quantity (KG)','Quantity (KG)','Process']].to_dict(orient='records')
        })
    return grouped

