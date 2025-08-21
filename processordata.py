import pandas as pd
import re


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
        Số_lượng_chưa_nhập_kho=("Khối lượng", "sum")
    ).reset_index()

    # 2️⃣ Phân loại tồn kho
    df_kho["Tồn kho loại"] = df_kho["SO Mapping"].apply(
        lambda x: "Không tự do" if pd.notna(x) else "Tự do"
    )

    # 3️⃣ Tổng hợp tồn kho theo Order
    summary = df_kho.groupby("Order").agg(
        Tổng_tồn_kho_tự_do=("Khối lượng", lambda x: x[df_kho.loc[x.index, "Tồn kho loại"] == "Tự do"].sum()),
        Tổng_tồn_kho_mapping_SO=("Khối lượng", lambda x: x[df_kho.loc[x.index, "Tồn kho loại"] == "Không tự do"].sum()),
        Tổng_tồn_kho=("Khối lượng", "sum")
    ).reset_index()

    # 4️⃣ Merge thêm cột số lượng chưa nhập kho
    summary = summary.merge(df_chua_nhap_sum, on="Order", how="left").fillna(0)
    # Đổi tên cột
    summary = summary.rename(columns={
        "Order": "Mã Order",
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
    # # 5️⃣ Xuất ra file Excel
    # with pd.ExcelWriter(output_path) as writer:
    #     df_chua_nhap_sum.to_excel(writer, sheet_name="Chưa nhập kho", index=False)
    #     summary.to_excel(writer, sheet_name="Tồn kho theo Order", index=False)

# ==============================
# 3. PHÂN LOẠI TRẠNG THÁI
# ==============================
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

    # lấy phạm vi hiệu lực LSX toàn cục
    lsx_start, lsx_end = get_lsx_range_from_file(lsx_path)

    # merge dữ liệu
    df_report = df_daily.merge(df_req, on="Order", how="left")
    df_report = df_report.merge(total_actual, on="Order", how="left")

    # check phạm vi hiệu lực LSX
    if lsx_start and lsx_end:
        df_report["Trong_khoang_LSX"] = (
            (df_report["Ngày"] >= lsx_start) &
            (df_report["Ngày"] <= lsx_end)
        )
    else:
        df_report["Trong_khoang_LSX"] = False

    # 🔴 lọc dữ liệu: giữ lại trong khoảng LSX hoặc chưa có LSX (NaN)
    df_report = df_report[
        df_report["Trong_khoang_LSX"] | df_report["SL trung bình/ngày"].isna()
    ]

    # phân loại trạng thái
    df_report["Trạng thái"] = df_report.apply(classify, axis=1)

    return df_report
# ==============================
# 5. CHẠY CHÍNH
# ==============================
# if __name__ == "__main__":
#     generate_report(
#         lsx_path="05.07.2025 LSX XC NM.HRC1 (ok).xlsx",
#         actual_path="ZBC04B_EXPORT_20250805_133956.xlsx"
#     )

# if __name__ == "__main__":
#     xu_ly_ton_kho(
#     file_sanluong="EXPORT_20250812_085402.xlsx",
#     file_kho="ZPP04.xlsx",
#     output_path="ket_qua_ton_kho_theo_order.xlsx"
#     )
#     generate_report(
#         lsx_path="02.08.2025 LSX XC NM.HRC1 1.xlsx",
#         actual_path="EXPORT_20250812_085402.xlsx"
#     )
