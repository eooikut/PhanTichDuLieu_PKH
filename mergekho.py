import pandas as pd
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
    summary["Tiền đổi"]="Pending"
    return summary
FILE_SANLUONG = "EXPORT_20250812_085402.xlsx"
FILE_KHO = "ZPP04.xlsx"
data = xu_ly_ton_kho(FILE_SANLUONG, FILE_KHO)
data.to_excel("sumary.xlsx",index=False)