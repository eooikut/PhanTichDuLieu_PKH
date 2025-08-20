import pandas as pd

# Đọc dữ liệu
# df = pd.read_excel("ZBC04B_EXPORT_20250805_133956.xlsx", sheet_name="Data")
df = pd.read_excel("EXPORT_20250812_085402.xlsx", sheet_name="Data")
# Xóa dòng trống
df = df.dropna(how="all")
df = df[df['Đã nhập kho'] == 'No']
# Chuyển kiểu dữ liệu
df['Ngày sản xuất'] = pd.to_datetime(df['Ngày sản xuất'], errors='coerce').dt.date
df['Khối lượng'] = pd.to_numeric(df['Khối lượng'], errors='coerce')

# Xóa các dòng không có Order hoặc Ngày sản xuất
df = df.dropna(subset=['Order', 'Ngày sản xuất'])

# Gom nhóm dữ liệu theo Ngày - Ca - Order
df_grouped = df.groupby(['Ngày sản xuất','Order'], as_index=False).agg({
    'Khối lượng': 'sum'
})

# Đổi tên cột cho dễ hiểu
df_grouped = df_grouped.rename(columns={'Khối lượng': 'Sản lượng thực tế'})

# Xuất dữ liệu đã xử lý
df_grouped.to_excel("sanluong_thucte_processed1.xlsx", index=False)

print("Hoàn tất tiền xử lý, dữ liệu lưu tại ")

