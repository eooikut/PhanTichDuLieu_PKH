# import pandas as pd

# # Đọc file
# df = pd.read_excel("ZPP04.xlsx")
# start_date = "2025-08-01"
# end_date   = "2025-08-14"

# # Lọc dữ liệu trong khoảng
# df_filtered = df[(df['Ngày sản xuất'] >= start_date) & (df['Ngày sản xuất'] <= end_date)]

# # Tạo mapping Order - SO, bỏ trùng
# mapping = df_filtered[['Order', 'Customer M']].drop_duplicates()
# mapping["Tiến độ"]=0
# # Xuất ra file Excel
# mapping.to_excel("mapping_order_so2.xlsx", index=False)

from flask import Flask, render_template
import pandas as pd

app = Flask(__name__)

def process_mapping_data():
    # Đọc file Excel
    try:
        df = pd.read_excel('mapping_order_so.xlsx', sheet_name='Sheet1')
    except:
        # Tạo dữ liệu mẫu nếu không có file
        data = {
            'Order': [1, 1, 1, 1, 1, 2, 2, 2],
            'SO': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'],
            'Tiền đổi': ['100%', '80%', None, None, '60%', '40%', None, None]
        }
        df = pd.DataFrame(data)
    
    # Làm sạch dữ liệu
    df = df.dropna(subset=['Order', 'SO Mapping'])
    df['Order'] = df['Order'].astype(int)
    
    # Nhóm dữ liệu theo Order
    result = []
    for order, group in df.groupby('Order'):
        so_list = group['SO Mapping'].tolist()
        tiendoi_list = group['Tiền đổi'].tolist()
        result.append({
            'Order': order,
            'SO_list': so_list,
            'Tiendoi_list': tiendoi_list
        })
    
    return result

@app.route('/')
def index():
    data = process_mapping_data()
    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)