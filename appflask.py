from flask import Flask, render_template, request
import pandas as pd
from uuid import uuid4
from final import xu_ly_ton_kho

app = Flask(__name__)

# File dữ liệu
df = pd.read_excel("bao_cao_san_luong_theo_ngay123.xlsx")
FILE_SANLUONG = "EXPORT_20250812_085402.xlsx"
FILE_KHO = "ZPP04.xlsx"

@app.route("/", methods=["GET"])
def xem_theo_ngay():
    selected_date = request.args.get("date")
    orders = []
    total_loss = 0
    seen_orders = set()  # tránh trùng Order

    if selected_date:
        df['Ngày'] = df['Ngày'].astype(str)
        filtered = df[df['Ngày'] == selected_date]

        if not filtered.empty:
            for _, row in filtered.iterrows():
                order_id = row['Order']
                if order_id not in seen_orders:
                    seen_orders.add(order_id)
                    sl_tb = row['SL trung bình/ngày']
                    sl_tt = row['Sản lượng thực tế']

                    if sl_tt < sl_tb:
                        trang_thai = f"Thất thoát {sl_tb - sl_tt:,.0f} kg"
                        total_loss += sl_tb - sl_tt
                    elif sl_tt > sl_tb:
                        trang_thai = "Vượt tiến độ"
                    else:
                        trang_thai = "Đúng tiến độ"

                    orders.append({
                        "Order": order_id,
                        "SL trung bình/ngày": sl_tb,
                        "Sản lượng thực tế": sl_tt,
                        "Trạng thái": trang_thai
                    })

    return render_template(
        "xem_theo_ngay.html",
        selected_date=selected_date,
        orders=orders,
        total_loss=total_loss,
        uuid=str(uuid4())  # chống cache
    )

@app.route("/tiendo_order")
def tien_do_order():
    data = xu_ly_ton_kho(FILE_SANLUONG, FILE_KHO)
    table_html = data.to_html(classes="table table-striped", index=False)

    return render_template(
        "tiendo_order.html",
        table_html=table_html
    )
@app.route('/test')
def index():
    # Đọc file Excel mapping
    df = pd.read_excel("mapping.xlsx")
    
    # Chuyển sang list dict để render
    data = df.to_dict(orient="records")
    
    return render_template("index.html", data=data)

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5001, debug=True)

