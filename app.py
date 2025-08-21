from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
import json
from uuid import uuid4
from werkzeug.utils import secure_filename
from processordata import xu_ly_ton_kho, generate_report

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
METADATA_FILE = "metadata.json"

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# === Utils ===
def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_metadata(data):
    with open(METADATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_lsx_by_id(lsx_id):
    metadata = load_metadata()
    return next((d for d in metadata if d["id"] == lsx_id), None)

# === Trang chủ: Hiển thị tiến độ theo ngày ===
@app.route("/", methods=["GET"])
def xem_theo_ngay():
    metadata = load_metadata()
    if not metadata:
        return render_template("xem_theo_ngay.html",has_data=False)

    latest = metadata[-1]
    df = generate_report(latest["lsx"], latest["sanluong"])

    selected_date = request.args.get("date")
    orders = []
    total_loss = 0
    seen_orders = set()

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

                    if pd.isna(sl_tb):
                        trang_thai = "Chờ LSX mới"
                    elif sl_tt < sl_tb:
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
         has_data=True,
        uuid=str(uuid4())
    )

# === Tiến độ tổng quan ===
@app.route("/tiendo_order")
def tien_do_order():
    metadata = load_metadata()
    if not metadata:
        return render_template("no_data.html")

    latest = metadata[-1]
    df = xu_ly_ton_kho(latest["sanluong"], latest["kho"])
    table_html = df.to_html(classes="table table-striped", index=False)

    return render_template("tiendo_order.html", table_html=table_html)

# === Upload bộ dữ liệu mới ===
@app.route("/upload", methods=["GET", "POST"])
def upload_files():
    if request.method == "POST":
        lsx_file = request.files.get("lsx_file")
        sl_file = request.files.get("sl_file")
        kho_file = request.files.get("kho_file")
        name = request.form.get("lsx_name")

        if not all([lsx_file, sl_file, kho_file, name]):
            return "Thiếu thông tin upload!", 400

        lsx_id = str(uuid4())
        folder = os.path.join(app.config["UPLOAD_FOLDER"], lsx_id)
        os.makedirs(folder, exist_ok=True)

        lsx_path = os.path.join(folder, "lsx.xlsx").replace("\\", "/")
        sl_path = os.path.join(folder, "sanluong.xlsx").replace("\\", "/")
        kho_path = os.path.join(folder, "kho.xlsx").replace("\\", "/")

        lsx_file.save(lsx_path)
        sl_file.save(sl_path)
        kho_file.save(kho_path)

        metadata = load_metadata()
        metadata.append({
            "id": lsx_id,
            "name": name,
            "lsx": lsx_path,
            "sanluong": sl_path,
            "kho": kho_path
        })
        save_metadata(metadata)

        return redirect(url_for("xem_theo_ngay"))

    return render_template("upload.html")

# === Chọn LSX đã upload để xem lại ===
@app.route("/chon_lsx", methods=["GET"])
def select_lsx():
    metadata = load_metadata()
    return render_template("select_lsx.html", datasets=metadata)

# === Xem một LSX cụ thể ===
@app.route("/xem/<lsx_id>")
def xem_lsx(lsx_id):
    selected = get_lsx_by_id(lsx_id)
    if not selected:
        return "Không tìm thấy LSX", 404

    df = generate_report(selected["lsx"], selected["sanluong"])
    data = df.to_dict(orient="records")

    return render_template("ket_qua_upload.html", data=data, name=selected["name"])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
