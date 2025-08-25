from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
import json
from uuid import uuid4
from werkzeug.utils import secure_filename
from processordata import  generate_report, load_data
from datetime import datetime
from openpyxl import load_workbook
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
        return render_template("xem_theo_ngay.html", has_data=False)

    # Lấy lsx_id từ request (nếu không có thì mặc định ALL)
    selected_lsx = request.args.get("lsx_id", "ALL")
    selected_date = request.args.get("date")

    # Chuẩn bị DataFrame theo LSX
    dfs = []
    if selected_lsx == "ALL":
        for entry in metadata:
            df = generate_report(entry["lsx"], entry["sanluong"])
            df["lsx_id"] = entry["id"]  # thêm cột để biết thuộc LSX nào
            dfs.append(df)
        df_all = pd.concat(dfs, ignore_index=True)
    else:
        entry = get_lsx_by_id(selected_lsx)
        if not entry:
            return render_template("xem_theo_ngay.html", has_data=False)
        df_all = generate_report(entry["lsx"], entry["sanluong"])
        df_all["lsx_id"] = entry["id"]

    # Bắt đầu xử lý lọc ngày
    orders = []
    total_loss = 0
    seen_orders = set()

    if selected_date:
        df_all['Ngày'] = df_all['Ngày'].astype(str)
        filtered = df_all[df_all['Ngày'] == selected_date]

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
                        "Trạng thái": trang_thai,
                        "LSX": row.get("lsx_id", "")
                    })

    return render_template(
        "xem_theo_ngay.html",
        selected_date=selected_date,
        selected_lsx=selected_lsx,
        lsx_list=[{"id": "ALL", "name": "ALL LSX"}] + [
            {"id": entry["id"], "name": entry.get("name", entry["id"])}
            for entry in metadata
        ],
        orders=orders,
        total_loss=total_loss,
        has_data=True,
        uuid=str(uuid4())
    )

# === Tiến độ tổng quan ===
@app.route("/tiendo_order")
def tien_do_order():
    metadata = load_metadata()
    latest = metadata[-1]
    grouped = load_data(latest["sanluong"], latest["kho"],latest["so"])
    return render_template('tiendo_order.html', grouped=grouped)

# === Upload bộ dữ liệu mới ===
@app.route("/upload", methods=["GET", "POST"])
def upload_files():
    if request.method == "POST":
        lsx_file = request.files.get("lsx_file")
        sl_file = request.files.get("sl_file")
        kho_file = request.files.get("kho_file")
        so_file = request.files.get("so_file")  
       
        if not (lsx_file and lsx_file.filename and 
        sl_file and sl_file.filename and
        kho_file and kho_file.filename and
        so_file and so_file.filename):
            return "Thiếu file upload!", 400

        # 🔹 Tạo thư mục theo ID
        lsx_id = str(uuid4())
        folder = os.path.join(app.config["UPLOAD_FOLDER"], lsx_id)
        os.makedirs(folder, exist_ok=True)

        # 🔹 Lưu file
        lsx_path = os.path.join(folder, "lsx.xlsx").replace("\\", "/")
        sl_path = os.path.join(folder, "sanluong.xlsx").replace("\\", "/")
        kho_path = os.path.join(folder, "kho.xlsx").replace("\\", "/")
        so_path = os.path.join(folder, "so.xlsx").replace("\\", "/")

        lsx_file.save(lsx_path)
        sl_file.save(sl_path)
        kho_file.save(kho_path)
        so_file.save(so_path)

            # 🔹 Lấy tên LSX từ dòng 3 của file LSX
        try:
        # đọc sheet thứ 3 (index=2 vì python bắt đầu từ 0)
            df_tmp = load_workbook(lsx_path, data_only=True)
            df_tm= df_tmp.worksheets[3]
        # lấy dòng 3 (index=2) trong sheet 3
            name = df_tm["B3"].value
            if not name:
                name = f"LSX_{lsx_id}"
        except Exception as e:
                name = f"LSX_{lsx_id}" 

        # 🔹 Lưu metadata
        metadata = load_metadata()
        metadata.append({
            "id": lsx_id,
            "name": name,
            "lsx": lsx_path,
            "sanluong": sl_path,
            "kho": kho_path,
            "so": so_path,
            "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        save_metadata(metadata)

        # 🔹 Tạo báo cáo và lưu log
        generate_report(lsx_path, sl_path, lsx_name_override=name)

        return redirect(url_for("xem_theo_ngay"))

    return render_template("upload.html")

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
    app.run(host="0.0.0.0", port=5001, debug=True)
