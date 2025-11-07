# Phân tích Dữ liệu Khảo sát Sức khỏe BRFSS 2024

Dự án này thực hiện quy trình ETL (Extract, Transform, Load) và phân tích dữ liệu từ bộ khảo sát **Behavioral Risk Factor Surveillance System (BRFSS) 2024** do CDC Hoa Kỳ cung cấp.

Mục tiêu là Nâng cao nhận thức về các yếu tố ảnh hưởng đến nguy cơ mắc bệnh tim mạch.

## 📊 Nguồn dữ liệu

* **Tên:** 2024 BRFSS Survey Data and Documentation
* **Tổ chức:** Centers for Disease Control and Prevention (CDC)
* **Link:** [https://www.cdc.gov/brfss/annual_data/annual_2024.html](https://www.cdc.gov/brfss/annual_data/annual_2024.html)

## 🛠️ Công nghệ sử dụng

* **Ngôn ngữ xử lý dữ liệu:** Python (Pandas, NumPy)
* **Cơ sở dữ liệu (Data Warehouse):** Microsoft SQL Server (MSSQL)
* **Trực quan hóa & Báo cáo:** Power BI

## ⚙️ Quy trình dự án (Project Workflow)

Dự án này tuân theo một quy trình BI tiêu chuẩn:

1.  **Extract (Trích xuất):** Dữ liệu thô (định dạng `.ASC`) từ trang web của CDC được tải về.
2.  **Transform (Biến đổi):** Một script `asc_to_csv.py` được sử dụng để đọc dữ liệu, xử lý các giá trị thiếu (NULLs), chuẩn hóa các mã biến (theo codebook của CDC), và chọn lọc các cột quan trọng cho phân tích.
3.  **Load (Tải):** Dữ liệu đã được làm sạch và biến đổi được tải vào một cơ sở dữ liệu MSSQL. [MÔ TẢ CẤU TRÚC DB. Ví dụ: "Dữ liệu được lưu trữ trong một bảng Fact chính và một số bảng Dimension liên quan (nếu có)."]
4.  **Visualize (Trực quan hóa):** Power BI kết nối trực tiếp với cơ sở dữ liệu MSSQL (sử dụng DirectQuery hoặc Import Mode) để xây dựng các báo cáo và dashboard tương tác.



## 🚀 Cài đặt và Sử dụng

### Yêu cầu hệ thống

* Python 3.9+
* Microsoft SQL Server (phiên bản 2019+)
* Power BI Desktop

### 1. Cài đặt Cơ sở dữ liệu

1.  Mở SQL Server Management Studio (SSMS).
2.  Chạy file `Database/schema.sql` để tạo cấu trúc bảng cần thiết.
3.  [NÊU CÁC BƯỚC KHÁC NẾU CÓ, ví dụ: "Chạy `Database/stored_procedures.sql` để tạo các Stored Procedure dùng cho việc chèn dữ liệu."]

### 2. Cài đặt Môi trường Python

1.  Clone repository này:
    ```bash
    git clone [LINK_GITHUB_CUA_BAN]
    cd [TEN_THU_MUC_DU_AN]
    ```
2.  Tạo môi trường ảo (khuyến nghị):
    ```bash
    python -m venv venv
    source venv/bin/activate  # Trên Windows là `venv\Scripts\activate`
    ```
3.  Cài đặt các thư viện cần thiết:
    ```bash
    pip install -r requirements.txt
    ```
    *(Lưu ý: Bạn cần tạo file `requirements.txt` bằng cách chạy `pip freeze > requirements.txt`)*

### 3. Cấu hình

Cập nhật chuỗi kết nối (connection string) tới MSSQL của bạn trong file `config.ini` hoặc `[TEN_FILE_CONFIG].py`.

### 4. Chạy quy trình ETL

Thực thi script Python chính để bắt đầu quá trình ETL:
```bash
python etl_main.py