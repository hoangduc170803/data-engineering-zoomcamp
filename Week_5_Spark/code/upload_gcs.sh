# gsutil: Công cụ dòng lệnh để thao tác với Google Cloud Storage.
# -m (multi-threading): Dùng để tăng tốc độ sao chép bằng cách chạy nhiều tiến trình cùng lúc.
# cp: Lệnh copy dữ liệu.
# -r: Copy toàn bộ thư mục (recursive).
# pq/: Thư mục nguồn trên máy cục bộ (chứa dữ liệu cần upload).
# gs://dtc_data_lake_de-zoomcamp_ny-taxi/pq: Đường dẫn đến bucket đích trên GCS.
$ gsutil -m cp -r pq/ gs://dtc_data_lake_de-zoomcamp_ny-taxi/pq