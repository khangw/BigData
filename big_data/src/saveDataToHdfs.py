from hdfs import InsecureClient
import os

# Cấu hình HDFS
hdfs_url = 'http://localhost:9870'  # Thường cổng HDFS Web UI là 50070 hoặc 9870
client = InsecureClient(hdfs_url, user='hdfs')  # Thay 'hdfs' bằng user bạn đang sử dụng

# Đường dẫn cục bộ và trên HDFS
local_json_dir = '../Data'  # Thư mục chứa file JSON trên máy cục bộ
hdfs_dir = '/data_crawl_test'    # Thư mục trên HDFS

# Kiểm tra kết nối HDFS
try:
    # Tạo thư mục trên HDFS nếu chưa tồn tại
    if not client.status(hdfs_dir, strict=False):
        client.makedirs(hdfs_dir)
        print(f"Đã tạo thư mục {hdfs_dir} trên HDFS.")
except Exception as e:
    print(f"Không thể kết nối HDFS hoặc tạo thư mục: {e}")
    exit(1)

# Duyệt qua các tệp JSON và tải lên HDFS
try:
    for json_file in os.listdir(local_json_dir):
        if json_file.endswith('.json'):
            local_file_path = os.path.join(local_json_dir, json_file)
            hdfs_file_path = os.path.join(hdfs_dir, json_file)

            # Ghi tệp lên HDFS
            with open(local_file_path, 'rb') as local_file:
                client.write(hdfs_file_path, local_file)

            print(f"Tải tệp {json_file} lên HDFS thành công!")
except Exception as e:
    print(f"Đã xảy ra lỗi trong quá trình tải tệp: {e}")
