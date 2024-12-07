from hdfs import InsecureClient
import os

hdfs_url = 'http://172.19.0.3:9870'
client = InsecureClient(hdfs_url)

# Đường dẫn đến thư mục chứa các tệp JSON trên hệ thống cục bộ
local_json_dir = '../Data'
# Đường dẫn đến thư mục trên HDFS nơi bạn muốn lưu các tệp JSON
hdfs_dir = '/data_film_crawl'

# Đảm bảo thư mục trên HDFS đã tồn tại, nếu chưa thì tạo nó
if not client.status(hdfs_dir, strict=False):
    client.makedirs(hdfs_dir)

# Duyệt qua tất cả các tệp JSON trong thư mục cục bộ và tải lên HDFS
for json_file in os.listdir(local_json_dir):
    if json_file.endswith('.json'):
        print(json_file)
        local_file_path = os.path.join(local_json_dir, json_file)
        hdfs_file_path = os.path.join(hdfs_dir, json_file)

        # Tải tệp lên HDFS
        with open(local_file_path, 'rb') as local_file:
            client.write(hdfs_file_path, local_file)

        print(f"Tải tệp {json_file} lên HDFS thành công!")

