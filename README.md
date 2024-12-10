# LƯU TRỮ, XỬ LÝ VÀ PHÂN TÍCH DỮ LIỆU PHIM LẺ

## MÔ TẢ DỰ ÁN  
Dự án này thực hiện quy trình thu thập, lưu trữ, xử lý và phân tích dữ liệu phim lẻ thông qua các bước:  
1. **THU THẬP DỮ LIỆU**: Crawling dữ liệu phim từ nguồn đầu vào.  
2. **LƯU TRỮ DỮ LIỆU**: Upload dữ liệu thu thập vào HDFS để đảm bảo tính phân tán và khả năng mở rộng.  
3. **XỬ LÝ DỮ LIỆU**: Dùng Spark để làm sạch và xử lý dữ liệu.  
4. **LƯU TRỮ KẾT QUẢ**: Upload dữ liệu đã xử lý lên ElasticSearch.  
5. **PHÂN TÍCH DỮ LIỆU**: Sử dụng Kibana để phân tích và trực quan hóa dữ liệu phim.

Dự án sử dụng Docker để triển khai hệ thống phân tán, đảm bảo khả năng mở rộng và triển khai linh hoạt.

## CÔNG NGHỆ SỬ DỤNG  
- **HDFS**: Lưu trữ dữ liệu phân tán.  
- **SPARK**: Xử lý dữ liệu lớn.  
- **ELASTICSEARCH**: Lưu trữ và truy vấn dữ liệu đã xử lý.  
- **KIBANA**: Trực quan hóa dữ liệu.  
- **DOCKER**: Triển khai hệ thống phân tán.

## LINK DRIVE  
[TẢI VỀ TÀI LIỆU VÀ MÃ NGUỒN](https://drive.google.com/drive/folders/1UzXoWcJn_hFo27GbgWOvTijv_Wo-6Nra?usp=sharing)  
