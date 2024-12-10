# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re, unicodedata
import patterns
import math

# Data Film
@udf(returnType=IntegerType())
def extract_type(type):
    return 1 if type == "Phim bộ" else 0

@udf(returnType=IntegerType())
def extract_release_year(nam_phat_hanh):
    match = re.search(r"\d{4}", nam_phat_hanh)
    if match:
        return int(match.group())
    return None  # Nếu không tìm thấy năm, trả về None


@udf(returnType=IntegerType())
def extract_status(trang_thai, so_tap):
    # Kiểm tra xem trang_thai có giá trị hợp lệ
    if trang_thai:
        match = re.search(r"\d+", trang_thai)
        if match:
            int_status = int(match.group())

            # Kiểm tra xem so_tap có giá trị hợp lệ
            if so_tap is not None and so_tap != '':
                try:
                    so_tap_int = int(so_tap)  # Chuyển so_tap thành int nếu có thể
                except ValueError:
                    so_tap_int = 0  # Hoặc một giá trị mặc định nếu không thể chuyển đổi
            else:
                so_tap_int = 0  # Nếu so_tap là None hoặc rỗng, đặt giá trị mặc định là 0

            if int_status < so_tap_int:
                return int_status
    return None
@udf(returnType=IntegerType())
def extract_episode_count(so_tap):
    # Kiểm tra xem so_tap có phải là chuỗi hay không
    if isinstance(so_tap, str):
        match = re.search(r"\d+", so_tap)
        if match:
            return int(match.group())
    return None  # Nếu không tìm thấy số hoặc so_tap không phải chuỗi, trả về None

@udf(returnType=StringType())
def map_condition(tinh_trang):
    return patterns.status_film.get(tinh_trang, "Unknown")  # Trả về "Unknown" nếu trạng thái không xác định

@udf(returnType=ArrayType(StringType()))
def extract_genres(the_loai):
    return [genre.strip() for genre in the_loai.split(",")]

@udf(returnType=StringType())
def map_director(dao_dien):
    if dao_dien == "Đang cập nhật":
        return "Updating"
    return dao_dien.strip()

@udf(returnType=ArrayType(StringType()))
def extract_actors(dien_vien):
    if dien_vien == "Đang cập nhật":
        return "Updating"
    return [actor.strip() for actor in dien_vien.split(",")]

@udf(returnType=FloatType())
def extract_rating(danh_gia):
    try:
        return float(danh_gia)
    except ValueError:
        return None  # Trả về None nếu không thể chuyển đổi được