# coding=utf-8
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from operator import add
import sys, os
from pyspark.sql.types import *

import patterns, udfs, queries, config, io_cluster

schema = StructType([
    StructField("name", StringType(), True),
    StructField("link", StringType(), True),
    StructField("type", StringType(), True),
    StructField("Năm phát hành", StringType(), True),
    StructField("Trạng thái", StringType(), True),
    StructField("Số tập", StringType(), True),
    StructField("Tình trạng", StringType(), True),
    StructField("Thể loại", StringType(), True),
    StructField("Đạo diễn", StringType(), True),
    StructField("Diễn viên", StringType(), True),
    StructField("Đánh giá", StringType(), True)
])

if __name__ == "__main__":
    APP_NAME = "PreprocessData"

    app_config = config.Config(
        elasticsearch_host="elasticsearch",
        elasticsearch_port="9200",
        elasticsearch_input_json="yes",
        elasticsearch_nodes_wan_only="true",
        hdfs_namenode="hdfs://namenode:9000"
    )
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext
    sc.addPyFile(os.path.dirname(__file__) + "/patterns.py")
    print("Connection success!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    spark.catalog.clearCache()
    raw_recruit_df = spark.read.schema(schema).option("multiline", "true").json(
        "hdfs://namenode:9000/data_test/*.json")
    # raw_recruit_df.show(5)
    extracted_recruit_df = raw_recruit_df.select(raw_recruit_df["name"].alias("FilmName"),
                                                 raw_recruit_df["link"].alias("LinkFilm"),
                                                 udfs.extract_type(raw_recruit_df["type"]).alias("IsMovies"),
                                                 udfs.extract_release_year(raw_recruit_df["Năm phát hành"]).alias("ReleaseYear"),
                                                 udfs.extract_status(raw_recruit_df["Trạng thái"], raw_recruit_df["Số tập"]).alias("Status"),
                                                 udfs.extract_episode_count(raw_recruit_df["Số tập"]).alias("EpisodeCount"),
                                                 udfs.map_condition(raw_recruit_df["Tình trạng"]).alias("Condition"),
                                                 udfs.extract_genres(raw_recruit_df["Thể loại"]).alias("Genres"),
                                                 udfs.map_director(raw_recruit_df["Đạo diễn"]).alias("Director"),
                                                 udfs.extract_actors(raw_recruit_df["Diễn viên"]).alias("Actors"),
                                                 udfs.extract_rating(raw_recruit_df["Đánh giá"]).alias("Rating")
                                                )
    extracted_recruit_df.cache()
    extracted_recruit_df.show(5)
    print("DONE_0")

    ##========save extracted_recruit_df to hdfs========================
    df_to_hdfs = (extracted_recruit_df,)
    df_hdfs_name = ("extracted_recruit.json",)
    io_cluster.save_dataframes_to_hdfs("/extracted_data", app_config, df_to_hdfs, df_hdfs_name)


 ## Thay bằng queries của nhóm
   # 1. Thống kê số lượng phim theo năm phát hành
    yearly_film_count_df = (
        extracted_recruit_df
        .groupBy("ReleaseYear")
        .agg(count("FilmName").alias("Số lượng phim"))
        .orderBy(desc("Số lượng phim"))
    )
    yearly_film_count_df.show(5)
    print("DONE_1")
    # 2. Thống kê số lượng phim theo thể loại
    genre_film_count_df = (
        extracted_recruit_df
        .withColumn("Genres", explode(col("Genres")))  # Dùng explode để chia mảng thành các dòng
        .groupBy("Genres")  # Nhóm theo từng thể loại
        .agg(count("FilmName").alias("Số lượng phim"))  # Đếm số lượng phim cho mỗi thể loại
        .orderBy(desc("Số lượng phim"))  # Sắp xếp theo số lượng phim giảm dần
    )
    genre_film_count_df.show(5)
    print("DONE_2")
    # 3. Thống kê số lượng phim bộ và phim lẻ
    type_film_count_df = (
        extracted_recruit_df
        .groupBy("IsMovies")
        .agg(count("FilmName").alias("Số lượng phim"))
        .orderBy(desc("Số lượng phim"))
    )
    type_film_count_df.show(5)

    # 4. Diễn viên đóng nhiều phim nhất
    actor_film_count_df = (
        extracted_recruit_df
        .withColumn("Actors", F.explode(F.col("Actors")))
        .filter(F.col("Actors") != "N/A")
        .groupBy("Actors")
        .agg(count("FilmName").alias("Số lượng phim"))
        .orderBy(desc("Số lượng phim"))
    )
    actor_film_count_df.show(5)

    # 5. Thống kê số lượng phim theo đánh giá (rating)
    rating_film_count_df = (
        extracted_recruit_df
        .groupBy("Rating")
        .agg(count("FilmName").alias("Số lượng phim"))
        .orderBy(desc("Số lượng phim"))
    )
    rating_film_count_df.show(5)

    # 6. Lưu các dataframe vào Elasticsearch
    df_to_elasticsearch = (
        extracted_recruit_df,
        yearly_film_count_df,
        genre_film_count_df,
        type_film_count_df,
        actor_film_count_df,
        rating_film_count_df
    )

    df_es_indices = (
        "recruit",
        "yearly_film_counts",
        "genre_film_counts",
        "type_film_counts",
        "actor_film_counts",
        "rating_film_counts"
    )

    # Lưu vào Elasticsearch
    io_cluster.save_dataframes_to_elasticsearch(df_to_elasticsearch, df_es_indices, app_config.get_elasticsearch_conf())

