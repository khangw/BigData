# coding=utf-8
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

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
        elasticsearch_host="localhost",
        elasticsearch_port="9200",
        elasticsearch_input_json="yes",
        elasticsearch_nodes_wan_only="true",
        hdfs_namenode="hdfs://namenode:9000"
    )
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext
    sc.addPyFile(os.path.dirname(__file__) + "/patterns.py")
    print("Connection success!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
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

    ##========save extracted_recruit_df to hdfs========================
    df_to_hdfs = (extracted_recruit_df,)
    df_hdfs_name = ("extracted_recruit.json",)
    io_cluster.save_dataframes_to_hdfs("/extracteddata", app_config, df_to_hdfs, df_hdfs_name)

    ##========make some query==========================================
    knowledge_df = queries.get_counted_knowledge(extracted_recruit_df)
    knowledge_df.cache()
    knowledge_df.show(5)

    udfs.broadcast_labeled_knowledges(sc, patterns.labeled_knowledges)
    grouped_knowledge_df = queries.get_grouped_knowledge(knowledge_df)
    grouped_knowledge_df.cache()
    grouped_knowledge_df.show()

    # extracted_recruit_df = extracted_recruit_df.drop("Knowledges")
    # extracted_recruit_df.cache()

    ##========save some df to elasticsearch========================
    # df_to_elasticsearch=(
    #                      extracted_recruit_df,
    #                     knowledge_df,
    #                      grouped_knowledge_df
    #                      )
    #
    # df_es_indices = (
    #                  "recruit",
    #                "knowledges",
    #                  "grouped_knowledges"
    #                  )
    # extracted_recruit_df.show(5)
    # io_cluster.save_dataframes_to_elasticsearch(df_to_elasticsearch,df_es_indices,app_config.get_elasticsearch_conf())
