# coding=utf-8
import pyspark
from cffi.model import StructType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from operator import add
import sys,os
from pyspark.sql.types import *

import patterns, udfs , queries, config, io_cluster


schema = StructType([
      StructField("name",StringType(),True),
      StructField("Năm phát hành",StringType(),True),
      StructField("Quốc gia",StringType(),True),
      StructField("Thể loại",StringType(),True),
      StructField("Đạo diễn",StringType(),True),
      StructField("Thời lượng",StringType(),True),
      StructField("Diễn viên",StringType(),True),
      StructField("Nội dụng",StringType(),True),
  ])

if __name__ == "__main__":
    
    APP_NAME="PreprocessData"
    
    app_config = config.Config(elasticsearch_host="localhost",
                               elasticsearch_port="9200",
                               elasticsearch_input_json="yes",
                               elasticsearch_nodes_wan_only="true",
                               hdfs_namenode="hdfs://localhost:9000"
                               )
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext
    sc.addPyFile(os.path.dirname(__file__)+"/patterns.py")
    
    raw_recruit_df = spark.read.schema(schema).option("multiline","true").json("hdfs://localhost:9000/datasource/*.json")
    # raw_recruit_df.show(5)
    extracted_recruit_df=raw_recruit_df.select(raw_recruit_df["name"].alias("CompanyName"),
          udfs.extract_framework_plattform("Mô tả công việc","Yêu cầu ứng viên").alias("FrameworkPlattforms"),
          udfs.extract_language("Mô tả công việc","Yêu cầu ứng viên").alias("Languages"),
          udfs.extract_design_pattern("Mô tả công việc","Yêu cầu ứng viên").alias("DesignPatterns"),
          udfs.extract_knowledge("Mô tả công việc","Yêu cầu ứng viên").alias("Knowledges"),
          udfs.normalize_salary("Quyền lợi").alias("Salaries")
          )
    extracted_recruit_df.cache()
    extracted_recruit_df.show(5)

    ##========save extracted_recruit_df to hdfs========================
    df_to_hdfs=(extracted_recruit_df,)
    df_hdfs_name = ("extracted_recruit.json",)
    io_cluster.save_dataframes_to_hdfs("/extracteddata", app_config, df_to_hdfs, df_hdfs_name)

    ##========make some query==========================================
    knowledge_df = queries.get_counted_knowledge(extracted_recruit_df)
    knowledge_df.cache()
    knowledge_df.show(5)

    udfs.broadcast_labeled_knowledges(sc,patterns.labeled_knowledges)
    grouped_knowledge_df = queries.get_grouped_knowledge(knowledge_df)
    grouped_knowledge_df.cache()
    grouped_knowledge_df.show()

    #extracted_recruit_df = extracted_recruit_df.drop("Knowledges")
    #extracted_recruit_df.cache()

    ##========save some df to elasticsearch========================
    df_to_elasticsearch=(
                         extracted_recruit_df,
                        knowledge_df,
                         grouped_knowledge_df
                         )
    
    df_es_indices = (
                     "recruit",
                   "knowledges",
                     "grouped_knowledges"
                     )
    extracted_recruit_df.show(5)
    io_cluster.save_dataframes_to_elasticsearch(df_to_elasticsearch,df_es_indices,app_config.get_elasticsearch_conf())