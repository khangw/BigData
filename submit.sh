#!/bin/bash
export SPARK_MASTER_URL=spark://spark-master:7077
export SPARK_HOME=/spark
/wait-for-step.sh
/execute-step.sh
echo "Submit application ${SPARK_APPLICATION_PYTHON_LOCATION} to Spark master ${SPARK_MASTER_URL}"
echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
PYSPARK_PYTHON=python3  /spark/bin/spark-submit \
    --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2 \
    --master spark://spark-master:7077 \
    --conf "spark.driver.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
    --conf "spark.executor.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
    --num-executors 4 --executor-memory 2G \
    /app/main.py
