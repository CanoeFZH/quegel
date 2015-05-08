export YARN_CONF_DIR=/etc/hadoop/conf
export SPARK_CLASSPATH=/usr/lib/hadoop/lib
export SPARK_LIBRARY_PATH=/usr/lib/hadoop/lib/native

/home/dp/spark/spark-1.3.1/spark-1.3.1/bin/spark-submit \
  --class bfs \
  --master yarn-cluster \
  --executor-memory 7G \
  --num-executors 200 \
  --executor-cores 2 \
  --jars /usr/lib/hadoop/lib/hadoop-lzo-0.4.15-cdh5.1.0.jar \
  graphRun.jar \
  "hdfs://dmmaster1.et2/tmp/fanzhihang/graph/adjGraph"  \
  "hdfs://dmmaster1.et2/tmp/fanzhihang/graph/output"  \
  "bfs" \
