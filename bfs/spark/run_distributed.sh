/usr/local/spark/bin/spark-submit \
    --class quegel.bfs \
    --master spark://master:7077 \
    --executor-memory 45G \
    --total-executor-cores 345 \
    target/scala-2.10/quegel-project_2.10-1.0.jar \
    spark://master:7077 \
    hdfs://master:9000/ylu/btc.txt \
    hdfs://master:9000/ylu/output/btcbfs \
    bfs
