/Users/ylu/spark/bin/spark-submit \
    --class quegel.bfs \
    --master local \
    target/scala-2.10/quegel-project_2.10-1.0.jar \
    hdfs://localhost:9000/ug.txt \
    hdfs://localhost:9000/result
