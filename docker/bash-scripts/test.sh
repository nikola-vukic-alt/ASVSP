# docker exec -it namenode /scripts/setup.sh
# docker exec -it spark-master bash -c "spark/bin/spark-submit /data/pretransform_batch_data.py"
# docker exec -it spark-master bash -c "spark/bin/spark-submit /data/pretransform_streaming_data.py"

# echo Querying batch data...

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q1.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q2.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q3.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q4.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q5.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q6.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q7.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q8.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q9.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q10.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /streaming/queries/q1.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /streaming/queries/q2.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /streaming/queries/q3.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /streaming/queries/q4.py"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /streaming/queries/q5.py"

sleep 2