docker exec -it namenode /scripts/setup.sh
docker exec -it spark-master bash -c "spark/bin/spark-submit /data/pretransform_data.py"

echo Querying batch data...

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q1.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q2.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q3.py"

# docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2. 12:8.6.0 /batch-queries/q4.py"

docker exec -it spark-master bash -c "/spark/bin/spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.6.0 /batch-queries/q5.py"


sleep 2