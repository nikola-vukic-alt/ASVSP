docker exec -it namenode /scripts/setup.sh
docker exec -it spark-master bash -c "spark/bin/spark-submit /data/create_raw_zone.py"
docker exec -it spark-master bash -c "spark/bin/spark-submit /data/transform_batch_data.py"
# docker exec -it spark-master bash -c "spark/bin/spark-submit /data/transform_streaming_data.py"