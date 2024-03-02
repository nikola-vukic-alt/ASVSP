docker exec -it namenode /bash/setup.sh
docker exec -it spark-master bash -c "spark/bin/spark-submit /data/pretransform_data.py"