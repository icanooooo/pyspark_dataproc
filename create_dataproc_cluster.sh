gcloud dataproc clusters create amazon-sales-cluster --region=asia-southeast2  --image-version=1.5-debian10 --properties=spark:spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.0