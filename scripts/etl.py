from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_timestamp
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("RetailETL") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .getOrCreate()

def run_etl():
    spark = create_spark_session()

    df = spark.read.csv(
        "data/retail_purchase_log.csv",
        header=True,
        inferSchema=True
    )

    df = df.dropDuplicates()
    df = df.dropna(subset=["transaction_id", "customer_id", "product_id", "quantity", "price"])
    df = df.withColumn("purchase_date", to_timestamp("purchase_date"))
    df = df.withColumn("total_amount", expr("quantity * price"))

    # Save output via pandas fallback (due to NativeIO issues on Windows)
    df_pd = df.toPandas()
    os.makedirs("output", exist_ok=True)
    df_pd.to_csv("output/retail_purchase_cleaned.csv", index=False)

    spark.stop()

if __name__ == "__main__":
    run_etl()
