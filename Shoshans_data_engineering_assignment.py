import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, last, avg, stddev
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import logging
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

try:
    args = getResolvedOptions(sys.argv, ['INPUT_PATH', 'OUTPUT_PATH'])
except Exception as e:
    logging.error(f"Error resolving arguments: {str(e)}", exc_info=True)
    sys.exit(1)

input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def save_to_s3(df, path):
    try:
        df.write.mode("overwrite").csv(path)
        logging.info(f"Data successfully written to {path}")
    except Exception as e:
        logging.error(f"Error writing data to {path}: {str(e)}", exc_info=True)

def check_s3_path_exists(path):
    s3 = boto3.client('s3')
    try:
        bucket, key = path.replace("s3://", "").split("/", 1)
        response = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        if 'Contents' in response:
            logging.info(f"S3 path {path} exists and is accessible.")
        else:
            logging.warning(f"S3 path {path} does not exist or is empty.")
    except NoCredentialsError:
        logging.error("No AWS credentials found.")
    except PartialCredentialsError:
        logging.error("Incomplete AWS credentials found.")
    except Exception as e:
        logging.error(f"Error accessing S3 path {path}: {str(e)}", exc_info=True)

def create_athena_table(database, table_name, s3_path):
    athena_client = boto3.client('athena')
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
        `Date` STRING,
        `ticker` STRING,
        `close_filled` DOUBLE,
        `volume_filled` BIGINT,
        `prev_close` DOUBLE,
        `daily_return` DOUBLE,
        `worth` DOUBLE,
        `standard_deviation` DOUBLE,
        `30_day_return` DOUBLE
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'serialization.format' = ',',
        'field.delim' = ','
    )
    LOCATION '{s3_path}'
    TBLPROPERTIES ('has_encrypted_data'='false')
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': 's3://data-engineer-assignment-shoshan/query-results/'}
        )
        logging.info(f"Athena table {table_name} creation query started.")
    except Exception as e:
        logging.error(f"Error creating Athena table {table_name}: {str(e)}", exc_info=True)

def main():
    try:
        logging.info("Starting PySpark stock data analysis...")

        logging.info(f"Input path: {input_path}")
        logging.info(f"Output path: {output_path}")

        logging.info(f"Attempting to read CSV file from path: {input_path}")
        try:
            df_stocks_data = spark.read.csv(input_path, header=True, inferSchema=True)
            logging.info(f"Successfully loaded data from {input_path}")
        except Exception as e:
            logging.error(f"Error loading data from {input_path}: {str(e)}", exc_info=True)
            return

        logging.info(f"Stock data schema: {df_stocks_data.schema}")
        logging.info(f"Number of rows in stock data: {df_stocks_data.count()}")

        windowSpec = Window.partitionBy("ticker").orderBy("Date")

        logging.info("Handling missing 'close' values by forward-filling...")
        df_filled_close = df_stocks_data.withColumn("close_filled", last("close", True).over(windowSpec))
        logging.info(f"Number of rows after forward-filling 'close' values: {df_filled_close.count()}")

        logging.info("Handling missing 'volume' values by filling with 0...")
        df_filled_close_volume = df_filled_close.withColumn("volume_filled", col("volume")).fillna({"volume_filled": 0})
        logging.info(f"Number of rows after filling 'volume' values: {df_filled_close_volume.count()}")

        logging.info("Calculating average daily return...")
        df_filled_close_volume = df_filled_close_volume.withColumn("prev_close", lag("close_filled").over(windowSpec))
        df_filled_close_volume = df_filled_close_volume.withColumn("daily_return",
            (col("close_filled") - col("prev_close")) / col("prev_close") * 100)
        df_avg_return = df_filled_close_volume.groupBy("Date").agg(avg("daily_return").alias("average_return"))
        logging.info(f"Number of rows in average daily return: {df_avg_return.count()}")

        logging.info("Saving average return results to S3...")
        save_to_s3(df_avg_return, f"s3://data-engineer-assignment-shoshan/output/average_return")
        check_s3_path_exists(f"s3://data-engineer-assignment-shoshan/output/average_return")

        logging.info("Calculating highest worth...")
        df_filled_close_volume = df_filled_close_volume.withColumn("worth", col("close_filled") * col("volume_filled"))
        df_highest_worth = df_filled_close_volume.groupBy("ticker").agg(avg("worth").alias("value")).orderBy(col("value").desc()).limit(1)
        logging.info(f"Highest worth stock: {df_highest_worth.collect()}")

        logging.info("Saving highest worth result to S3...")
        save_to_s3(df_highest_worth, f"s3://data-engineer-assignment-shoshan/output/highest_worth_stock")
        check_s3_path_exists(f"s3://data-engineer-assignment-shoshan/output/highest_worth_stock")

        logging.info("Calculating stock volatility...")
        df_volatility = df_filled_close_volume.groupBy("ticker").agg(stddev("daily_return").alias("standard_deviation")).orderBy(col("standard_deviation").desc()).limit(1)
        logging.info(f"Most volatile stock: {df_volatility.collect()}")

        logging.info("Saving volatility result to S3...")
        save_to_s3(df_volatility, f"s3://data-engineer-assignment-shoshan/output/most_volatile_stock")
        check_s3_path_exists(f"s3://data-engineer-assignment-shoshan/output/most_volatile_stock")

        logging.info("Calculating top 30-day returns...")
        df_filled_close_volume = df_filled_close_volume.withColumn("30_day_return",
            (col("close_filled") - lag("close_filled", 30).over(windowSpec)) / lag("close_filled", 30).over(windowSpec) * 100)

        df_top_30_day = df_filled_close_volume.orderBy(col("30_day_return").desc()).select("ticker", "Date", "30_day_return").limit(3)
        logging.info(f"Top 30-day returns: {df_top_30_day.collect()}")

        logging.info("Saving top 30-day return results to S3...")
        save_to_s3(df_top_30_day, f"s3://data-engineer-assignment-shoshan/output/top_30_day_returns")
        check_s3_path_exists(f"s3://data-engineer-assignment-shoshan/output/top_30_day_returns")

        # Create Athena tables
        create_athena_table("shoshan_glue_database", "average_return", "s3://data-engineer-assignment-shoshan/output/average_return")
        create_athena_table("shoshan_glue_database", "highest_worth_stock", "s3://data-engineer-assignment-shoshan/output/highest_worth_stock")
        create_athena_table("shoshan_glue_database", "most_volatile_stock", "s3://data-engineer-assignment-shoshan/output/most_volatile_stock")
        create_athena_table("shoshan_glue_database", "top_30_day_returns", "s3://data-engineer-assignment-shoshan/output/top_30_day_returns")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
