from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from configparser import ConfigParser
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from pyspark.sql.functions import max


# define a function to create spark session
def create_spark_session():
    spark = SparkSession.builder.appName("IncrementalDataLoad").getOrCreate()
    return spark


# Define a function to read Parquet files and create a DataFrame
def read_parquet(spark, parquet_path):
    return spark.read.parquet(parquet_path)


def load_data_from_db(spark, table_names, properties, parquet_path):
    for i in table_names:
        db_df = spark.read.jdbc(url= properties["url"], table=i, properties=properties)

        #         Getting the latest date from dataframe
        latest_date = db_df.agg(max("created_date")).collect()[0][0]

        # filter data
        filtered_db_df = db_df.filter(db_df["created_date"] > latest_date)

        filtered_db_df.write.mode("append").parquet(f"{parquet_path}/{i}")


def main():
    spark = create_spark_session()
    config = ConfigParser()
    config_path = "C:/Users/swetkuma/PycharmProjects/pythonProject/config/config.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()
    config.read_string(content)

    properties = {
        "driver": config.get("Db_Connection", "Db_Connection.driver"),
        "user": config.get("Db_Connection", "Db_Connection.user"),
        "url": config.get("Db_Connection", "Db_Connection.url"),
        "password": config.get("Db_Connection", "Db_Connection.password"),
    }

    # # For customer tables
    # orders_parquet_path = config.get("parquet_path", "parquet_path.orders")
    # items_parquet_path = config.get("parquet_path", "parquet_path.items")
    # orders_details_parquet_path = config.get("parquet_path", "parquet_path.order_details")
    # salesperson_parquet_path = config.get("parquet_path", "parquet_path.salesperson")
    # ship_to_parquet_path = config.get("parquet_path", "parquet_path.ship_to")
    # customer_parquet_path = config.get("parquet_path", "parquet_path.customers")
    #
    # customer_df = read_parquet(spark, customer_parquet_path)
    # orders_df = read_parquet(spark, orders_parquet_path)
    # items_df = read_parquet(spark, items_parquet_path)
    # orders_details_df = read_parquet(spark, orders_details_parquet_path)
    # salesperson_df = read_parquet(spark, salesperson_parquet_path)
    # ship_to_df = read_parquet(spark, ship_to_parquet_path)
    #
    # customer_df.show()
    # orders_df.show()
    # items_df.show()
    # orders_details_df.show()
    # salesperson_df.show()
    # ship_to_df.show()

    table_names = ["customers", "orders", "items", "salesperson", "order_details", "ship_to"]
    output_path = config.get("output", "output.path")

    load_data_from_db(spark, table_names, properties, output_path)


if __name__ == '__main__':
    main()