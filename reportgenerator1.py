from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from configparser import ConfigParser
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format


# define a function to create spark session
def create_spark_session():
    spark = SparkSession.builder.appName("ReportGenerator").getOrCreate()
    return spark


# Define a function to read Parquet files and create a DataFrame
def read_parquet(spark, parquet_path):
    return spark.read.parquet(parquet_path)


if __name__ == "__main__":
    config = ConfigParser()
    config_path = "C:/Users/swetkuma/PycharmProjects/pythonProject/config/config.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()

    config.read_string(content)

    spark = create_spark_session()
    properties = {
        "driver": config.get("Db_Connection", "Db_Connection.driver"),
        "user": config.get("Db_Connection", "Db_Connection.user"),
        "url": config.get("Db_Connection", "Db_Connection.url"),
        "password": config.get("Db_Connection", "Db_Connection.password"),
        "save_path": config.get("Db_Connection", "Db_Connection.base_path")
    }

    # For customer tables
    customer_parquet_path = config.get("parquet_path", "parquet_path.customers")
    orders_parquet_path = config.get("parquet_path", "parquet_path.orders")
    items_parquet_path = config.get("parquet_path", "parquet_path.items")
    orders_details_parquet_path = config.get("parquet_path", "parquet_path.order_details")
    salesperson_parquet_path = config.get("parquet_path", "parquet_path.salesperson")
    ship_to_parquet_path = config.get("parquet_path", "parquet_path.ship_to")

    customer_df = read_parquet(spark, customer_parquet_path)
    orders_df = read_parquet(spark, orders_parquet_path)
    items_df = read_parquet(spark, items_parquet_path)
    orders_details_df = read_parquet(spark, orders_details_parquet_path)
    salesperson_df = read_parquet(spark, salesperson_parquet_path)
    ship_to_df = read_parquet(spark, ship_to_parquet_path)

    customer_df.show()
    orders_df.show()
    items_df.show()
    orders_details_df.show()
    salesperson_df.show()
    ship_to_df.show()

    customer_df.createOrReplaceTempView("customers")
    orders_df.createOrReplaceTempView("orders")
    items_df.createOrReplaceTempView("items")
    orders_details_df.createOrReplaceTempView("order_details")
    salesperson_df.createOrReplaceTempView("salesperson")
    ship_to_df.createOrReplaceTempView("ship_to")

    customers = spark.sql('select * from customers')
    items = spark.sql('select * from items')
    order_details = spark.sql('select * from order_details')
    orders = spark.sql('select * from orders')
    # salesperson = spark.sql('select * from_salesperson')
    # ship_to = spark.sql('select * from ship_to')

    #--Monthly/Weekly Customer wise order count
    report_1 = spark.sql(
         "SELECT c.cust_name,date_format(o.order_date, 'yyyy-MM') AS month, COUNT(o.order_id) AS order_count FROM "
         "orders o JOIN customers c ON o.cust_id = c.cust_id GROUP BY c.cust_name, month")

    #add current date colum
    report_1 = report_1.withColumn('current_date', lit(current_date()))
    report_1.show()
    report_1.write.jdbc(url=properties['url'], table="report_1", mode="overwrite", properties=properties)

    output_path = "C:\\Users\\swetkuma\\Desktop\\pythonprojects\\output_data\\partition"
    report_1.write.mode("overwrite").partitionBy('current_date').parquet(output_path + '/report_1')


    report_3 = spark.sql('''select i.item_description as item_name, count(o.item_quantity) as total_order_count from items i
                            join order_details o
                            on i.item_id = o.item_id
                            group by item_name''')
    report_3 = report_3.withColumn('current_date', lit(current_date()))
    report_3.show()
    report_3.write.jdbc(url=properties['url'], table="report_3", mode="overwrite",
                        properties=properties)  # adding current date colum in postgrees
    output_path = "C:\\Users\\swetkuma\\Desktop\\pythonprojects\\output_data\\partition"
    report_3.write.mode("overwrite").partitionBy('current_date').parquet(output_path + '/report_3.')

    # Item name/category wise  Total Order count descending
    report_4 = spark.sql('''select i.category as category, count(o.order_id) as total_orders
                            from items i 
                            join order_details o
                            on i.item_id = o.item_id
                            group by category
                            order by total_orders desc''')
    report_4 = report_4.withColumn('current_date', lit(current_date()))
    report_4.show()
    report_4.write.jdbc(url=properties['url'], table="report_4", mode="overwrite",
                        properties=properties)  # adding current date colum in postgrees
    report_4.write.mode("overwrite").partitionBy('current_date').parquet(output_path + '/report_4.')
