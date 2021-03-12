import sys
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Transaction details ETL load") \
    .getOrCreate()

original_df = spark.read \
      .format("jdbc") \
      .option("url", "jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com") \
      .option("dbtable", "POC.ORDERS") \
      .option("user", "poc") \
      .option("password", "WElcome##123") \
      .option("driver", "oracle.jdbc.driver.OracleDriver") \
      .load()

split_col = F.split(original_df['order_date'], '-')

selectfield = original_df.select(original_df['ORDER_ID'], original_df['order_date'].alias('ORDER_DATE'), split_col.getItem(0).alias('YEAR_Y'), split_col.getItem(1).alias('MONTH_M'), split_col.getItem(2).alias('DATE_D'), original_df['CUSTOMER_ID'], original_df['QUANTITY'], original_df['total_sale'].alias('PRICE'),original_df['PRODUCT_ID'],original_df['PRODUCT_NAME'])

selectfield.show(1)

selectfield.write.format('jdbc').options(
      url='jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com',
      driver='oracle.jdbc.driver.OracleDriver',
      dbtable='POC.CUST_TRANSACTION_DWH',
      user='poc',
      password='WElcome##123').mode('overwrite').save()