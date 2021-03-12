import sys
from pyspark.context import SparkContext
import random
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext



sc = SparkContext()

sqlc=SQLContext(sc)

original_df = sqlc.read \
      .format("jdbc") \
      .option("url", "jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com") \
      .option("dbtable", "POC.CUSTOMER_DWH") \
      .option("user", "poc") \
      .option("password", "WElcome##123") \
      .option("driver", "oracle.jdbc.driver.OracleDriver") \
      .load()

original_select = original_df.select(original_df['CUSTOMER_NAME'], original_df['SOCIAL_MEDIA_ID'], original_df['CUSTOMER_EMAIL'], original_df['LONGITUDE'], original_df['LATITUDE'])

original_select.write.format('jdbc').options(
      url='jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com',
      driver='oracle.jdbc.driver.OracleDriver',
      dbtable='POC.VERTEX_TABLE',
      user='poc',
      truncate='true',
      password='WElcome##123').mode('overwrite').save()


