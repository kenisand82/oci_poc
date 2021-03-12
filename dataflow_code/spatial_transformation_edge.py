from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
import sys
from pyspark.context import SparkContext
import random
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import split
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import format_string
from pyspark.sql.functions import monotonically_increasing_id
import functools

sc = SparkContext()

sqlc=SQLContext(sc)

def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs) 

original_df = sqlc.read \
      .format("jdbc") \
      .option("url", "jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com") \
      .option("dbtable", "POC.social_media_dwh") \
      .option("user", "poc") \
      .option("password", "WElcome##123") \
      .option("driver", "oracle.jdbc.driver.OracleDriver") \
      .load()
	  
selectDF=original_df.select(original_df['social_media_id'], original_df['friends_list'])

selectDF1 =selectDF.withColumn('friend1', split(selectDF['friends_list'], "\|").getItem(0)).withColumn('friend2', split(selectDF['friends_list'], "\|").getItem(1)).withColumn('friend3', split(selectDF['friends_list'], "\|").getItem(2)).withColumn('friend4', split(selectDF['friends_list'], "\|").getItem(3)).withColumn('friend5', split(selectDF['friends_list'], "\|").getItem(4))

selectDF1_load1 =selectDF1.select(selectDF1['social_media_id'].alias('FROM_SOCIAL_MEDIA_ID'), selectDF1['friend1'].alias('TO_SOCIAL_MEDIA_ID'))

selectDF1_load2 =selectDF1.select(selectDF1['social_media_id'].alias('FROM_SOCIAL_MEDIA_ID'), selectDF1['friend2'].alias('TO_SOCIAL_MEDIA_ID'))

selectDF1_load3 =selectDF1.select(selectDF1['social_media_id'].alias('FROM_SOCIAL_MEDIA_ID'), selectDF1['friend3'].alias('TO_SOCIAL_MEDIA_ID'))

selectDF1_load4 =selectDF1.select(selectDF1['social_media_id'].alias('FROM_SOCIAL_MEDIA_ID'), selectDF1['friend4'].alias('TO_SOCIAL_MEDIA_ID'))

selectDF1_load5 =selectDF1.select(selectDF1['social_media_id'].alias('FROM_SOCIAL_MEDIA_ID'), selectDF1['friend5'].alias('TO_SOCIAL_MEDIA_ID'))

unioned_df = unionAll([selectDF1_load1, selectDF1_load2, selectDF1_load3, selectDF1_load4, selectDF1_load5])

df_sorted = unioned_df.orderBy("FROM_SOCIAL_MEDIA_ID")

df = df_sorted.withColumn("ROW_ID", monotonically_increasing_id())

df.write.format('jdbc').options(
     url='jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com',
     driver='oracle.jdbc.driver.OracleDriver',
     dbtable='POC.EDGE_TABLE',
     user='poc',
     truncate='true',
     password='WElcome##123').mode('overwrite').save()
