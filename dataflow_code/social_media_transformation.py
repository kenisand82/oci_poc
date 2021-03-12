import sys
from pyspark.context import SparkContext
import random
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext



sc = SparkContext()

sqlc=SQLContext(sc)

def get_f_list(b):
      
      l1 = b_rdd.value #accessing the broadcast variable

      l2 = b.split('|')
      
      new_list = []
      for element in l2:
          if element in l1:
              new_list.append(element)

      result= ''
      for element in new_list:
        result = result + "|" + str(element)
      fixed_result = result[1:]
      
      
      return fixed_result
      
getfrnd = udf(get_f_list, StringType())

original_df = sqlc.read \
      .format("jdbc") \
      .option("url", "jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com") \
      .option("dbtable", "POC.CUSTOMER_DETAILS") \
      .option("user", "poc") \
      .option("password", "WElcome##123") \
      .option("driver", "oracle.jdbc.driver.OracleDriver") \
      .load()

sm_id_df = original_df.select(original_df['social_media_id'])

sm_id_df_rdd = sm_id_df.rdd.flatMap(lambda x: x).collect()

b_rdd = sc.broadcast(sm_id_df_rdd)

b1 = original_df.select(original_df['CUSTOMER_NAME'], original_df['social_media_id'].alias('SOCIAL_MEDIA_ID'), original_df['customer_email'].alias('EMAIL'), original_df['customer_address'].alias('ADDRESS'), original_df['customer_number'].alias('CONTACT'), getfrnd(original_df['is_friends_with']).alias('FRIENDS_LIST'))

b1.write.format('jdbc').options(
      url='jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com',
      driver='oracle.jdbc.driver.OracleDriver',
      dbtable='POC.SOCIAL_MEDIA_DWH',
      user='poc',
      password='WElcome##123').mode('overwrite').save()


