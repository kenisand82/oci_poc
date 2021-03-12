from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyzipcode import ZipCodeDatabase

def get_zip(b):
 
    zcdb = ZipCodeDatabase()
    zipcode = zcdb[b]
    longi = zipcode.longitude
    Lati = zipcode.latitude
    cord = str(longi) + ',' +  str(Lati)
    return cord
	
getzip = udf(get_zip, StringType())
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
original_df = spark.read \
      .format("jdbc") \
      .option("url", "jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com") \
      .option("dbtable", "POC.CUSTOMER_DETAILS") \
      .option("user", "poc") \
      .option("password", "WElcome##123") \
      .option("driver", "oracle.jdbc.driver.OracleDriver") \
      .load()
selectDF=original_df.select(original_df['customer_address'],original_df['social_media_id'],original_df['city'],original_df['customer_number'],original_df['customer_email'],original_df['customer_name'],original_df['state'],original_df['customer_id'],original_df['region'],original_df['zipcode'],getzip(original_df['zipcode']).alias('long_lat'))

split_col = F.split(selectDF['long_lat'], ',')

sselectDF=selectDF.select(selectDF['CUSTOMER_ADDRESS'],selectDF['SOCIAL_MEDIA_ID'],selectDF['CITY'],selectDF['CUSTOMER_NUMBER'],selectDF['CUSTOMER_EMAIL'],selectDF['CUSTOMER_NAME'],selectDF['STATE'],selectDF['CUSTOMER_ID'],selectDF['REGION'],selectDF['zipcode'].alias('ZIPCODE'),split_col.getItem(0).alias('LONGITUDE'),split_col.getItem(1).alias('LATITUDE'))


sselectDF.show(1)

sselectDF.write.format('jdbc').options(
      url='jdbc:oracle:thin:@150.136.138.197:1521/BIASDB_PDB1.subnet12011439.vcn12011439.oraclevcn.com',
      driver='oracle.jdbc.driver.OracleDriver',
      dbtable='POC.CUSTOMER_DWH',
      user='poc',
      password='WElcome##123').mode('overwrite').save()

