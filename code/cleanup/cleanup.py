from pyspark import SparkConf, SparkContext,SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType,LongType
from pyspark.sql.functions import *
import sys, operator,os

def main():
	#Setting up spark configuration
	conf = SparkConf().setAppName('cleanup')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'
	sqlContext = HiveContext(sc)
	#Setting Input file path and output file path
	inputs = sys.argv[1]
	output = sys.argv[2]
	#Decaring Schema for data frame
	customSchema = StructType([StructField("id", LongType(), False),StructField("timeSet", StringType(), False),StructField("country", StringType(), False),StructField("province", StringType(), False),StructField('city',StringType(), False),StructField('latitude',FloatType(), False),StructField('longtitude',FloatType(), False)])
	df_data1= sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(inputs,schema = customSchema)
	df_data2=df_data1 #copy of data
	#Register data frame as table
	df_data1.registerTempTable("table1")
	df_data2.registerTempTable("table2")
	#Cluster the data based on the column to join to reduce shuffling
	df_data1 = sqlContext.sql("SELECT * FROM table1 CLUSTER BY timeSet,latitude,longtitude,id")
	df_data2 = sqlContext.sql("SELECT * FROM table2 CLUSTER BY timeSet,latitude,longtitude,id")
	#Data frame join to find duplicates
	df_duplicate=df_data1.join(df_data2, (df_data1.timeSet==df_data2.timeSet) & (df_data1.latitude==df_data2.latitude) & (df_data1.longtitude==df_data2.longtitude) & (df_data1.id!=df_data2.id), "inner").select(df_data1.id,df_data1.timeSet,df_data1.country,df_data1.province,df_data1.city,df_data1.latitude,df_data1.longtitude)
	#remove the duplicates
	df_clean =df_data1.subtract(df_duplicate)
	#Save the cleaned data
	df_clean.coalesce( 1 ).write.format('com.databricks.spark.csv').save(output)

	
if __name__ == "__main__":
    main()
