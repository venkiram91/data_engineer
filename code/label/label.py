from pyspark import SparkConf, SparkContext,SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType,LongType
from pyspark.sql.functions import *
import sys, operator,os

#Function to calculate distance in Kilometer
def cal_distance(lat1,long1,lat2,long2):
	R = 6371
	x = (long2 - long1) * cos( 0.5*(lat2+lat1) )
	y = lat2 - lat1
	d = R * sqrt( x*x + y*y )
	return d

def main():
	#Setting up spark configuration
	conf = SparkConf().setAppName('label')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'
	sqlContext = HiveContext(sc)
	#Reading Input file path and Output file path
	input1 = sys.argv[1]
	input2 = sys.argv[2]
	output = sys.argv[3]
	#Setting the schema for the data frame
	customSchema = StructType([StructField("id", LongType(), False),StructField("timeSet", StringType(), False),StructField("country", StringType(), False),StructField("province", StringType(), False),StructField('city',StringType(), False),StructField('latitude',FloatType(), False),StructField('longtitude',FloatType(), False)])
	poi_schema=StructType([StructField("poi_id", StringType(), False),StructField('poi_latitude',FloatType(), False),StructField('poi_longtitude',FloatType(), False)])
	#Reading data into data frame
	df_input1= sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(input1,schema = customSchema)
	df_input2= sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(input2,schema = poi_schema)
	#Performing join after Broadcast
	distance_calc=df_input1.join(broadcast(df_input2)).withColumn('distance_in_km',cal_distance(df_input1.latitude,df_input1.longtitude,df_input2.poi_latitude,df_input2.poi_longtitude))
	distance_calc.registerTempTable("table1")
	#Cluster the data  based on the column to group to reduce shuffling
	distance_calc = sqlContext.sql("SELECT * FROM table1 CLUSTER BY id")
	#Minimum distance is calculated
	poi_min_distance = distance_calc.groupBy('id').agg(min('distance_in_km').alias('distance_in_km'))
	#assigning the labels
	label_data=distance_calc.join(poi_min_distance, ['id', 'distance_in_km']).select('id', 'timeSet','country','province','city','latitude','longtitude','poi_id','poi_latitude','poi_longtitude','distance_in_km')
	label_data.registerTempTable("table2")
	#assigning the rank to remove duplicate within subgroup
	df_result=sqlContext.sql("select id,timeSet,country,province,city,latitude,longtitude,poi_id,poi_latitude,poi_longtitude,distance_in_km,rank() over ( partition by id order by poi_id) as rank from table2")
	df_result=df_result.where(df_result['rank'] ==1)
	#Saving the result
	df_result.coalesce( 1 ).write.format('com.databricks.spark.csv').save(output)



if __name__ == "__main__":
    main()
