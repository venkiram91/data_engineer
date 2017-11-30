from pyspark import SparkConf, SparkContext,SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType,LongType
from pyspark.sql.functions import *
import sys, operator,os


def main():
	#Setting up spark configuration
	conf = SparkConf().setAppName('analysis')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'
	sqlContext = HiveContext(sc)
	#Setting Input file path and output file path
	inputs = sys.argv[1]
	output = sys.argv[2]
	#Decaring Schema for data frame
	customSchema = StructType([StructField("id", LongType(), False),StructField("timeSet", StringType(), False),StructField("country", StringType(), False),StructField("province", StringType(), False),StructField('city',StringType(), False),StructField('latitude',FloatType(), False),StructField('longtitude',FloatType(), False),StructField('poi_id',StringType(), False),StructField('poi_latitude',FloatType(), False),StructField('poi_longtitude',FloatType(), False),StructField('distance_in_km',FloatType(), False)])
	#read data into data frame
	df1= sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(inputs,schema = customSchema)
	#Register data frame into table
	df1.registerTempTable("temp_table")
	#Cluster the data based on the column to group to reduce shuffling
	df1 = sqlContext.sql("SELECT * FROM temp_table CLUSTER BY poi_id")
	df1.registerTempTable("table1")
	#Calculating POI average
	df_avg = sqlContext.sql("SELECT poi_id,avg(distance_in_km) as avg_distane_in_km,stddev(distance_in_km) FROM table1 GROUP BY poi_id")
	df_avg.coalesce( 1 ).write.format('com.databricks.spark.csv').save(output+'/average_stddeviation',header = 'true')
	#Calculating POI density
	df_density = sqlContext.sql("SELECT poi_id,count(poi_id) as density FROM table1 GROUP BY poi_id")
	df_density.coalesce( 1 ).write.format('com.databricks.spark.csv').save(output+'/density',header = 'true')
	#Calculating POI circle area
	df_circle_area = sqlContext.sql("SELECT poi_id,3.14*(max(distance_in_km))*(max(distance_in_km))as circle_area FROM table1 GROUP BY poi_id")
	df_circle_area.coalesce( 1 ).write.format('com.databricks.spark.csv').save(output+'/circle_area',header = 'true')

	

	



if __name__ == "__main__":
    main()
