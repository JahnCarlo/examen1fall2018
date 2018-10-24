
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count

spark = SparkSession.builder.getOrCreate()

schoolSchema = StructType([
	StructField('sch_region', StringType()), 
	StructField('sch_district', StringType()), 
	StructField('sch_city', StringType()), 
	StructField('sch_ID', IntegerType()), 
	StructField('sch_name', StringType()), 
	StructField('sch_level', StringType()), 
	StructField('sch_serial', IntegerType())])

schoolsDF = spark.read.format('csv').option('header', 'false').schema(schoolSchema).load("../hive/escuelasPR.csv")
schoolsDF.filter(schoolsDF.region == 'Arecibo').groupBy('sch_district', 'sch_city').count().write.csv('./results2')
