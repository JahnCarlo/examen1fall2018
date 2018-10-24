
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schoolSchema = StructType([
	StructField('region', StringType()), 
	StructField('district', StringType()), 
	StructField('city', StringType()), 
	StructField('sch_ID', IntegerType()), 
	StructField('schName', StringType()), 
	StructField('sch_level', StringType()), 
	StructField('serialNum', IntegerType())])

studentSchema = StructType([
	StructField('region', StringType()), 
	StructField('district', StringType()), 
	StructField('sch_ID', IntegerType()), 
	StructField('schName', StringType()),
	StructField('sch_level', StringType()), 
	StructField('std_sex', StringType()), 
	StructField('std_id', IntegerType())])


studentsDF = spark.read.format('csv').option('header', 'false').schema(studentSchema).load("../hive/studentsPR.csv")
schoolsDF = spark.read.format('csv').option('header', 'false').schema(schoolSchema).load("../hive/escuelasPR.csv")

studentsDF.join(schoolsDF, 'sch_ID').filter(studentsDF.std_sex == 'M').filter((schoolsDF.city == 'Ponce')| (schoolsDF.city == 'San Juan')).filter(schoolsDF.sch_level == 'Superior').select(studentsDF.std_id).write.csv('./results1')
