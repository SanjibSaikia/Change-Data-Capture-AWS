#Import all necessary libraries
from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

#Create a Spark Session
spark = SparkSession.builder.appName("CDC").getOrCreate()

#Capture the File path and bucket that are passed as parameters from Lambda
args = getResolvedOptions(sys.argv,['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

#Provide the path for Input and Outut File Directory 
# IMP : (Output File Directory(S3 : "cdc-output-pyspark") is separate from Input Directory(where files will land from RDS) as if we save the output path in the same S3 bucket spark will trigger the lambda function and restart the glue job)

inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://cdc-ouput-pyspark/output"

# --> If file is read for the first time from RDS, load it directlly in output directory("cdc-ouput-pyspark").In case of Full Load the file will be named as LOAD00000001.csv
if "LOAD" in fileName:
    fullLoaddf = spark.read.csv(inputFilePath)
    fullLoaddf = fullLoaddf.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    fullLoaddf.write.mode("overwrite").csv(finalFilePath)

# --> Any furthur changes in MySQL db, changes will be merged the Modified Full Load File(from output directory("cdc-ouput-pyspark")) and Updated File will be written back to output S3 bucket.
else:
	#Read the new changed file from input S3 bucket
    updateddf = spark.read.csv(inputFilePath)
    updateddf = updateddf.withColumnRenamed("_c0","action").withColumnRenamed("_c1","id").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    
    #Read the fullLoad file from output s3 bucket 
    finalMergeddf = spark.read.csv(finalFilePath)
    finalMergeddf = finalMergeddf.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    
    #As per the changes(Update : U,Insert : I,Delete : D) made to the original dataset operations will be performed
    for row in updateddf.collect(): 
      if row["action"] == 'U':
        finalMergeddf = finalMergeddf.withColumn("FullName", when(finalMergeddf["id"] == row["id"], row["FullName"]).otherwise(finalMergeddf["FullName"]))      
        finalMergeddf = finalMergeddf.withColumn("City", when(finalMergeddf["id"] == row["id"], row["City"]).otherwise(finalMergeddf["City"]))
    
      if row["action"] == 'I':
        insertedRow = [list(row)[1:]]
        columns = ['id', 'FullName', 'City']
        newdf = spark.createDataFrame(insertedRow, columns)
        finalMergeddf = finalMergeddf.union(newdf)
    
      if row["action"] == 'D':
        finalMergeddf = finalMergeddf.filter(finalMergeddf.id != row["id"])
        
    # DataFrame will be saved backed to the fullLoad File in output S3 bucket
    finalMergeddf.write.mode("overwrite").csv(finalFilePath)  