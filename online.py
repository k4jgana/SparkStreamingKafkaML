from pyspark.sql import SparkSession
from pyspark.sql.functions import *







kafka_topic_name = "sensors"
kafka_bootstrap_servers = 'localhost:9092'



spark = SparkSession \
    .builder \
    .appName("PySpark-Streaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


df = spark \
.readStream  \
.format("kafka") \
.option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
.option("subscribe", kafka_topic_name) \
.load().selectExpr("CAST(value AS STRING)")


df.printSchema()

    
features = df.select(



       [ split(df.value,',')[0].alias('Diabetes_binary'),
       split(df.value,',')[1].alias('HighBP'),
       split(df.value,',')[2].alias('HighChol'),
       split(df.value,',')[3].alias('CholCheck'),
       split(df.value,',')[4].alias('BMI'),
       split(df.value,',')[5].alias('Smoker'),
       split(df.value,',')[6].alias('Stroke'),
       split(df.value,',')[7].alias('HeartDiseaseorAttack'),
       split(df.value,',')[8].alias('PhysActivity'),
       split(df.value,',')[9].alias('Fruits'),
       split(df.value,',')[10].alias('Veggies'),
       split(df.value,',')[11].alias('HvyAlcoholConsump'),
       split(df.value,',')[12].alias('AnyHealthcare'),
       split(df.value,',')[13].alias('NoDocbcCost'),
       split(df.value,',')[14].alias('GenHlth'),
       split(df.value,',')[15].alias('MentHlth'),
       split(df.value,',')[16].alias('PhysHlth'),
       split(df.value,',')[17].alias('DiffWalk'),
       split(df.value,',')[18].alias('Sex'),
       split(df.value,',')[19].alias('Age'),
       split(df.value,',')[20].alias('Education'),
       split(df.value,',')[21].alias('Income')]
       
).withColumn('Diabetes_binary',col('Diabetes_binary').substr(3,4))\
.withColumn('Income',col('Income').substr(0,1))\
.selectExpr('CAST(Diabetes_binary AS INTEGER)',
'CAST(HighBP AS INTEGER)',
'CAST(HighChol AS INTEGER)',
'CAST(CholCheck AS INTEGER)',
'CAST(BMI AS INTEGER)',
'CAST(Smoker AS INTEGER)',
'CAST(Stroke AS INTEGER)',
'CAST(HeartDiseaseorAttack AS INTEGER)',
'CAST(PhysActivity AS INTEGER)',
'CAST(Fruits AS INTEGER)',
'CAST(Veggies AS INTEGER)',
'CAST(HvyAlcoholConsump AS INTEGER)',
'CAST(AnyHealthcare AS INTEGER)',
'CAST(NoDocbcCost AS INTEGER)',
'CAST(GenHlth AS INTEGER)',
'CAST(MentHlth AS INTEGER)',
'CAST(PhysHlth AS INTEGER)',
'CAST(DiffWalk AS INTEGER)',
'CAST(Sex AS INTEGER)',
'CAST(Age AS INTEGER)',
'CAST(Education AS INTEGER)',
'CAST(Income AS INTEGER)')






df_write=features.writeStream\
    .trigger(processingTime='3 seconds')\
    .outputMode("update")\
    .option('truncate','false')\
    .format('console')\
    .start()\




df_write.awaitTermination()


