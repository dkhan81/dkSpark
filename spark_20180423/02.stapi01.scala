%sh
wget https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2015-summary.json


%sh
head /zeppelin/2015-summary.json


%spark
val df = spark.read.format("json").load("/zeppelin/2015-summary.json")

df.printSchema()


import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))

val df = spark.read.format("json").schema(myManualSchema)
  .load("/zeppelin/2015-summary.json")
  
  
%spark
df.show()

%spark
spark.read.format("json").load("/zeppelin/2015-summary.json").columns


%spark
df.first()


%spark
import org.apache.spark.sql.Row
val myRow = Row("Hello", null, 1, false)


%spark
myRow(0)


