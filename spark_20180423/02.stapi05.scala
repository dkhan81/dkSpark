import org.apache.spark.sql.functions.{current_date, current_timestamp}
val dateDF = spark.range(10)
  .withColumn("today", current_date())
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")


dateDF.printSchema()


import org.apache.spark.sql.functions.{date_add, date_sub}
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
//SELECT date_sub(today, 5), date_add(today, 5) FROM dateTable


%sh
wget https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/by-day/2010-12-01.csv


%spark
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/zeppelin/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")


%spark
df.na.drop()
df.na.drop("any")
 

%spark
df.filter("Description is null").show


%spark
df.na.drop("all")
df.na.drop("all", Seq("StockCode", "InvoiceNo"))

%spark
val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
df.na.fill(fillColValues)


%spark
df.na.replace("Description", Map("" -> "UNKNOWN"))
df.show

