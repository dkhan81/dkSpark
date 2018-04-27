val df = spark.read.format("json").load("/zeppelin/2015-summary.json")
df.createOrReplaceTempView("dfTable")


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))
val myRows = Seq(Row("Hello", null, 1L))
val myRDD = spark.sparkContext.parallelize(myRows)
val myDf = spark.createDataFrame(myRDD, myManualSchema)
myDf.show()


val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")


myDf.show()



-- in SQL
SELECT * FROM dataFrameTable
SELECT columnName FROM dataFrameTable
SELECT columnName * 10, otherColumn, someOtherCol as c FROM dataFrameTable


df.select("DEST_COUNTRY_NAME").show(2)
//SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
//SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2


import org.apache.spark.sql.functions.{expr, col, column}
df.select(
    df.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    'DEST_COUNTRY_NAME,
    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME"))
  .show(2)


df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
//SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2


df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
  .show(2)
//SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2


df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
//SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2


