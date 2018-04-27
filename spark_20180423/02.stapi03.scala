df.printSchema
df.drop("ORIGIN_COUNTRY_NAME").columns

df.withColumn("count2", col("count").cast("long"))
//SELECT *, cast(count as long) AS count2 FROM dfTable

df.withColumn("count2", col("count").cast("long")).show

df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
//SELECT * FROM dfTable WHERE count < 2 LIMIT 2

// in Scala
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
  .show(2)
//SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
//SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable

df.select("ORIGIN_COUNTRY_NAME").distinct().count()
//SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME) FROM dfTable




