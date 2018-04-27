df.selectExpr("(Description, InvoiceNo) as complex", "*")

import org.apache.spark.sql.functions.struct
val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")



complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))


complexDF.select("complex.*")


df.select(split(col("Description"), " ")).show(2)



import org.apache.spark.sql.functions.{split, explode}

df.withColumn("splitted", split(col("Description"), " "))
  .withColumn("exploded", explode(col("splitted")))
  .select("Description", "InvoiceNo", "exploded").show(2)


//SELECT Description, InvoiceNo, exploded
//FROM (SELECT *, split(Description, " ") as splitted FROM dfTable)
//LATERAL VIEW explode(splitted) as exploded
