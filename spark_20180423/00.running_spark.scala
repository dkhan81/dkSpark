
docker ps -a | sed '1d' | awk '{print $1}' | xargs -L1 docker rm
docker images -a | sed '1d' | awk '{print $3}' | xargs -L1 docker rmi -f





docker run  -p 4040:4040 -p 7077:7077 -p 8080:8080 --name zeppelin-server --privileged=true -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_NOTEBOOK_DIR='/notebook' -e ZEPPELIN_LOG_DIR='/logs' -d apache/zeppelin:0.7.3 /zeppelin/bin/zeppelin.sh

docker run  -p 4040:4040 -p 6066:6066 -p 7077:7077 -p 8080:8080 -p 7777:7777 --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=false -d bde2020/spark-master:2.3.0-hadoop2.7


docker exec -i -t c187842327e8 /bin/bash
docker stop zeppelin-server

docker start spark-master


val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
sc.parallelize(1 to 9, 1)
res1.partitions.size
distData.collect


%sh
wget https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/csv/2015-summary.csv


%sh
head /zeppelin/2015-summary.csv


%spark
val flightData2015 = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/zeppelin/2015-summary.csv")


flightData2015.take(3)


flightData2015.sort("count").explain()


spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)


flightData2015.createOrReplaceTempView("flight_data_2015")


val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

val dataFrameWay = flightData2015
  .groupBy('DEST_COUNTRY_NAME)
  .count()

sqlWay.explain
dataFrameWay.explain


spark.sql("SELECT max(count) from flight_data_2015").take(1)


import org.apache.spark.sql.functions.max

flightData2015.select(max("count")).take(1)


val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()



import org.apache.spark.sql.functions.desc

flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .show()


flightData2015.cache()


flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .show()
