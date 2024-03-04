package jenkins

import org.apache.spark.sql.SparkSession

import java.util.Properties

object SparkHiveConnector {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    val user = "consultants"
    val password = "WelcomeItc@2022"


    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    val spark = SparkSession.builder()
      .master("local[*]")
//      .appName("Full Load")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.read.jdbc(url, "people", properties)
     df.createOrReplaceTempView("temp_people")
//    var tableName = "people"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS testdb")
    spark.sql(s"USE testdb")
    spark.sql(s"DROP TABLE IF EXISTS people")
    spark.sql(s"CREATE OR REPLACE TABLE people AS SELECT * FROM temp_people")
    df.write.format("hive").mode("overwrite").option("path", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/USUK30/elaheh/hive/import_people").saveAsTable(s"testdb.people")
    spark.stop()



//    df.write
//      .format("hive")
//      .mode("overwrite")
      //   .option("path", "hdfs://<HDFSNameNode>:<Port>/path/to/your/table/location")
//      .option("path", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/USUK30/elaheh/hive/import_people")
      // hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/USUK30/Asad/import
//      .saveAsTable(s"$hiveDatabaseName.$hiveTableName")
  }

}