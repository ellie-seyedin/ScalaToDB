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
      .appName("Full Load")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.read.jdbc(url, "people", properties)
//    val df = spark.read.format("csv").load("file:///C://Users/elahe/Documents/BigData/Spark-Scala-Load/people.csv")
    df.write.mode("overwrite").saveAsTable("`scala_people`")

    spark.stop()
  }

}