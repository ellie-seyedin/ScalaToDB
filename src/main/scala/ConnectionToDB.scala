import java.sql.{Connection, DriverManager, ResultSet}


object ConnectionToDB {
  def main(args: Array[String]): Unit = {
  val url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
  val user = "consultants"
  val password = "WelcomeItc@2022"

  val connection = DriverManager.getConnection(url, user, password)

  val statement = connection.createStatement()
  val resultSet = statement.executeQuery("SELECT * FROM people")

  while (resultSet.next()) {
    val column1Value = resultSet.getString("name")
    val column2Value = resultSet.getString("occupation")

    println(s"Column 1 Value: $column1Value, Column 2 Value: $column2Value")

  }
  connection.close()
}}