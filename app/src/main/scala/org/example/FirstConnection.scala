package org.example


import java.sql.{Connection, DriverManager, ResultSet}


object FirstConnection extends App {

  val url = "jdbc:sqlserver://DESKTOP-TFOF660:1433;databaseName=vinnu;encrypt=false"
  val userName = "sa"
  val password = "vinesh"

  var connection: Connection = null

  try {
    connection = DriverManager.getConnection(url, userName, password)
    println("Connected Successfully")
    val statement = connection.createStatement()
    val setQuery = "select * from student"
    val resultSet:ResultSet = statement.executeQuery(setQuery)
    while(resultSet.next()){
      val id = resultSet.getInt("id")
      val name = resultSet.getString("name")
      println(s"ID: $id | Name: $name" )
    }
    resultSet.close()
    statement.close()
  }
  catch {
    case e : Exception => e.printStackTrace()
  }
  finally {
    if(connection != null)connection.close()
  }
}