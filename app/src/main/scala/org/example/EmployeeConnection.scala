package org.example

import java.sql.{Connection, DriverManager, ResultSet}

object EmployeeConnection extends App{
  val url =  "jdbc:sqlserver://DESKTOP-TFOF660:1433;databaseName=vinnu;encrypt=false"
  val userName = "sa"
  val password = "vinesh"
  // create a connection
  var connection: Connection = null
  try{
    connection = DriverManager.getConnection(url,userName,password)
    println("connected succesfully")
    val Statement = connection.createStatement()
    val query = "select * from employee"
    val updateQyery="update employee set empName='adam' where empName= 'miller' "
    val update = Statement.executeUpdate(updateQyery)
    val resultSet : ResultSet = Statement.executeQuery(query)
    println(update)
    while (resultSet.next()){
      val empId = resultSet.getInt("empId")
      val empName = resultSet.getString("empName")
      println(s"id = $empId name = $empName")
    }
    resultSet.close()
    Statement.close()
  }
  catch {
    case e:Exception=> e.printStackTrace()
  }
  finally {
    if(connection != null) connection.close()
  }
}
