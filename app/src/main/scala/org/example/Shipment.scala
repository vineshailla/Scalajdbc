package org.example

import org.example.EmployeeConnection.{connection, password, url, userName}

import java.sql.{Connection, DriverManager, ResultSet}

object Shipment extends App{

  val url = "jdbc:sqlserver://DESKTOP-TFOF660:1433;databaseName=vinnu;encrypt=false"
  val userName = "sa"
  val password = "vinesh"

  var connection: Connection = null
  try{
    connection = DriverManager.getConnection(url,userName,password)
    println("connected succesfully")
    val Statement = connection.createStatement()
    val setQuery = "select * from shipment"
    val resultSet:ResultSet=Statement.executeQuery(setQuery)
    while (resultSet.next()){
      val ShipmentId = resultSet.getInt("ShipmentId")
      val SPID = resultSet.getString("SPID")
      println(s"$ShipmentId $SPID")
    }
    Statement.close()
    resultSet.close()
  }
  catch {
    case e : Exception => e.printStackTrace()
  }
  finally {
    if(connection != null) connection.close()
  }

}
