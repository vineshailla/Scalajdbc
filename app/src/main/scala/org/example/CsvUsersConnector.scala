package org.example

import com.github.tototoshi.csv.CSVReader
import org.example.CSVConnector.{getConnection, insertCustomers, password, readCSVFile, url, userName}

import java.sql.{Connection, DriverManager}
import scala.util.{Try, Using}

object CsvUsersConnector {
  val url = "jdbc:sqlserver://DESKTOP-TFOF660:1433;databaseName=vinnu;encrypt=false"
  val userName = "sa"
  val password = "vinesh"

  def main(args:Array[String]):Unit= {
    val CSVfilePath = "C:\\csvfiles\\users.csv"
 println("starting CSV to SQL Server import...")
 val users = readCSVFile(CSVfilePath)
  if (users.nonEmpty) {
    println(s"✓ Read ${users.size} records from CSV")
    createUserTable()
    insertUsers(users)
  }
    else {
     println("✗ No records found in CSV file")
    }
  }
  case class Users( user_id : String,
                    country:String,
                    total_scrobbles:String)
  def readCSVFile(filePath:String):List[Users]={
    println(s"Reading CSV file: $filePath")
    Try {
      val reader = CSVReader.open(new java.io.File(filePath))
      val records = reader.allWithHeaders()
      reader.close()
      records.map { row =>
        Users(
          user_id = row.getOrElse("user_id",""),
          country = row.getOrElse("country", ""),
          total_scrobbles = row.getOrElse("total_scrobbles","")
        )
      }
      }.recover{
      case e :Exception =>
        println(s"Error reading CSV file: ${e.getMessage}")
        e.printStackTrace()
        List.empty[Users]
    }.get
  }
  def createUserTable():Unit={
    Using.resource(getConnection()){ connection =>
      val createTableSql = """
      IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Users' AND xtype='U')
      CREATE TABLE Users (
           user_id VARCHAR(100) PRIMARY KEY,
           country NVARCHAR(100),
           total_scrobbles BIGINT
          )
          """
      val statement = connection.createStatement()
      statement.execute(createTableSql)
      println("users table is created")
    }
  }
  def insertUsers(user:List[Users]):Unit={
    Using.resource(getConnection()){connection =>
      connection.setAutoCommit(false)
      val insertSQl = """insert into Users(user_id,country,total_scrobbles) values (?,?,?)"""
      Using.resource(connection.prepareStatement(insertSQl)){ preapredStatement =>
        var successCount = 0
        var errorCount = 0
        user.foreach{ users =>
          try{
            preapredStatement.setString(1,users.user_id)
            preapredStatement.setString(2,users.country)
            preapredStatement.setString(3,users.total_scrobbles)
            preapredStatement.addBatch()
            successCount+=1
            if(successCount % 50000 == 0){
              val batchResults = preapredStatement.executeBatch()
              println(s"Processed $successCount records...")
            }
          }
          catch {
            case e:Exception=>
              println(s"failed ${users.user_id} : ${e.getMessage}")
              errorCount += 1
        }
        }
        try{
          val batchResults = preapredStatement.executeBatch()
          connection.commit()
          println(s"successfully inserted $successCount record into database")
          if(errorCount>0){
            println(s"failed to insert $errorCount records due to error")
          }
        } catch {
          case e : Exception =>
            connection.rollback()
            println(s" Batch insert failed: ${e.getMessage}")
        }
      }
    }
  }
  def getConnection(): Connection = {
    DriverManager.getConnection(url , userName , password)
  }
}




