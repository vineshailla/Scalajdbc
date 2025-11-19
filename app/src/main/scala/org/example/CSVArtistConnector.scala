package org.example

import com.github.tototoshi.csv.CSVReader
import org.example.CSVConnector.getConnection
import org.example.CsvUsersConnector.{createUserTable, password, url, userName}

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Using

object CSVArtistConnector {

  val url = "jdbc:sqlserver://DESKTOP-TFOF660:1433;databaseName=vinnu;encrypt=false"
  val userName = "sa"
  val password = "vinesh"

  case class Artist(
                     user_id: String,
                     rank: String,
                     artist_name: String,
                     playcount: String,
                     mbid: String
                   )

  val filePath = "C:\\csvfiles\\user_top_artists.csv"
  def main(args:Array[String]): Unit = {
    println("starting the excecution process")
    CreateArtistTable()
    insertArtists(filePath)
  }
  def insertArtists(filePath:String): Unit = {
    Using.resource(getConnection){
      connection =>
        connection.setAutoCommit(false)
        val insert = "insert into Artist(user_id, rank,artist_name, playcount, mbid) values(?,?,?,?,?)"
        Using.resource(connection.prepareStatement(insert)){
          preparedStatement=>
            Using.resource(CSVReader.open(filePath)){reader =>
              val records = reader.iteratorWithHeaders
              var count = 0
              var totalCount =0
              records.foreach{ row=>
                val artists = Artist(
                  user_id = row.getOrElse("user_id",""),
                  rank = row.getOrElse("rank",""),
                  artist_name = row.getOrElse("artist_name",""),
                  playcount = row.getOrElse("playcount",""),
                  mbid = row.getOrElse("mbid","")
                )
                preparedStatement.setString(1,artists.user_id)
                preparedStatement.setString(2,artists.rank)
                preparedStatement.setString(3,artists.artist_name)
                preparedStatement.setString(4,artists.playcount)
                preparedStatement.setString(5,artists.mbid)
                preparedStatement.addBatch()
                count+=1
                totalCount+=1
                if(count>=5000){
                  preparedStatement.executeBatch()
                  connection.commit()
                  count=0
                  println(s"total $totalCount inserted")
                }
              }
            }
            preparedStatement.executeBatch()
            connection.commit()
        }
    }
  }
  def CreateArtistTable(): Unit = {
    Using.resource(getConnection()) { connection =>
      val statement = connection.createStatement()
      val createTableSql =
        """
          |IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Artist' AND xtype='U')
          |CREATE TABLE Artist (
          |  user_id NVARCHAR(100),
          |  rank NVARCHAR(50),
          |  artist_name NVARCHAR(1000),
          |  playcount NVARCHAR(50),
          |  mbid NVARCHAR(100)
          |)
          |""".stripMargin
      statement.execute(createTableSql)
      connection.commit()
      println("Table 'Artist' verified/created successfully.")
    }
  }
  def getConnection(): Connection = {
    DriverManager.getConnection(url , userName , password)
  }
}