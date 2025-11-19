package Etl.Sevice

import Etl.Config.{ConfigTrait, ConfigurationData}
import Etl.Model.Artists
import com.github.tototoshi.csv.CSVReader
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.{Try, Using}

object CsvTable {
  private val logger = LoggerFactory.getLogger(getClass)

  // Use the trait type and default to the object
  def readCsvFile(batchSize: Int, config: ConfigurationData): List[Artists] = {
    println("entered")
    var successCount = 0
    var errorCount = 0
    var newList = List.empty[Artists]

    Using.resource(CSVReader.open(new File(config.path))) {
      reader =>
        val read = reader.iteratorWithHeaders
        while (read.hasNext) {
          try {
            val row = read.next()
            val art = Artists(
              user_id = Try(row("user_id")).getOrElse(""),
              rank = row.getOrElse("rank", ""),
              artist_name = row.getOrElse("artist_name", ""),
              playcount = row.getOrElse("playcount", ""),
              mbid = row.getOrElse("mbid", "")
            )
            newList = art :: newList
            successCount += 1
            if (successCount % batchSize == 0) {
              insertTable(newList.reverse, config)
              newList = List.empty[Artists]
              logger.info(s"processed $successCount batches")
            }
          } catch {
            case e: Exception =>
              errorCount += 1
              if (errorCount <= 10) {
                e.printStackTrace()
              }
          }
        }
        if (newList.nonEmpty) {
          insertTable(newList.reverse, config)
          logger.info(s"processed final ${newList.size} batches")
        }
        newList
    }
  }

  def CreateArtistTable(config: ConfigTrait = ConfigurationData): Boolean = {
    Using.resource(config.getConnection()) { connection =>
      val statement = connection.createStatement()
      val createTable =
        """
          |IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Artist' AND xtype='U')
          |CREATE TABLE Artist (
          |  user_id NVARCHAR(100),
          |  rank NVARCHAR(50),
          |  artist_name NVARCHAR(500),
          |  playcount NVARCHAR(50),
          |  mbid NVARCHAR(100)
          |)
          |""".stripMargin
      statement.execute(createTable)
      connection.commit()
      logger.info("table is created")
      statement.close()
      true
    }
  }

  def updateTable(config: ConfigTrait): Boolean = {
    Try {
      Using.resource(config.getConnection()) { connection =>
        val stmt = connection.createStatement()
        val rows = stmt.executeUpdate("update Artist set playcount = 5")
        rows > 0
      }
    }.getOrElse(false)
  }

  def deleteTable(config: ConfigTrait): Boolean = {
    try {
      Using.resource(config.getConnection()) { connection =>
        val deleteSql = "DELETE FROM Artist WHERE rank = 2"
        val stmt = connection.createStatement()

        val rowsAffected = stmt.executeUpdate(deleteSql)

        rowsAffected > 0
      }
    } catch {
      case e: Exception =>
        false
    }
  }


def insertTable(artists: List[Artists], config: ConfigTrait): Boolean = {
  try {
    Using.resource(config.getConnection()) { connection =>
      connection.setAutoCommit(false)

      val insertSql =
        "insert into Artist(user_id, rank, artist_name, playcount, mbid) values(?,?,?,?,?)"

      val success = Using.resource(connection.prepareStatement(insertSql)) { stmt =>
        artists.foreach { a =>
          stmt.setString(1, a.user_id)
          stmt.setString(2, a.rank)
          stmt.setString(3, a.artist_name)
          stmt.setString(4, a.playcount)
          stmt.setString(5, a.mbid)
          stmt.addBatch()
        }

        try {
          stmt.executeBatch()
          connection.commit()
          logger.info("inserted ")
          true
        } catch {
          case e: Exception =>
            logger.error(s"Failed to execute batch : ${e.getMessage}")
            connection.rollback()
            false
        }
      }

      success
    }
  } catch {
    case e: Exception =>
      logger.error(s"Unexpected DB error : ${e.getMessage}")
      false
  }
}

}