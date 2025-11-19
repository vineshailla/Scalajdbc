package Etl.Main
import Etl.Config.ConfigurationData
import Etl.Sevice.CsvTable
import org.slf4j.LoggerFactory
object CsvMain {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("Starting CSV → SQL Server import process...")
    CsvTable.CreateArtistTable()
    val artist = CsvTable.readCsvFile(batchSize = 20000,ConfigurationData)
    CsvTable.updateTable(ConfigurationData)
    CsvTable.deleteTable(ConfigurationData)
    println("exit")
//    val (success, failed) = CsvTable.insertTable(artist)
  }
}
