package org.example.IntegrationTesting
import ConfigTest.{H2DatabaseData, TestSchema}
import Etl.Sevice.CsvTable
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.sql.ResultSet

class H2DatabaseUpdateTest extends AnyFlatSpec with Matchers with MockitoSugar{
  val logger = LoggerFactory.getLogger(getClass)
  "CsvTable" should "update table test case"in{
    val connection = H2DatabaseData.getConnection()
    val statement = connection.createStatement()
    val updateQuery ={
      """
        update artist set playcount = '1000'
        |""".stripMargin
    }
    statement.execute(TestSchema.createArtistTable)
    val result = CsvTable.insertTable(TestSchema.artists,H2DatabaseData)
    result shouldBe true
    val rs :ResultSet = statement.executeQuery("SELECT COUNT(*) as total FROM Artist")
    val count = statement.executeUpdate(updateQuery)
    logger.info(s"the count of updated is $count")
    connection.close()

  }
}

