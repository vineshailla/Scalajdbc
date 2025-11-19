package org.example.IntegrationTesting

import ConfigTest.{H2DatabaseData, TestSchema}
import Etl.Sevice.CsvTable
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.sql.ResultSet

class H2DatabaseDeleteTest extends AnyFlatSpec with Matchers with MockitoSugar{
  val logger = LoggerFactory.getLogger(getClass)
  "delete table" should "delete the test cases" in{
    val connection = H2DatabaseData.getConnection()
    val statement = connection.createStatement()
    val DeleteQuery={
      """
         Delete from artist where rank = 2
        """.stripMargin
    }
    statement.execute(TestSchema.createArtistTable)
    val result = CsvTable.insertTable(TestSchema.artists,H2DatabaseData)
    result shouldBe true
    val rs :ResultSet = statement.executeQuery("SELECT COUNT(*) as total FROM Artist")
    val count = statement.executeUpdate(DeleteQuery)
    logger.info(s"the number of rows deleted is:$count")
    connection.close()
  }
}
