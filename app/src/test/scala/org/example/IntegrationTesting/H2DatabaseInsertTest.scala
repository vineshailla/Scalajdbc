package org.example.IntegrationTesting


import ConfigTest.{H2DatabaseData, TestSchema}
import Etl.Model.Artists
import Etl.Sevice.CsvTable
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.sql.ResultSet

class H2DatabaseInsertTest extends AnyFlatSpec with Matchers with MockitoSugar{

  val loggerFactory = LoggerFactory.getLogger(getClass)
  "insert table" should "insert into h2 database succesfully" in{
    val connection = H2DatabaseData.getConnection()
    val statement = connection.createStatement()
    statement.executeUpdate("DROP TABLE IF EXISTS Artist")
    statement.execute(TestSchema.createArtistTable)
    loggerFactory.info("created successfully")
    val result = CsvTable.insertTable(TestSchema.artists,H2DatabaseData)
    result shouldBe true
    val rs :ResultSet = statement.executeQuery("SELECT COUNT(*) as total FROM Artist")
    rs.next()
    rs.getInt(1) shouldBe 2
  }
}

