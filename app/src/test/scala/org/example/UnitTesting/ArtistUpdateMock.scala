package org.example.UnitTesting
import Etl.Config.ConfigTrait
import Etl.Sevice.CsvTable
import org.mockito.ArgumentMatchers.anyString
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.sql.{Connection, Statement}

class ArtistUpdateMock extends AnyFlatSpec with Matchers with MockitoSugar {
  val logger = LoggerFactory.getLogger(getClass)
  "Service.CsvTable" should "update the artist test case" in {
    val mockConfig = mock[ConfigTrait]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]

    when(mockConfig.getConnection()).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeUpdate(anyString())).thenReturn(1)
    doNothing.when(mockConnection).close()
    val result = CsvTable.updateTable(mockConfig)
    verify(mockConfig).getConnection()
    verify(mockConnection).createStatement()
    verify(mockStatement).executeUpdate(anyString())
    verify(mockConnection).close()

    result shouldBe true
  }
  "service table" should "update failure test cases" in{
    val mockConfig = mock[ConfigTrait]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]
    when(mockConfig.getConnection()).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeUpdate(anyString())).thenReturn(0)
    doNothing.when(mockConnection).close()
    val res = CsvTable.updateTable(mockConfig)
    verify(mockConfig).getConnection()
    verify(mockConnection).createStatement()
    verify(mockStatement).executeUpdate(anyString())
    verify(mockConnection).close()
    res shouldBe false
  }
}