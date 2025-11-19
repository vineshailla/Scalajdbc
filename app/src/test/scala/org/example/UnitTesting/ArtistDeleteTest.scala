package org.example.UnitTesting

import Etl.Config.ConfigTrait
import Etl.Sevice.CsvTable
import org.mockito.ArgumentMatchers.anyString
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, Statement}

class ArtistDeleteTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "Service.CsvTable" should "delete the artist test case" in {
    val mockConfig = mock[ConfigTrait]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]
    when(mockConfig.getConnection()).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeUpdate(anyString())).thenReturn(1)
    doNothing.when(mockConnection).close()
    val result = CsvTable.deleteTable(mockConfig)
    verify(mockConfig).getConnection()
    verify(mockConnection).createStatement()
    verify(mockStatement).executeUpdate(anyString())
    verify(mockConnection).close()
    result shouldBe true
  }
  "service table" should "delete the artist test case for failure "in{
    val mockConfig = mock[ConfigTrait]
    val mockConnection = mock[Connection]
    val mockStatement = mock[Statement]
    when(mockConfig.getConnection()).thenReturn(mockConnection)
    when(mockConnection.createStatement()).thenReturn(mockStatement)
    when(mockStatement.executeUpdate(anyString())).thenReturn(0)
    doNothing.when(mockConnection).close()
    val result = CsvTable.deleteTable(mockConfig)
    verify(mockConfig).getConnection()
    verify(mockConnection).createStatement()
    verify(mockStatement).executeUpdate(anyString())
    verify(mockConnection).close()
    result shouldBe false
  }
}
