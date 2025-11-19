package org.example.UnitTesting
import Etl.Config.ConfigTrait
import Etl.Model.Artists
import Etl.Sevice.CsvTable
import org.junit.platform.commons.logging.LoggerFactory
import org.mockito.ArgumentMatchers.{anyInt, anyString}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, PreparedStatement}

class ArtistInsertMock extends AnyFlatSpec with Matchers with MockitoSugar {

  "insert table" should "insert batch of artists" in {
    val logger = LoggerFactory.getLogger(getClass)
    // Mock the dependencies - use ConfigTrait instead of ConfigurationData.type
    val mockConfig = mock[ConfigTrait]
    val mockConnection = mock[Connection]
    val mockStatement = mock[PreparedStatement]

    // Setup the mock behaviors
    when(mockConfig.getConnection()).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement)

    // Use doNothing for void methods
    doNothing.when(mockConnection).setAutoCommit(false)
    doNothing.when(mockStatement).setString(anyInt, anyString())
    doNothing.when(mockStatement).addBatch()
    doNothing.when(mockConnection).commit()
    doNothing.when(mockStatement).close()
    doNothing.when(mockConnection).close()

    // Mock executeBatch to return array
    when(mockStatement.executeBatch()).thenReturn(Array(1))

    val artists = List(
      Artists("1", "1", "vinnu", "100", "mbid1"),
      Artists("2", "2", "king", "200", "mbid2")
    )

    // Execute the test
    val result = CsvTable.insertTable(artists, mockConfig)

    // Verify interactions
    verify(mockConfig).getConnection()
    verify(mockConnection).setAutoCommit(false)
    verify(mockConnection).prepareStatement(anyString())
    verify(mockStatement, times(10)).setString(anyInt, anyString()) // 5 fields × 2 artists = 10 calls
    verify(mockStatement, times(2)).addBatch()
    verify(mockStatement).executeBatch()
    verify(mockConnection).commit()
    verify(mockStatement).close()
    verify(mockConnection).close()

    // Assert the result
    result shouldBe true
  }
  "insert table"should "return false when executeBatch fails" in{
    val mockConfig = mock[ConfigTrait]
    val mockConnection = mock[Connection]
    val mockStatement = mock[PreparedStatement]
    when(mockConfig.getConnection()).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement)
    doNothing.when(mockConnection).setAutoCommit(false)
    doNothing.when(mockStatement).setString(anyInt,anyString())
    doNothing.when(mockStatement).addBatch()
    doNothing.when(mockConnection).rollback()
    when(mockStatement.executeBatch()).thenThrow(new RuntimeException("DB error"))
    doNothing.when(mockStatement).close()
    doNothing.when(mockConnection).close()
    val artists = List(
      Artists("1", "1", "vinnu", "100", "mbid1"),
      Artists("2", "2", "king", "200", "mbid2")
    )
    val result = CsvTable.insertTable(artists,mockConfig)
    result shouldBe false
  }
}
