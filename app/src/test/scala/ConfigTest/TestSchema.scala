package ConfigTest

import Etl.Model.Artists


object TestSchema {

  val createArtistTable: String =
    """ CREATE TABLE Artist (
      |      user_id NVARCHAR(100),
      |      rank NVARCHAR(50),
      |      artist_name NVARCHAR(500),
      |      playcount NVARCHAR(50),
      |      mbid NVARCHAR(100)
      |    )
      |""".stripMargin
  val artists = List(
    Artists("1", "1", "vinnu", "100", "mbid1"),
    Artists("2", "2", "king", "200", "mbid2")
  )
}
