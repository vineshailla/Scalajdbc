package ConfigTest

import Etl.Config.ConfigTrait
import com.typesafe.config.{Config, ConfigFactory}

import java.sql.{Connection, DriverManager}

class H2DatabaseData extends ConfigTrait {
  private val config:Config = ConfigFactory.load()
  private val dataconfig = config.getConfig("adapters")
  override def getUrl: String = dataconfig.getString("url")

  override def userName: String = dataconfig.getString("username")

  override def password: String = dataconfig.getString("password")
//  Class.forName("org.h2.Driver")

//  override def getConnection(): Connection = DriverManager.getConnection(getUrl, userName, password)

  override def getConnection(): Connection = DriverManager.getConnection(getUrl,userName,password)

  override def path: String = ""
}
// companion objects
object H2DatabaseData extends H2DatabaseData
