package Etl.Config

import com.typesafe.config.{Config, ConfigFactory}
import java.sql.{Connection, DriverManager}

class ConfigurationData extends ConfigTrait {
  private val config: Config = ConfigFactory.load()
  private val dataConfig = config.getConfig("database")

  override def getUrl: String = dataConfig.getString("url")
  override def userName: String = dataConfig.getString("userName")
  override def password: String = dataConfig.getString("password")
  override def getConnection(): Connection = DriverManager.getConnection(getUrl, userName, password)
  override def path: String = "C:\\csvfiles\\user_top_artists.csv"
}

// Companion object for easy access
object ConfigurationData extends ConfigurationData