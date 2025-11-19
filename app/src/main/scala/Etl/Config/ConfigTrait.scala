package Etl.Config

import java.sql.Connection

trait ConfigTrait {
  def getUrl:String
  def userName:String
  def password:String
  def path:String
  def getConnection():Connection
}
