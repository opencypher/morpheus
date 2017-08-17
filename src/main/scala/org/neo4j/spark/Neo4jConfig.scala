package org.neo4j.spark

import org.apache.spark.SparkConf
import org.neo4j.driver.v1.{Driver, AuthTokens, Config, GraphDatabase}

/**
  * @author mh
  * @since 02.03.16
  */
case class Neo4jConfig(val url: String, val user: String = "neo4j", val password: Option[String] = None) {

  def boltConfig() = Config.build.withoutEncryption().toConfig

  def driver(config: Neo4jConfig) : Driver = config.password match {
    case Some(pwd) => GraphDatabase.driver(config.url, AuthTokens.basic(config.user, pwd), boltConfig())
    case _ => GraphDatabase.driver(config.url, boltConfig())
  }

  def driver() : Driver = driver(this)

  def driver(url: String): Driver = GraphDatabase.driver(url, boltConfig())

}

object Neo4jConfig {
  val prefix = "spark.neo4j.bolt."
  def apply(sparkConf: SparkConf): Neo4jConfig = {
    val url = sparkConf.get(prefix + "url", "bolt://localhost")
    val user = sparkConf.get(prefix + "user", "neo4j")
    val password: Option[String] = sparkConf.getOption(prefix + "password")
    Neo4jConfig(url, user, password)
  }
}
