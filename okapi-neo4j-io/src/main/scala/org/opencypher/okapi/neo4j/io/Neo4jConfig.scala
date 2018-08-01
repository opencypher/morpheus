package org.opencypher.okapi.neo4j.io

import java.net.URI

import org.neo4j.driver.v1.{AuthTokens, Config, Driver, GraphDatabase}

case class Neo4jConfig(
  uri: URI,
  user: String = "neo4j",
  password: Option[String] = None,
  encrypted: Boolean = true
) {

  def driver(): Driver = password match {
    case Some(pwd) => GraphDatabase.driver(uri, AuthTokens.basic(user, pwd), boltConfig())
    case _ => GraphDatabase.driver(uri, boltConfig())
  }

  private def boltConfig(): Config = {
    val builder = Config.build

    if (encrypted)
      builder.withEncryption().toConfig
    else
      builder.withoutEncryption().toConfig
  }
}
