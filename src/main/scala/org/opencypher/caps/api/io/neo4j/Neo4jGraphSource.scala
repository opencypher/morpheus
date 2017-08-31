package org.opencypher.caps.api.io.neo4j

import java.net.{URI, URLDecoder}

import org.neo4j.driver.v1.Config
import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class Neo4jGraphSource(config: EncryptedNeo4jConfig,
                            nodeQuery: String,
                            relQuery: String)
  extends GraphSource {

  override def handles(uri: URI): Boolean =
    uri.getScheme == "bolt" && uri.getHost == config.uri.getHost && uri.getPort == config.uri.getPort

  override def get(implicit capsSession: CAPSSession): CAPSGraph = {
    Neo4jGraphLoader.fromNeo4j(config, nodeQuery, relQuery)
  }
}

object Neo4jGraphSourceFactory extends GraphSourceFactory {

  override val protocol = "bolt"

  override def fromURI(uri: URI): Neo4jGraphSource = {
    val (user, passwd) = getUserInfo(uri)
    val (nodeQuery, relQuery) = getQueries(uri)

    Neo4jGraphSource(new EncryptedNeo4jConfig(uri, user, passwd, Config.EncryptionLevel.NONE), nodeQuery, relQuery)
  }

  private def getUserInfo(uri: URI) = Option(uri.getUserInfo) match {
    case Some(info) =>
      val tokens = info.split(":")
      if (tokens.size != 2) Raise.invalidArgument("username:password", "nothing")
      tokens(0) -> Some(tokens(1))

    case None => "" -> None
  }

  private def getQueries(uri: URI) = Option(uri.getQuery) match {
    case Some(queries) =>
      val tokens = queries.split(";")
      val nodeQuery = tokens.headOption.getOrElse(Raise.invalidArgument("a node query", "none"))
      val relQuery = tokens.tail.headOption.getOrElse(Raise.invalidArgument("a relationship query", "none"))
      URLDecoder.decode(nodeQuery, "UTF-8") -> URLDecoder.decode(relQuery, "UTF-8")

    case None => Raise.invalidArgument("node and relationship query", "none")
  }
}
